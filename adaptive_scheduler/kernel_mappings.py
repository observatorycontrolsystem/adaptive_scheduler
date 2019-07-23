#!/usr/bin/env python

'''
kernel_mappings.py - Construct Kernel objects from Scheduler objects.

The scheduling kernel operates CompoundReservations, which are constructed from
Reservations. These are scheduled according to the constraints provided by a set of
resource windows. The timeline over which the kernel operates is a continuous
abstract number line, measured in seconds. The kernel schedules intervals; it knows
nothing about telescopes, rise and set times, or datetimes.

The scheduler which surrounds this kernel works with a higher level set of objects
which more closely model the telescope network domain. It manages the receipt and
transfer of CompoundRequests, expressed in real-world datetimes,
into CompoundReservations which can be fed to the kernel.

This module contains functions to convert CompoundRequests into CompoundReservations.
This also involves interfacing with rise_set functions to calculate visible
intervals.

Author: Eric Saunders
March 2012
'''

from rise_set.angle           import Angle
from rise_set.visibility      import Visibility
from rise_set.utils           import is_static_target

from time_intervals.intervals import Intervals
from adaptive_scheduler.kernel.reservation_v3 import Reservation_v3 as Reservation
from adaptive_scheduler.kernel.reservation_v3 import CompoundReservation_v2 as CompoundReservation

from adaptive_scheduler.utils    import ( datetime_to_epoch, normalise,
                                          timeit, metric_timer )
from adaptive_scheduler.printing import plural_str as pl
from adaptive_scheduler.models   import Window, Windows, filter_compounds_by_type, RequestGroup
from adaptive_scheduler.request_filters import (filter_on_duration, filter_on_type,
                                                truncate_upper_crossing_windows,
                                                filter_out_future_windows,
                                                drop_empty_requests,
                                                log_windows)
from adaptive_scheduler.log         import RequestGroupLogger

from multiprocessing import Pool, cpu_count, current_process, TimeoutError
from redis import Redis
import cPickle
import os

# Set up and configure a module scope logger
import logging
log = logging.getLogger(__name__)

multi_rg_log = logging.getLogger('rg_logger')
rg_log = RequestGroupLogger(multi_rg_log)

redis = Redis(host=os.getenv('REDIS_URL', 'redisdev'), db=0, password='schedulerpass', socket_connect_timeout=15,
              socket_timeout=30)

local_cache = {}


def telescope_to_rise_set_telescope(telescope):
    '''Convert scheduler Telescope to rise_set telescope dict.'''

    # TODO: Move scheduler Telescope code to rise_set.
    HOURS_TO_DEGREES = 15
    telescope_dict = {
                        'latitude'  : Angle(degrees=telescope['latitude']),
                        'longitude' : Angle(degrees=telescope['longitude']),
                        'ha_limit_neg' : Angle(degrees=telescope['ha_limit_neg'] * HOURS_TO_DEGREES),
                        'ha_limit_pos' : Angle(degrees=telescope['ha_limit_pos'] * HOURS_TO_DEGREES),
                     }

    return telescope_dict


def rise_set_to_kernel_intervals(intervals):
    '''Convert rise_set intervals (a list of (start, end) datetime tuples) to
       kernel Intervals (an object that stores Timepoints).'''
    return Intervals(intervals)


def req_windows_to_kernel_intervals(windows_for_resource):
    '''Convert windows for resources into kernel intervals for resources
    '''
    return {resource: req_window_to_kernel_intervals(windows) for resource, windows in windows_for_resource.items()}


def req_window_to_kernel_intervals(windows):
    '''Convert rise_set intervals (a list of (start, end) datetime tuples) to
       kernel Intervals (an object that stores Timepoints).'''

    intervals = []
    for window in windows:
        intervals.append((window.start, window.end))

    return Intervals(intervals)


def normalise_dt_intervals(dt_intervals, dt_earliest):
    '''Convert datetime Intervals into normalised kernel Intervals.'''

    epoch_earliest = datetime_to_epoch(dt_earliest)

    epoch_timepoints = []
    for tp in dt_intervals.toDictList():
        epoch_time = normalise(datetime_to_epoch(tp['time']), epoch_earliest)
        epoch_timepoints.append({'time': epoch_time, 'type': tp['type']})

    return Intervals(epoch_timepoints)


def cache_rise_set_timepoint_intervals(args):
    '''Calculates the rise set timepoint interval of a target and attempts to put the result in redis. If it fails and
        throws an exception, the calling code should catch this and fall back to compute rise sets synchronously
    '''
    try:
	log.info('process {} is calculating a rise set'.format(current_process().pid))
        (resource, rise_set_target, visibility, max_airmass, min_lunar_distance) = args
        intervals = get_rise_set_timepoint_intervals(rise_set_target, visibility, max_airmass, min_lunar_distance)
        cache_key = make_cache_key(resource, rise_set_target, max_airmass, min_lunar_distance)
        redis.set(cache_key, cPickle.dumps(intervals))
	log.info('process {} finished calculating rise set'.format(current_process().pid))
    except Exception as e:
	log.warn('received an error when trying to cache rise set value {}'.format(repr(e)))
        # Catch and reraise the exception as a base Exception to make sure it is pickleable and doesn't hang the process
        raise Exception(repr(e))


def get_rise_set_timepoint_intervals(rise_set_target, visibility, max_airmass, min_lunar_distance):
    ''' Computes the rise set timepoint intervals for a given target, visibility object, and constraints
    '''
    # arguments are packed into a tuple since multiprocessing pools only work with single arg functions
    rs_dark_intervals = visibility.get_dark_intervals()
    rs_up_intervals = visibility.get_target_intervals(target=rise_set_target, up=True,
                                                      airmass=max_airmass)
    if not is_static_target(rise_set_target):
        # get the moon distance intervals using the target intervals and min_lunar_distance constraint
        rs_up_intervals = visibility.get_moon_distance_intervals(target=rise_set_target,
                                                                 target_intervals=rs_up_intervals,
                                                                 moon_distance=Angle(degrees=min_lunar_distance))
    # HA support only currently implemented for ICRS targets
    if 'ra' in rise_set_target:
        rs_ha_intervals = visibility.get_ha_intervals(rise_set_target)
    else:
        rs_ha_intervals = rs_up_intervals

    # Convert the rise_set intervals into kernel speak
    dark_intervals = rise_set_to_kernel_intervals(rs_dark_intervals)
    # the target intervals then are then those that pass the moon distance constraint
    up_intervals   = rise_set_to_kernel_intervals(rs_up_intervals)
    ha_intervals   = rise_set_to_kernel_intervals(rs_ha_intervals)
    dark_intervals = dark_intervals.intersect([up_intervals, ha_intervals])
    # Construct the intersection (dark AND up) representing actual visibility
    return dark_intervals


def construct_compound_reservation(request_group, semester_start):
    '''Convert a UserRequest into a CompoundReservation, translating datetimes
       to kernel epoch times. The Request windows were already translated into visible windows during the 
       filter_on_visibility step.
    '''
    reservations = []
    for index, request in enumerate(request_group.requests):
        visibility_intervals_for_resources = req_windows_to_kernel_intervals(request.windows.windows_for_resource)
        kernel_intervals_for_resources = translate_request_windows_to_kernel_windows(visibility_intervals_for_resources,
                                                                                     semester_start)

        # Construct the kernel Reservation
        res = Reservation(request_group.get_effective_priority(index), request.duration, kernel_intervals_for_resources,
                          previous_solution_reservation=request.scheduled_reservation)
        # Store the original requests for recovery after scheduling
        # TODO: Do this with a field provided for this purpose, not this hack
        res.request_group = request_group
        res.request = request

        reservations.append(res)

    # Combine Reservations into CompoundReservations
    # Each CompoundReservation represents an actual request to do something
    compound_res = CompoundReservation(reservations, request_group.operator)

    return compound_res


def construct_many_compound_reservation(request_group, request_index, semester_start):
    request = request_group.requests[request_index]
    visibility_intervals_for_resources = req_windows_to_kernel_intervals(request.windows.windows_for_resource)
    kernel_intervals_for_resources = translate_request_windows_to_kernel_windows(visibility_intervals_for_resources,
                                                                                 semester_start)
    # Construct the kernel Reservation
    res = Reservation(request_group.get_effective_priority(request_index), request.duration,
                      kernel_intervals_for_resources, previous_solution_reservation=request.scheduled_reservation)
    # Store the original requests for recovery after scheduling
    # TODO: Do this with a field provided for this purpose, not this hack
    res.request_group = request_group
    res.request          = request

    # Create a CR of type 'single' for kernel scheduling
    compound_res = CompoundReservation([res], 'single')

    return compound_res


def translate_request_windows_to_kernel_windows(intersection_dict, sem_start):
    window_dict = {}

    # Build the normalised Windows data structure for the kernel
    for resource_name, dark_up_intervals in intersection_dict.iteritems():
        # Convert timepoints into normalised epoch time
        epoch_intervals = normalise_dt_intervals(dark_up_intervals, sem_start)

        # Construct Reservations
        # Priority comes from the parent CompoundRequest
        # Each Reservation represents the set of available windows of opportunity
        # The resource is governed by the timepoint.resource attribute
        window_dict[resource_name] = epoch_intervals

    return window_dict


@timeit
@metric_timer('filter_on_scheduling_horizon', num_requests=lambda x: len(x))
def filter_on_scheduling_horizon(request_groups, scheduling_horizon):
    '''Filter out windows in user requests that extend beyond the scheduling
       horizon for types (single, many)
    '''
    rgs_by_type = filter_compounds_by_type(request_groups)
    log.info("Identified %s, %s, %s, %s" % (pl(len(rgs_by_type['single']), 'single'),
                                            pl(len(rgs_by_type['many']), 'many'),
                                            pl(len(rgs_by_type['and']), 'and'),
                                            pl(len(rgs_by_type['oneof']), 'oneof')))

    # Filter windows that are beyond the short-term scheduling horizon
    log.info("Filtering RGs of type 'single' and 'many' based on scheduling horizon (%s)" % scheduling_horizon)
    horizon_limited_rgs = rgs_by_type['single'] + rgs_by_type['many']
    horizon_limited_rgs = truncate_upper_crossing_windows(horizon_limited_rgs, horizon=scheduling_horizon)
    horizon_limited_rgs = filter_out_future_windows(horizon_limited_rgs, horizon=scheduling_horizon)
    #TODO: Add the duration filter here?
    # Clean up Requests without any windows
    horizon_limited_rgs = filter_on_type(horizon_limited_rgs)
    # Many's may have children with no windows that should be removed from consideration
    removed_requests = drop_empty_requests(horizon_limited_rgs)
    log.info("After filtering, %d horizon-limited rgs remain" % len(horizon_limited_rgs))
    
    # Compounds (and/oneof) are not constrained to the short-term scheduling horizon
    # TODO: Remove this block after review
    log.info("Filtering compound RGs of type 'and' and 'oneof', not constrained by scheduling horizon")
    unlimited_rgs = rgs_by_type['and'] + rgs_by_type['oneof']
    unlimited_rgs = truncate_upper_crossing_windows(unlimited_rgs)
    unlimited_rgs = filter_out_future_windows(unlimited_rgs)
    
    # TODO: it's possible that one-ofs and ands may have these windowless 
    # children at this point from requests that crossed the semester boundary
    # might need to drop empty requests before filtering on type   
    
    # Clean up Requests without any windows
    unlimited_rgs = filter_on_type(unlimited_rgs)
    log.info("After filtering, %d unlimited RGs remain" % len(unlimited_rgs))
    
    remaining_rgs = horizon_limited_rgs + unlimited_rgs
     
    return remaining_rgs


@timeit
def filter_for_kernel(request_groups, visibility_for_resource, downtime_intervals, semester_start, semester_end,
                      scheduling_horizon):
    '''After throwing out and marking RGs as UNSCHEDULABLE, reduce windows by
       considering dark time and target visibility. Remove any RGs that are now too
       small to hold their duration after this consideration, so they are not passed
       to the kernel.
       NOTE: We do this as an explicit additional filtering step, because we do not
       want to set the UNSCHEDULABLE flag for these Requests. This is because the
       step is network-dependent; if the network subsequently changes (e.g. a
       telescope becomes available), then the Request may then be schedulable.'''
    # trim windows to scheduling horizon, expiry, or end of semester and filter
    rgs = filter_on_scheduling_horizon(request_groups, scheduling_horizon)

    # Filter on rise_set/airmass/downtime intervals
    rgs = filter_on_visibility(rgs, visibility_for_resource, downtime_intervals, semester_start, semester_end)

    # Clean up now impossible Requests
    rgs = filter_on_duration(rgs)
    # TODO: Do we need to drop empty requests here before carrying on?
    rgs = filter_on_type(rgs, [])

    return rgs


def make_cache_key(resource, rs_target, max_airmass, min_lunar_distance):
    return str(resource) + '_' + str(max_airmass) + '_'  + str(min_lunar_distance) + '_' + repr(sorted(rs_target.iteritems()))



def update_cached_semester(semester_start, semester_end):
    if 'current_semester' not in local_cache:
        try:
            current_semester = redis.get('current_semester')
            local_cache['current_semester'] = current_semester
        except Exception:
            current_semester = ''
            log.error("current semester is not in the local cache, and redis is unavailable. Please restart redis.")
    else:
        current_semester = local_cache['current_semester']
    if str(current_semester) != '{}_{}'.format(semester_start, semester_end):
        # if the current semester has changed from what redis previously knew, clear redis entirely and re-cache
        local_cache.clear()
        current_semester = '{}_{}'.format(semester_start, semester_end)
        local_cache['current_semester'] = current_semester
        try:
            redis.flushdb()
            redis.set('current_semester', current_semester)
        except Exception:
            log.error("Redis is down, and the current semester has rolled over. Please manually delete the redis cache file and restart redis.")


@log_windows
def filter_on_visibility(rgs, visibility_for_resource, downtime_intervals, semester_start, semester_end):
    update_cached_semester(semester_start, semester_end)
    rise_sets_to_compute_later = {}
    for rg in rgs:
        for r in rg.requests:
            for conf in r.configurations:
                rise_set_target = conf.target.in_rise_set_format()
                for resource in r.windows.windows_for_resource:
                    cache_key = make_cache_key(resource, rise_set_target, conf.constraints['max_airmass'],
                                               conf.constraints['min_lunar_distance'])
                    if cache_key not in local_cache:
                        try:
                            # put intersections from the redis cache into the local cache for use later
                            local_cache[cache_key] = cPickle.loads(redis.get(cache_key))
                        except Exception:
                            # need to compute the rise_set for this target/resource/airmass/lunar_distance combo
                            rise_sets_to_compute_later[cache_key] = ((resource, rise_set_target,
                                                                      visibility_for_resource[resource],
                                                                      conf.constraints['max_airmass'],
                                                                      conf.constraints['min_lunar_distance']))

    num_processes = cpu_count() - 1
    log.info("computing {} rise sets with {} processes".format(len(rise_sets_to_compute_later.keys()), num_processes))
    # now use a thread pool to compute the missing rise_set intervals for a resource and target
    if rise_sets_to_compute_later:
        pool = Pool(processes=num_processes)
        try:
            pool.map_async(cache_rise_set_timepoint_intervals, rise_sets_to_compute_later.values()).get(300)
        except TimeoutError as te:
            pool.terminate()
            log.warn('300 second timeout reached on multiprocessing rise_set computations. Falling back to synchronous computation')
        except Exception:
            log.warn('Failed to save rise_set intervals into redis. Please check that redis is online. Falling back on synchronous rise_set calculations.')
        log.info("finished computing rise_sets")
        pool.close()
        pool.join()
        log.info("finished closing thread pool")
        for cache_key in rise_sets_to_compute_later.keys():
            try:
                local_cache[cache_key] = cPickle.loads(redis.get(cache_key))
            except Exception:
                # failed to load this cache_key from redis, maybe redis is down. Will run synchronously.
                (resource, rise_set_target, visibility, max_airmass, min_lunar_distance) = rise_sets_to_compute_later[cache_key]
                local_cache[cache_key] = get_rise_set_timepoint_intervals(rise_set_target, visibility, max_airmass, min_lunar_distance)

    # now that we have all the rise_set intervals in local cache, perform the visibility filter on the requests
    for rg in rgs:
        for r in rg.requests:
            intervals_by_resource = {}
            for conf in r.configurations:
                for resource in r.windows.windows_for_resource:
                    cache_key = make_cache_key(resource, conf.target.in_rise_set_format(),
                                    conf.constraints['max_airmass'],
                                    conf.constraints['min_lunar_distance'])
                    target_intervals = local_cache[cache_key]
                    if resource in intervals_by_resource:
                        intervals_by_resource[resource] = intervals_by_resource[resource].intersect([target_intervals])
                    else:
                        intervals_by_resource[resource] = target_intervals
            process_request_visibility(rg.id, r, intervals_by_resource, downtime_intervals)

    return rgs


def process_request_visibility(request_group_id, request, target_intervals, downtime_intervals):
    request = compute_request_availability(request, target_intervals, downtime_intervals)
    if request.has_windows():
        tag = 'RequestIsVisible'
        msg = 'Request {} (RG {}) is visible ({} windows remaining)'.format(request.id, request_group_id,
                                                                            request.n_windows())
    else:
        tag = 'RequestIsNotVisible'
        msg = 'Request {} (RG {}) is not up and dark at any available telescope'.format(request.id,
                                                                                        request_group_id)
    RequestGroup.emit_request_group_feedback(request_group_id, msg, tag)


def compute_request_availability(req, target_intervals_by_resource, downtime_intervals):
    intervals_for_resource = {}
    for resource, target_intervals in target_intervals_by_resource.items():
        # Intersect with any window provided in the user request
        user_windows = req.windows.at(resource)
        user_intervals = req_window_to_kernel_intervals(user_windows)
        intervals_for_resource[resource] = target_intervals.intersect([user_intervals])
        if resource in downtime_intervals and len(downtime_intervals[resource]) > 0:
            downtime_kernel_intervals = rise_set_to_kernel_intervals(downtime_intervals[resource])
            intervals_for_resource[resource] = intervals_for_resource[resource].subtract(downtime_kernel_intervals)

    req.windows = intervals_to_windows(req, intervals_for_resource)
    return req


def intervals_to_windows(req, intersections_for_resource):
    windows = Windows()
    for resource_name, intervals in intersections_for_resource.iteritems():
        windows_for_resource = req.windows.windows_for_resource[resource_name]
        # TODO: This needs cleanup
        # It's possible there are no windows for this resource so we can't
        # assume that we will be able to get a handle on the resource from the
        # first window.
        if(len(windows_for_resource) > 0):
            resource = windows_for_resource[0].resource

            for (start, end) in intervals.toTupleList():
                w = Window( { 'start' : start, 'end' : end }, resource )
                windows.append(w)

    return windows


@timeit
@metric_timer('make_compound_reservations', num_requests=lambda x: len(x))
def make_compound_reservations(request_groups, semester_start):
    '''Parse a list of CompoundRequests, and produce a corresponding list of
       CompoundReservations.'''
    to_schedule = []
    for rg in request_groups:
        # Make and store the CompoundReservation
        compound_res = construct_compound_reservation(rg, semester_start)
        to_schedule.append(compound_res)

    return to_schedule


@timeit
def make_many_type_compound_reservations(many_request_groups, semester_start):
    '''Parse a list of CompoundRequests of type 'many', and produce a corresponding
       list of CompoundReservations. Each 'many' will produce one CompoundReservation
       per Request child.'''
    to_schedule = []
    for many_rg in many_request_groups:
        # Produce a distinct CR for each R in a 'many'
        # We do this because the kernel knows nothing about 'many', and will treat
        # the scheduling of the children as completely independent
        for request_index, _ in enumerate(many_rg.requests):
            compound_res = construct_many_compound_reservation(many_rg, request_index, semester_start)
            to_schedule.append(compound_res)

    return to_schedule


def construct_resource_windows(visibility_for_resource, semester_start, availabile_resources):
    '''Construct the set of epoch time windows for each resource, during which that
       resource is available.'''

    resource_windows = {}
    for tel_name, visibility in visibility_for_resource.iteritems():
        if tel_name in availabile_resources:
            rs_dark_intervals = visibility.get_dark_intervals()
            dark_intervals    = rise_set_to_kernel_intervals(rs_dark_intervals)
            ep_dark_intervals = normalise_dt_intervals(dark_intervals, semester_start)
            resource_windows[tel_name] = ep_dark_intervals

    return resource_windows


def construct_visibilities(tels, semester_start, semester_end, twilight='nautical'):
    '''Construct Visibility objects for each telescope.'''

    visibility_for_resource = {}
    for tel_name, tel in tels.iteritems():
        rs_telescope = telescope_to_rise_set_telescope(tel)
        visibility = Visibility(rs_telescope, semester_start,
                                semester_end, tel['horizon'],
                                twilight, tel['ha_limit_neg'],
                                tel['ha_limit_pos'])

        visibility_for_resource[tel_name] = visibility

    return visibility_for_resource


def construct_global_availability(resource_interval_mask, semester_start, resource_windows):
    '''Use the interval mask to make unavailable portions of each resource where an
       observation is running/too request will occur. Normalise and intersect with the resource windows to
       get a final global availability for each resource.
       resource_intervals_mask is expected to be a dict like:
       { 'resource_name' : Intervals() }
    '''
    for resource_name, masked_intervals in resource_interval_mask.items():
        norm_masked_interval = normalise_dt_intervals(masked_intervals, semester_start)
        resource_windows[resource_name] = resource_windows[resource_name].subtract(norm_masked_interval)

    return resource_windows
