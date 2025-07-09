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

from datetime import datetime, timedelta

from rise_set.angle import Angle
from rise_set.visibility import Visibility
from rise_set.utils import is_static_target
from rise_set.exceptions import MovingViolation

from time_intervals.intervals import Intervals
from adaptive_scheduler.kernel.reservation import Reservation
from adaptive_scheduler.kernel.reservation import CompoundReservation

from adaptive_scheduler.utils import (normalise_datetime_intervals, timeit, metric_timer, OptimizationType)
from adaptive_scheduler.printing import plural_str as pl
from adaptive_scheduler.models import (Window, Windows, filter_compounds_by_type, RequestGroup, redis_instance)
from adaptive_scheduler.request_filters import (filter_on_duration, filter_on_type,
                                                truncate_upper_crossing_windows,
                                                filter_out_future_windows,
                                                drop_empty_requests,
                                                log_windows)
from adaptive_scheduler.log import RequestGroupLogger

from multiprocessing import cpu_count, current_process, TimeoutError, get_context
import pickle

# Set up and configure a module scope logger
import logging

log = logging.getLogger(__name__)

multi_rg_log = logging.getLogger('rg_logger')
rg_log = RequestGroupLogger(multi_rg_log)

local_cache = {}


def telescope_to_rise_set_telescope(telescope):
    """Convert scheduler Telescope to rise_set telescope dict."""
    # TODO: Move scheduler Telescope code to rise_set.
    HOURS_TO_DEGREES = 15
    return {
        'latitude': Angle(degrees=telescope['latitude']),
        'longitude': Angle(degrees=telescope['longitude']),
        'ha_limit_neg': Angle(degrees=telescope['ha_limit_neg'] * HOURS_TO_DEGREES),
        'ha_limit_pos': Angle(degrees=telescope['ha_limit_pos'] * HOURS_TO_DEGREES),
        'zenith_blind_spot': Angle(degrees=telescope['zenith_blind_spot'] * HOURS_TO_DEGREES)
    }


def rise_set_to_kernel_intervals(intervals):
    '''Convert rise_set intervals (a list of (start, end) datetime tuples) to
       kernel Intervals (an object that stores Timepoints).'''
    return Intervals(intervals)


def cache_rise_set_timepoint_intervals(args):
    '''Calculates the rise set timepoint interval of a target and attempts to put the result in redis. If it fails and
        throws an exception, the calling code should catch this and fall back to compute rise sets synchronously
    '''
    try:
        log.info('process {} is calculating a rise set'.format(current_process().pid))
        (resource, rise_set_target, visibility, max_airmass, min_lunar_distance, max_lunar_phase) = args
        intervals = get_rise_set_timepoint_intervals(rise_set_target, visibility, max_airmass, min_lunar_distance, max_lunar_phase)
        cache_key = make_cache_key(resource, rise_set_target, max_airmass, min_lunar_distance, max_lunar_phase)
        redis_instance.set(cache_key, pickle.dumps(intervals))
        log.info('process {} finished calculating rise set'.format(current_process().pid))
    except Exception as e:
        log.warn('received an error when trying to cache rise set value {}'.format(repr(e)))
        # Catch and reraise the exception as a base Exception to make sure it is pickleable and doesn't hang the process
        raise Exception(repr(e))


def get_rise_set_timepoint_intervals(rise_set_target, visibility, max_airmass, min_lunar_distance, max_lunar_phase):
    ''' Computes the rise set timepoint intervals for a given target, visibility object, and constraints
    '''
    # arguments are packed into a tuple since multiprocessing pools only work with single arg functions
    rs_dark_intervals = visibility.get_dark_intervals()
    rs_up_intervals = []
    try:
        rs_up_intervals = visibility.get_target_intervals(target=rise_set_target, up=True,
                                                          airmass=max_airmass)
    except MovingViolation as mv:
        log.warning(repr(mv))
        log.warning("rise-set failed on target")
        log.warning(rise_set_target)

    if not is_static_target(rise_set_target):
        # get the moon distance intervals using the target intervals and min_lunar_distance constraint
        if min_lunar_distance > 0.0:
            rs_up_intervals = visibility.get_moon_distance_intervals(target=rise_set_target,
                                                                     target_intervals=rs_up_intervals,
                                                                     moon_distance=Angle(degrees=min_lunar_distance))
        # Apply the moon phase intervals if max_lunar_phase < 1.0 (full moon)
        if max_lunar_phase < 1.0:
            rs_up_intervals = visibility.get_moon_phase_intervals(target_intervals=rs_up_intervals,
                                                                  max_moon_phase=max_lunar_phase)

        if visibility.zenith_blind_spot.in_degrees() > 0.0:
            rs_up_intervals = visibility.get_zenith_distance_intervals(target=rise_set_target,
                                                                       target_intervals=rs_up_intervals)

    # HA support only currently implemented for ICRS targets
    if 'ra' in rise_set_target:
        rs_ha_intervals = visibility.get_ha_intervals(rise_set_target)
    else:
        rs_ha_intervals = rs_up_intervals

    # Convert the rise_set intervals into kernel speak
    dark_intervals = rise_set_to_kernel_intervals(rs_dark_intervals)
    # the target intervals then are then those that pass the moon distance constraint
    up_intervals = rise_set_to_kernel_intervals(rs_up_intervals)
    ha_intervals = rise_set_to_kernel_intervals(rs_ha_intervals)
    dark_intervals = dark_intervals.intersect([up_intervals, ha_intervals])
    # Construct the intersection (dark AND up) representing actual visibility
    return dark_intervals


def construct_compound_reservation(request_group, semester_start, network_model):
    '''Convert a RequestGroup into a CompoundReservation, translating datetimes
       to kernel epoch times. The Request windows were already translated into visible windows during the 
       filter_on_visibility step.
    '''
    reservations = []
    for index, request in enumerate(request_group.requests):
        kernel_intervals_for_resources = request.windows.to_kernel_intervals(semester_start)

        # If the request has an optimization type of AIRMASS, pre-calculate and cache the airmasses at epoch values here.
        if request.optimization_type == OptimizationType.AIRMASS:
            request.cache_airmasses_within_kernel_windows(kernel_intervals_for_resources, network_model, semester_start)

        # Construct the kernel Reservation
        res = Reservation(request_group.get_effective_priority(index), request.duration, kernel_intervals_for_resources,
                          previous_solution_reservation=request.scheduled_reservation, request=request, request_group_id=request_group.id)
        reservations.append(res)

    # Combine Reservations into CompoundReservations
    # Each CompoundReservation represents an actual request to do something
    compound_res = CompoundReservation(reservations, request_group.operator)

    return compound_res


def construct_many_compound_reservation(request_group, request_index, semester_start, network_model):
    request = request_group.requests[request_index]
    kernel_intervals_for_resources = request.windows.to_kernel_intervals(semester_start)

    # If the request has an optimization type of AIRMASS, pre-calculate and cache the airmasses at epoch values here.
    if request.optimization_type == OptimizationType.AIRMASS:
        request.cache_airmasses_within_kernel_windows(kernel_intervals_for_resources, network_model, semester_start)

    # Construct the kernel Reservation
    res = Reservation(request_group.get_effective_priority(request_index), request.duration,
                      kernel_intervals_for_resources, previous_solution_reservation=request.scheduled_reservation,
                      request=request, request_group_id=request_group.id)

    # Create a CR of type 'single' for kernel scheduling
    compound_res = CompoundReservation([res], 'single')

    return compound_res


@timeit
@metric_timer('filter_on_scheduling_horizon', num_requests=len)
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
    # TODO: Add the duration filter here?
    # Clean up Requests without any windows
    horizon_limited_rgs = filter_on_type(horizon_limited_rgs)
    # Many's may have children with no windows that should be removed from consideration
    drop_empty_requests(horizon_limited_rgs)
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
def filter_for_kernel(request_groups, visibility_for_resource, downtime_intervals, seeing_monitor,
                      semester_start, semester_end, estimated_scheduler_end, scheduling_horizon):
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
    rgs = filter_on_visibility(rgs, visibility_for_resource, downtime_intervals, seeing_monitor, semester_start, semester_end, estimated_scheduler_end)

    # Clean up now impossible Requests
    rgs = filter_on_duration(rgs)
    # TODO: Do we need to drop empty requests here before carrying on?
    rgs = filter_on_type(rgs, [])

    return rgs


def make_cache_key(resource, rs_target, max_airmass, min_lunar_distance, max_lunar_phase):
    return f"{resource}_{max_airmass}_{min_lunar_distance}_{max_lunar_phase}_{sorted(rs_target.items())}"


def update_cached_semester(semester_start, semester_end):
    if 'current_semester' not in local_cache:
        try:
            current_semester = redis_instance.get('current_semester')
            local_cache['current_semester'] = current_semester
        except Exception:
            current_semester = ''
            log.error("current semester is not in the local cache, and redis is unavailable. Please restart redis.")
    else:
        current_semester = local_cache['current_semester']
    if isinstance(current_semester, bytes):
        # This is needed in case we are loading an old python2 saved pickle input
        current_semester = current_semester.decode('utf-8')
    if str(current_semester) != '{}_{}'.format(semester_start, semester_end):
        # if the current semester has changed from what redis previously knew, clear redis entirely and re-cache
        local_cache.clear()
        current_semester = '{}_{}'.format(semester_start, semester_end)
        local_cache['current_semester'] = current_semester
        try:
            redis_instance.flushdb()
            redis_instance.set('current_semester', current_semester)
        except Exception:
            log.error(
                "Redis is down, and the current semester has rolled over. Please manually delete the redis cache file and restart redis.")


@log_windows
def filter_on_visibility(rgs, visibility_for_resource, downtime_intervals, seeing_monitor, semester_start, semester_end, estimated_scheduler_end):
    update_cached_semester(semester_start, semester_end)
    rise_sets_to_compute_later = {}
    for rg in rgs:
        for r in rg.requests:
            for conf in r.configurations:
                rise_set_target = conf.target.in_rise_set_format()
                for resource in r.windows.windows_for_resource:
                    # If the eccentricity is > 1.0, we cant do our normal semester long caching as the orbit may be unstable
                    # over longer time horizons. Instead, just cache for this exact request only
                    if rise_set_target.get('eccentricity', 0.0) >= 1.0:
                        # Add the request id to the rise_set_target since that is part of its cache key
                        rise_set_target['request_id'] = r.id
                    cache_key = make_cache_key(resource, rise_set_target, conf.constraints['max_airmass'],
                                               conf.constraints['min_lunar_distance'], conf.constraints['max_lunar_phase'])
                    if cache_key not in local_cache:
                        try:
                            # put intersections from the redis cache into the local cache for use later
                            local_cache[cache_key] = pickle.loads(redis_instance.get(cache_key))
                        except Exception:
                            # need to compute the rise_set for this target/resource/airmass/lunar_distance/lunar_phase combo
                            # If it happens to have been something with eccentricity >= 1.0, then do not use the cached visibility_for_resource
                            if 'request_id' in rise_set_target:
                                windows_start, windows_end = windows_list_to_range(r.windows.windows_for_resource[resource])
                                visibility = duplicate_visibility_with_new_window(visibility_for_resource[resource], windows_start, windows_end)
                                rise_sets_to_compute_later[cache_key] = ((resource, rise_set_target,
                                                                          visibility,
                                                                          conf.constraints['max_airmass'],
                                                                          conf.constraints['min_lunar_distance'],
                                                                          conf.constraints['max_lunar_phase']))
                            else:
                                rise_sets_to_compute_later[cache_key] = ((resource, rise_set_target,
                                                                          visibility_for_resource[resource],
                                                                          conf.constraints['max_airmass'],
                                                                          conf.constraints['min_lunar_distance'],
                                                                          conf.constraints['max_lunar_phase']))

    num_processes = cpu_count() - 1
    log.info("computing {} rise sets with {} processes".format(len(rise_sets_to_compute_later.keys()), num_processes))
    # now use a thread pool to compute the missing rise_set intervals for a resource and target
    if rise_sets_to_compute_later:
        with get_context('spawn').Pool(processes=num_processes) as pool:
            try:
                pool.map_async(cache_rise_set_timepoint_intervals, rise_sets_to_compute_later.values()).get(300)
            except TimeoutError:
                pool.terminate()
                log.warn(
                    '300 second timeout reached on multiprocessing rise_set computations. Falling back to synchronous computation')
            except Exception:
                log.warn(
                    'Failed to save rise_set intervals into redis. Please check that redis is online. Falling back on synchronous rise_set calculations.')
            log.info("finished computing rise_sets")
            pool.close()
            pool.join()
            log.info("finished closing thread pool")
        for cache_key in rise_sets_to_compute_later.keys():
            try:
                local_cache[cache_key] = pickle.loads(redis_instance.get(cache_key))
            except Exception:
                # failed to load this cache_key from redis, maybe redis is down. Will run synchronously.
                (resource, rise_set_target, visibility, max_airmass, min_lunar_distance,
                 max_lunar_phase) = rise_sets_to_compute_later[cache_key]
                local_cache[cache_key] = get_rise_set_timepoint_intervals(rise_set_target, visibility, max_airmass,
                                                                          min_lunar_distance, max_lunar_phase)
                # save the newly calculated rise-set values into the redis cache for next restart
                try:
                    redis_instance.set(cache_key, pickle.dumps(local_cache[cache_key]))
                except Exception:
                    log.warn(
                    'Failed to save rise_set intervals into redis. Please check that redis is online.')


    # now that we have all the rise_set intervals in local cache, perform the visibility filter on the requests
    for rg in rgs:
        for r in rg.requests:
            intervals_by_resource = {}
            for conf in r.configurations:
                for resource in r.windows.windows_for_resource:
                    rise_set_target = conf.target.in_rise_set_format()
                    # If the eccentricity is > 1.0, we cant do our normal semester long caching as the orbit may be unstable
                    # over longer time horizons. Instead, just cache for this exact request only
                    if rise_set_target.get('eccentricity', 0.0) >= 1.0:
                        # Add the request id to the rise_set_target since that is part of its cache key
                        rise_set_target['request_id'] = r.id
                    cache_key = make_cache_key(resource, rise_set_target,
                                               conf.constraints['max_airmass'],
                                               conf.constraints['min_lunar_distance'],
                                               conf.constraints['max_lunar_phase'])
                    target_intervals = local_cache[cache_key]
                    if resource in intervals_by_resource:
                        intervals_by_resource[resource] = intervals_by_resource[resource].intersect([target_intervals])
                    else:
                        intervals_by_resource[resource] = target_intervals
            process_request_visibility(rg.id, r, intervals_by_resource, downtime_intervals, seeing_monitor, estimated_scheduler_end)

    return rgs


def process_request_visibility(request_group_id, request, target_intervals, downtime_intervals, seeing_monitor, estimated_scheduler_end):
    request = compute_request_availability(request, target_intervals, downtime_intervals, seeing_monitor, estimated_scheduler_end)
    if request.has_windows():
        tag = 'RequestIsVisible'
        msg = 'Request {} (RG {}) is visible ({} windows remaining)'.format(request.id, request_group_id,
                                                                            request.n_windows())
    else:
        tag = 'RequestIsNotVisible'
        msg = 'Request {} (RG {}) is not up and dark at any available telescope'.format(request.id,
                                                                                        request_group_id)
    RequestGroup.emit_request_group_feedback(request_group_id, msg, tag)


def compute_request_availability(request, target_intervals_by_resource, downtime_intervals, seeing_monitor, estimated_scheduler_end=datetime.utcnow()):
    intervals_for_resource = {}
    seeing_by_resources = seeing_monitor.retrieve_data()
    for resource, target_intervals in target_intervals_by_resource.items():
        # Intersect with any window provided in the user request
        user_windows = request.windows.at(resource)
        user_intervals = Windows.request_window_to_kernel_intervals(user_windows)
        intervals_for_resource[resource] = target_intervals.intersect([user_intervals])
        if resource in downtime_intervals:
            for instrument_type, intervals in downtime_intervals[resource].items():
                if instrument_type == 'all' or instrument_type.upper() == request.configurations[0].instrument_type.upper():
                    downtime_kernel_intervals = rise_set_to_kernel_intervals(intervals)
                    intervals_for_resource[resource] = intervals_for_resource[resource].subtract(downtime_kernel_intervals)
        if resource in seeing_by_resources:
            for conf in request.configurations:
                if 'max_seeing' in conf.constraints and conf.constraints['max_seeing'] <= seeing_by_resources[resource]['seeing']:
                    blockoff_until = seeing_by_resources[resource]['time'] + timedelta(minutes=seeing_monitor.seeing_valid_time_period)
                    if blockoff_until > estimated_scheduler_end:
                        blockoff_seeing_interval = rise_set_to_kernel_intervals([(estimated_scheduler_end, blockoff_until)])
                        intervals_for_resource[resource] = intervals_for_resource[resource].subtract(blockoff_seeing_interval)
                    # We've already blocked off time for the seeing constraint being violated, so no need to check any more configurations
                    break

    request.windows = intervals_to_windows(request, intervals_for_resource)
    return request


def intervals_to_windows(req, intersections_for_resource):
    windows = Windows()
    for resource_name, intervals in intersections_for_resource.items():
        windows_for_resource = req.windows.windows_for_resource[resource_name]
        # TODO: This needs cleanup
        # It's possible there are no windows for this resource so we can't
        # assume that we will be able to get a handle on the resource from the
        # first window.
        if (len(windows_for_resource) > 0):
            resource = windows_for_resource[0].resource

            for (start, end) in intervals.toTupleList():
                w = Window({'start': start, 'end': end}, resource)
                windows.append(w)

    return windows


@timeit
@metric_timer('make_compound_reservations', num_requests=len)
def make_compound_reservations(request_groups, semester_start, network_model):
    '''Parse a list of CompoundRequests, and produce a corresponding list of
       CompoundReservations.'''
    to_schedule = []
    for rg in request_groups:
        # Make and store the CompoundReservation
        compound_res = construct_compound_reservation(rg, semester_start, network_model)
        to_schedule.append(compound_res)

    return to_schedule


@timeit
def make_many_type_compound_reservations(many_request_groups, semester_start, network_model):
    '''Parse a list of CompoundRequests of type 'many', and produce a corresponding
       list of CompoundReservations. Each 'many' will produce one CompoundReservation
       per Request child.'''
    to_schedule = []
    for many_rg in many_request_groups:
        # Produce a distinct CR for each R in a 'many'
        # We do this because the kernel knows nothing about 'many', and will treat
        # the scheduling of the children as completely independent
        for request_index, _ in enumerate(many_rg.requests):
            compound_res = construct_many_compound_reservation(many_rg, request_index, semester_start, network_model)
            to_schedule.append(compound_res)

    return to_schedule


def construct_resource_windows(visibility_for_resource, semester_start, availabile_resources):
    '''Construct the set of epoch time windows for each resource, during which that
       resource is available.'''

    resource_windows = {}
    for tel_name, visibility in visibility_for_resource.items():
        if tel_name in availabile_resources:
            rs_dark_intervals = visibility.get_dark_intervals()
            dark_intervals = rise_set_to_kernel_intervals(rs_dark_intervals)
            ep_dark_intervals = normalise_datetime_intervals(dark_intervals, semester_start)
            resource_windows[tel_name] = ep_dark_intervals

    return resource_windows


def construct_visibilities(tels, semester_start, semester_end, twilight='nautical'):
    '''Construct Visibility objects for each telescope.'''

    visibility_for_resource = {}
    for tel_name, tel in tels.items():
        rs_telescope = telescope_to_rise_set_telescope(tel)
        visibility = Visibility(rs_telescope, semester_start,
                                semester_end, tel['horizon'],
                                twilight, tel['ha_limit_neg'],
                                tel['ha_limit_pos'],
                                tel['zenith_blind_spot'])

        visibility_for_resource[tel_name] = visibility

    return visibility_for_resource


def duplicate_visibility_with_new_window(visibility, start, end, twilight='nautical'):
    '''Construct Visibility objects for each telescope.'''
    new_visibility = Visibility(visibility.site,
                                start,
                                end,
                                visibility.horizon.in_degrees(),
                                twilight,
                                visibility.ha_limit_neg,
                                visibility.ha_limit_pos,
                                visibility.zenith_blind_spot.in_degrees())
    return new_visibility


def windows_list_to_range(windows_list):
    start = None
    end = None
    for window in windows_list:
        if start is None or window.start < start:
            start = window.start
        if end is None or window.end > end:
            end = window.end
    return start, end


def construct_global_availability(resource_interval_mask, semester_start, resource_windows):
    '''Use the interval mask to make unavailable portions of each resource where an
       observation is running/rr request will occur. Normalise and intersect with the resource windows to
       get a final global availability for each resource.
       resource_intervals_mask is expected to be a dict like:
       { 'resource_name' : Intervals() }
    '''
    for resource_name, masked_intervals in resource_interval_mask.items():
        norm_masked_interval = normalise_datetime_intervals(masked_intervals, semester_start)
        resource_windows[resource_name] = resource_windows[resource_name].subtract(norm_masked_interval)

    return resource_windows
