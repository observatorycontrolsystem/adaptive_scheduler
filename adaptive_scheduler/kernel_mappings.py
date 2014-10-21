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

from adaptive_scheduler.kernel.timepoint      import Timepoint
from adaptive_scheduler.kernel.intervals      import Intervals
#from adaptive_scheduler.kernel.reservation_v2 import Reservation_v2 as Reservation
#from adaptive_scheduler.kernel.reservation_v2 import CompoundReservation_v2 as CompoundReservation
from adaptive_scheduler.kernel.reservation_v3 import Reservation_v3 as Reservation
from adaptive_scheduler.kernel.reservation_v3 import CompoundReservation_v2 as CompoundReservation

from adaptive_scheduler.utils    import ( datetime_to_epoch, normalise,
                                          normalised_epoch_to_datetime,
                                          epoch_to_datetime, timeit )
from adaptive_scheduler.printing import print_req_summary, plural_str as pl
from adaptive_scheduler.model2   import Window, Windows, differentiate_by_type, filter_compounds_by_type
from adaptive_scheduler.request_filters import (filter_on_duration, filter_on_type,
                                                truncate_upper_crossing_windows,
                                                filter_out_future_windows,
                                                drop_empty_requests,
                                                log_windows)
from adaptive_scheduler.memoize     import Memoize
from adaptive_scheduler.log         import UserRequestLogger
from adaptive_scheduler.event_utils import report_visibility_outcome

import math

# Set up and configure a module scope logger
import logging
log = logging.getLogger(__name__)

multi_ur_log = logging.getLogger('ur_logger')
ur_log = UserRequestLogger(multi_ur_log)


def telescope_to_rise_set_telescope(telescope):
    '''Convert scheduler Telescope to rise_set telescope dict.'''

    # TODO: Move scheduler Telescope code to rise_set.
    HOURS_TO_DEGREES = 15
    telescope_dict = {
                        'latitude'  : Angle(degrees=telescope.latitude),
                        'longitude' : Angle(degrees=telescope.longitude),
                        'ha_limit_neg' : Angle(degrees=telescope.ha_limit_neg * HOURS_TO_DEGREES),
                        'ha_limit_pos' : Angle(degrees=telescope.ha_limit_pos * HOURS_TO_DEGREES),
                     }

    return telescope_dict


def rise_set_to_kernel_intervals(intervals):
    '''Convert rise_set intervals (a list of (start, end) datetime tuples) to
       kernel Intervals (an object that stores Timepoints).'''

    timepoints = []
    for dt_start, dt_end in intervals:
        timepoints.append(Timepoint(dt_start, 'start'))
        timepoints.append(Timepoint(dt_end, 'end'))

    return Intervals(timepoints)


def req_window_to_kernel_intervals(windows):
    '''Convert rise_set intervals (a list of (start, end) datetime tuples) to
       kernel Intervals (an object that stores Timepoints).'''

    timepoints = []
    for window in windows:
        timepoints.append(Timepoint(window.start, 'start'))
        timepoints.append(Timepoint(window.end, 'end'))

    return Intervals(timepoints)


def normalise_dt_intervals(dt_intervals, dt_earliest):
    '''Convert datetime Intervals into normalised kernel Intervals.'''

    epoch_earliest = datetime_to_epoch(dt_earliest)

    epoch_timepoints = []
    for tp in dt_intervals.timepoints:
        epoch_time = normalise(datetime_to_epoch(tp.time), epoch_earliest)
        epoch_timepoints.append(Timepoint(epoch_time, tp.type))

    return Intervals(epoch_timepoints)


def make_dark_up_kernel_intervals(req, visibility_from, verbose=False):
    '''Find the set of intervals where the target of the provided request it is both
       dark and up from the requested resource, and convert this into a list of
       kernel intervals to return.'''

    # TODO: Expand to allow proper motion etc.
    rs_target = req.target.in_rise_set_format()

    intersections_for_resource = {}
    for resource_name in req.windows.windows_for_resource:

        visibility        = visibility_from[resource_name][0]
        rs_dark_intervals = visibility_from[resource_name][1]()
        rs_up_intervals   = visibility_from[resource_name][2](
                                             target=rs_target,
                                             up=True,
                                             airmass=req.constraints.max_airmass)
        # HA support only currently implemented for sidereal targets
        if 'ra' in rs_target:
            rs_ha_intervals   = visibility_from[resource_name][3](rs_target)
        else:
            rs_ha_intervals   = rs_up_intervals


        # Convert the rise_set intervals into kernel speak
        dark_intervals = rise_set_to_kernel_intervals(rs_dark_intervals)
        up_intervals   = rise_set_to_kernel_intervals(rs_up_intervals)
        ha_intervals   = rise_set_to_kernel_intervals(rs_ha_intervals)

        # Construct the intersection (dark AND up) reprsenting actual visibility
        intersection = dark_intervals.intersect([up_intervals, ha_intervals])

        # Intersect with any window provided in the user request
        user_windows   = req.windows.at(resource_name)
        user_intervals = req_window_to_kernel_intervals(user_windows)
        intersection   = intersection.intersect([user_intervals])
        intersections_for_resource[resource_name] = intersection


    return intersections_for_resource


def construct_compound_reservation(compound_request, dt_intervals_list, sem_start):
    '''Convert a UserRequest into a CompoundReservation, translating datetimes
       to kernel epoch times.
    '''

    idx = 0
    reservations = []
    for intersection_dict in dt_intervals_list:

        request = compound_request.requests[idx]

        window_dict = translate_request_windows_to_kernel_windows(intersection_dict,
                                                                  sem_start)

        # Construct the kernel Reservation
        res = Reservation(compound_request.priority, request.duration, window_dict)
        # Store the original requests for recovery after scheduling
        # TODO: Do this with a field provided for this purpose, not this hack
        res.compound_request = compound_request
        res.request          = request

        reservations.append(res)

        idx += 1

    # Combine Reservations into CompoundReservations
    # Each CompoundReservation represents an actual request to do something
    compound_res = CompoundReservation(reservations, compound_request.operator)

    return compound_res

def construct_many_compound_reservation(many_c_req, child_idx,
                                        intersection_dict, sem_start):
    '''Take a Request child of a CompoundRequest of type 'many', and convert it into
       a CompoundReservation of type 'single', translating datetimes to kernel epoch times.
    '''

    request = many_c_req.requests[child_idx]
    window_dict = translate_request_windows_to_kernel_windows(intersection_dict,
                                                              sem_start)

    # Construct the kernel Reservation
    res = Reservation(many_c_req.priority, request.duration, window_dict)
    # Store the original requests for recovery after scheduling
    # TODO: Do this with a field provided for this purpose, not this hack
    res.compound_request = many_c_req
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
def filter_on_scheduling_horizon(user_requests, scheduling_horizon):
    '''Filter out windows in user requests that extend beyond the scheduling
       horizon for types (single, many)
    '''
    urs_by_type = filter_compounds_by_type(user_requests)
    log.info("Identified %s" % pl(len(urs_by_type['single']), 'single'))
    log.info("Identified %s" % pl(len(urs_by_type['many']), 'many'))
    log.info("Identified %s" % pl(len(urs_by_type['and']), 'and'))
    log.info("Identified %s" % pl(len(urs_by_type['oneof']), 'oneof'))


    # Filter windows that are beyond the short-term scheduling horizon
    log.info("Filtering URs of type 'single' and 'many' based on scheduling horizon (%s days)" % scheduling_horizon)
    horizon_limited_urs = urs_by_type['single'] + urs_by_type['many']
    horizon_limited_urs = truncate_upper_crossing_windows(horizon_limited_urs, horizon=scheduling_horizon)
    horizon_limited_urs = filter_out_future_windows(horizon_limited_urs, horizon=scheduling_horizon)
    #TODO: Add the duration filter here?
    # Clean up Requests without any windows
    horizon_limited_urs = filter_on_type(horizon_limited_urs)
    # Many's may have children with no windows that should be removed from consideration
    removed_requests = drop_empty_requests(horizon_limited_urs)
    log.info("After filtering, %d horizon-limited urs remain" % len(horizon_limited_urs))
    
    # Compounds (and/oneof) are not constrained to the short-term scheduling horizon
    # TODO: Remove this block after review
    log.info("Filtering compound URs of type 'and' and 'oneof', not constrained by scheduling horizon")
    unlimited_urs = urs_by_type['and'] + urs_by_type['oneof']
    unlimited_urs = truncate_upper_crossing_windows(unlimited_urs)
    unlimited_urs = filter_out_future_windows(unlimited_urs)
    
    # TODO: it's possible that one-ofs and ands may have these windowless 
    # children at this point from requests that crossed the semester boundry
    # might need to drop empty requests before filtering on type   
    
    # Clean up Requests without any windows
    unlimited_urs = filter_on_type(unlimited_urs)
    log.info("After filtering, %d unlimited URs remain" % len(unlimited_urs))
    
    remaining_urs = horizon_limited_urs + unlimited_urs
     
    return remaining_urs


@timeit
def filter_for_kernel(user_requests, visibility_from, semester_start, semester_end, scheduling_horizon):
    '''After throwing out and marking URs as UNSCHEDULABLE, reduce windows by
       considering dark time and target visibility. Remove any URs that are now too
       small to hold their duration after this consideration, so they are not passed
       to the kernel.
       NOTE: We do this as an explicit additional filtering step, because we do not
       want to set the UNSCHEDULABLE flag for these Requests. This is because the
       step is network-dependent; if the network subsequently changes (e.g. a
       telescope becomes available), then the Request may then be schedulable.'''
    # trim windows to scheduling horizon, expiry, or end of semester and filter
    urs = filter_on_scheduling_horizon(user_requests, scheduling_horizon)

    # Filter on rise_set/airmass
    urs = filter_on_visibility(urs, visibility_from)

    # Clean up now impossible Requests
    urs = filter_on_duration(urs)
    # TODO: Do we need to drop empty requests here before carrying on?
    urs = filter_on_type(urs, [])

    return urs


def list_available_tels(visibility_from):
    available_tels = []
    for tel in visibility_from.keys():
        n_dark_intervals = visibility_from[tel][0].dark_intervals
        if n_dark_intervals:
            available_tels.append(tel)

    return available_tels


@log_windows
def filter_on_visibility(crs, visibility_from):
    for cr in crs:
        ur_log.info("Intersecting windows with dark/up intervals", cr.tracking_number)
        for r in cr.requests:
            r = compute_intersections(r, visibility_from)
            if r.has_windows():
                tag = 'RequestIsVisible'
                msg = 'Request %s (UR %s) is visible (%d windows remaining)' % (r.request_number,
                                                                                cr.tracking_number,
                                                                                r.n_windows())
            else:
                tag = 'RequestIsNotVisible'
                msg = 'Request %s (UR %s) is not up and dark at any available telescope' % (r.request_number,
                                                                                            cr.tracking_number)
                tel_names = list_available_tels(visibility_from)
                ur_log.info("Available telescopes are: %s" % tel_names, cr.tracking_number)

            cr.emit_user_feedback(msg, tag)

    return crs


def compute_intersections(req, visibility_from):
    # Find the dark/up intervals for each Request in this CompoundRequest
    intersections_for_resource = make_dark_up_kernel_intervals(req,
                                                               visibility_from,
                                                               verbose=True)
    req.windows = intervals_to_windows(req, intersections_for_resource)
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

            while intervals.timepoints:
                tp1 = intervals.timepoints.pop(0)
                tp2 = intervals.timepoints.pop(0)
                w   = Window( { 'start' : tp1.time, 'end' : tp2.time }, resource )
                windows.append(w)

    return windows


@timeit
def make_compound_reservations(compound_requests, visibility_from, semester_start):
    '''Parse a list of CompoundRequests, and produce a corresponding list of
       CompoundReservations.'''

    # TODO: Generalise to handle arbitrary nesting.
    to_schedule = []
    for c_req in compound_requests:

        dark_ups = find_dark_ups_of_children(c_req, visibility_from)

        # Make and store the CompoundReservation
        compound_res = construct_compound_reservation(c_req, dark_ups, semester_start)
        to_schedule.append(compound_res)

    return to_schedule


def find_dark_ups_of_children(c_req, visibility_from):
    # Find the dark/up intervals for each Request in this CompoundRequest
    dark_ups = []
    for req in c_req.requests:
        intersections_for_resource = make_dark_up_kernel_intervals(req,
                                                                   visibility_from,
                                                                   verbose=False)
        dark_ups.append(intersections_for_resource)

    return dark_ups


@timeit
def make_many_type_compound_reservations(many_compound_requests, visibility_from,
                                         semester_start):
    '''Parse a list of CompoundRequests of type 'many', and produce a corresponding
       list of CompoundReservations. Each 'many' will produce one CompoundReservation
       per Request child.'''
    to_schedule = []
    for many_c_req in many_compound_requests:
        dark_ups = find_dark_ups_of_children(many_c_req, visibility_from)

        # Produce a distinct CR for each R in a 'many'
        # We do this because the kernel knows nothing about 'many', and will treat
        # the scheduling of the children as completely independent
        for child_idx, dark_up in enumerate(dark_ups):
            compound_res = construct_many_compound_reservation(many_c_req, child_idx,
                                                               dark_up, semester_start)
            to_schedule.append(compound_res)

    return to_schedule


def construct_resource_windows_orig(visibility_from, semester_start):
    '''Construct the set of epoch time windows for each resource, during which that
       resource is available.'''

    resource_windows = {}
    for tel_name, visibility in visibility_from.iteritems():
        rs_dark_intervals = visibility.get_dark_intervals()
        dark_intervals    = rise_set_to_kernel_intervals(rs_dark_intervals)
        ep_dark_intervals = normalise_dt_intervals(dark_intervals, semester_start)
        resource_windows[tel_name] = ep_dark_intervals

    return resource_windows


def construct_resource_windows(visibility_from, semester_start, availabile_resources):
    '''Construct the set of epoch time windows for each resource, during which that
       resource is available.'''

    resource_windows = {}
    for tel_name, visibility_tuple in visibility_from.iteritems():
        if tel_name in availabile_resources:
            rs_dark_intervals = visibility_tuple[1]()
            dark_intervals    = rise_set_to_kernel_intervals(rs_dark_intervals)
            ep_dark_intervals = normalise_dt_intervals(dark_intervals, semester_start)
            resource_windows[tel_name] = ep_dark_intervals

    return resource_windows


def make_empty_list(*args, **kwargs):
    return []

def construct_visibilities(tels, semester_start, semester_end, twilight='nautical'):
    '''Construct Visibility objects for each telescope.'''

    visibility_from = {}
    for tel_name, tel in tels.iteritems():
        rs_telescope = telescope_to_rise_set_telescope(tel)
        visibility = Visibility(rs_telescope, semester_start,
                                semester_end, tel.horizon,
                                twilight, tel.ha_limit_neg,
                                tel.ha_limit_pos)
        get_target = Memoize(visibility.get_target_intervals)
        get_dark   = visibility.get_dark_intervals
        get_ha     = Memoize(visibility.get_ha_intervals)

        visibility_from[tel_name] = (visibility, get_dark, get_target, get_ha)

    return visibility_from


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
