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

from adaptive_scheduler.utils    import (datetime_to_epoch, normalise,
                                         normalised_epoch_to_datetime)
from adaptive_scheduler.printing import print_req_summary



def target_to_rise_set_target(target):
    '''Convert scheduler Target to rise_set target dict.'''

    # TODO: Change to default_dict, expand to allow proper motion etc.
    target_dict = {
                    'ra'    : target.ra,
                    'dec'   : target.dec,
                   }

    return target_dict


def telescope_to_rise_set_telescope(telescope):
    '''Convert scheduler Telescope to rise_set telescope dict.'''

    # TODO: Move scheduler Telescope code to rise_set.
    telescope_dict = {
                        'latitude'  : Angle(degrees=telescope.latitude),
                        'longitude' : Angle(degrees=telescope.longitude),
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


def make_dark_up_kernel_intervals(req, resource_name, visibility_from):
    '''Find the set of intervals where the target of the provided request it is both
       dark and up from the requested resource, and convert this into a list of
       kernel intervals to return.'''

    rs_target  = target_to_rise_set_target(req.target)
    visibility = visibility_from[resource_name]

    # Find when it's dark, and when the target is up
    rs_dark_intervals = visibility.get_dark_intervals()
    rs_up_intervals   = visibility.get_target_intervals(rs_target)

    # Convert the rise_set intervals into kernel speak
    dark_intervals = rise_set_to_kernel_intervals(rs_dark_intervals)
    up_intervals   = rise_set_to_kernel_intervals(rs_up_intervals)

    # Construct the intersection (dark AND up) reprsenting actual visibility
    intersection = dark_intervals.intersect([up_intervals])

    # Intersect with any window provided in the user request
    user_windows   = req.windows.at(resource_name)
    user_intervals = req_window_to_kernel_intervals(user_windows)
    intersection   = intersection.intersect([user_intervals])

    # Print some summary info
    print_req_summary(req, resource_name, rs_dark_intervals, rs_up_intervals, intersection)

    return intersection


def construct_compound_reservation(compound_request, dt_intervals_list, sem_start):
    '''Convert a CompoundRequest into a CompoundReservation, translating datetimes
       to kernel epoch times.
    '''

    idx = 0
    reservations = []
    for dark_up_intervals in dt_intervals_list:
        # Convert timepoints into normalised epoch time
        epoch_intervals = normalise_dt_intervals(dark_up_intervals, sem_start)

        # Construct Reservations
        # Priority comes from the parent CompoundRequest
        # Each Reservation represents the set of available windows of opportunity
        # The resource is governed by the timepoint.resource attribute
        request = compound_request.requests[idx]
        window_dict = {
                        request.telescope.name : epoch_intervals
                      }
        reservations.append( Reservation(compound_request.priority,
                                         request.duration,
                                         window_dict
                                         ) )

        # Store the original requests for recovery after scheduling
        # TODO: Do this with a field provided for this purpose, not this hack
        reservations[-1].compound_request = compound_request
        reservations[-1].request          = request

        idx += 1

    # Combine Reservations into CompoundReservations
    # Each CompoundReservation represents an actual request to do something
    compound_res = CompoundReservation(reservations, compound_request.operator)

    return compound_res


def make_compound_reservations(compound_requests, visibility_from, semester_start):
    '''Parse a list of compoundRequests, and produce a corresponding list of
       CompoundReservations.'''

    # TODO: Generalise to handle arbitrary nesting.
    to_schedule = []
    for c_req in compound_requests:

        # Find the dark/up intervals for each Request in this CompoundRequest
        dark_ups = []
        for req in c_req.requests:
            dark_ups.append( make_dark_up_kernel_intervals(req, visibility_from) )

        # Make and store the CompoundReservation
        compound_res = construct_compound_reservation(c_req, dark_ups, semester_start)
        to_schedule.append(compound_res)

    return to_schedule


def construct_resource_windows(visibility_from, semester_start):
    '''Construct the set of epoch time windows for each resource, during which that
       resource is available.'''

    resource_windows = {}
    for tel_name, visibility in visibility_from.iteritems():
        rs_dark_intervals = visibility.get_dark_intervals()
        dark_intervals    = rise_set_to_kernel_intervals(rs_dark_intervals)
        ep_dark_intervals = normalise_dt_intervals(dark_intervals, semester_start)
        resource_windows[tel_name] = ep_dark_intervals

    return resource_windows


def construct_visibilities(tels, semester_start, semester_end, twilight='nautical'):
    '''Construct Visibility objects for each telescope.'''

    visibility_from = {}
    for tel_name, tel in tels.iteritems():
        rs_telescope = telescope_to_rise_set_telescope(tel)
        visibility_from[tel_name] = Visibility(rs_telescope, semester_start,
                                               semester_end, tel.horizon,
                                               twilight)

    return visibility_from
