#!/usr/bin/env python

'''
input.py - Read and marshal scheduling input (targets, telescopes, etc)

description

Author: Eric Saunders
November 2011
'''

from rise_set.sky_coordinates import RightAscension, Declination
from rise_set.angle           import Angle

from adaptive_scheduler.model    import Telescope, Target, Request, CompoundRequest
from adaptive_scheduler.printing import print_req_summary

from adaptive_scheduler.kernel.timepoint import Timepoint
from adaptive_scheduler.kernel.intervals import Intervals
from adaptive_scheduler.kernel.reservation_v2 import Reservation_v2 as Reservation
from adaptive_scheduler.kernel.reservation_v2 import CompoundReservation_v2 as CompoundReservation

import ast
import calendar
from datetime import datetime


def file_to_dicts(filename):
    fh = open(filename, 'r')
    data = fh.read()

    return ast.literal_eval(data)


def build_telescopes(filename):
    telescopes = {}
    tel_dicts  = file_to_dicts(filename)

    for d in tel_dicts:
        telescopes[ d['name'] ] = Telescope(d)

    return telescopes


def build_targets(filename):
    targets = {}
    target_dicts  = file_to_dicts(filename)

    for d in target_dicts:
        targets[ d['name'] ] = Target(d)

    return targets


def build_requests(req_list, targets, telescopes):
    '''
        This one is a little different from the other build methods, because
        Requests are always intended to be sub-components of a CompoundRequest
        object (even if there is only one Request (type single)).
    '''

    requests = []

    for d in req_list:
        requests.append(
                         Request(
                                  target    = targets[ d['target_name'] ],
                                  telescope = telescopes[ d['telescope'] ],
                                  priority  = d['priority'],
                                  duration  = d['duration'],
                                )
                       )

    return requests



def build_compound_requests(filename, targets, telescopes, semester_start, semester_end):
    # TODO: Currently we assume all windows are the width of the semester. Allow
    # user-specified windows.
    compound_requests = []
    request_dicts = file_to_dicts(filename)

    for d in request_dicts:
        requests = build_requests(d['requests'], targets, telescopes)


        compound_requests.append(
                                 CompoundRequest(
                                          requests  = requests,
                                          res_type  = d['res_type'],
                                          windows   = [semester_start, semester_end],
                                        )
                                )

    return compound_requests



def target_to_rise_set_target(target):
    '''
        Convert scheduler Target to rise_set target dict.
        TODO: Move scheduler Target code to rise_set.
        TODO: Change to default_dict, expand to allow proper motion etc.
    '''

    target_dict = {
                    'ra'    : RightAscension(target.ra),
                    'dec'   : Declination(target.dec),
                    'epoch' : target.epoch,
                   }

    return target_dict


def telescope_to_rise_set_telescope(telescope):
    '''
        Convert scheduler Telescope to rise_set telescope dict.
        TODO: Move scheduler Telescope code to rise_set.
    '''

    telescope_dict = {
                        'latitude'  : Angle(degrees=telescope.latitude),
                        'longitude' : Angle(degrees=telescope.longitude),
                      }

    return telescope_dict



def rise_set_to_kernel_intervals(intervals):
    '''
        Convert rise_set intervals (a list of (start, end) datetime tuples) to
        kernel Intervals (an object that stores Timepoints).
    '''

    timepoints = []
    for (start, end) in intervals:
        timepoints.append(Timepoint(start, 'start'))
        timepoints.append(Timepoint(end, 'end'))

    return Intervals(timepoints)



def dt_to_epoch_intervals(dt_intervals, earliest):
    # Convert into epoch time for a consistent linear scale
    earliest = datetime_to_epoch(earliest)

    epoch_timepoints = []
    for tp in dt_intervals.timepoints:
        epoch_time = normalise(datetime_to_epoch(tp.time), earliest)
        epoch_timepoints.append(Timepoint(epoch_time, tp.type))

    return Intervals(epoch_timepoints)



def make_dark_up_kernel_interval(req, visibility_from):
    rs_target  = target_to_rise_set_target(req.target)
    visibility = visibility_from[req.telescope.name]

    # Find when it's dark, and when the target is up
    rs_dark_intervals = visibility.get_dark_intervals()
    rs_up_intervals   = visibility.get_target_intervals(rs_target)

    # Convert the rise_set intervals into kernel speak
    dark_intervals = rise_set_to_kernel_intervals(rs_dark_intervals)
    up_intervals   = rise_set_to_kernel_intervals(rs_up_intervals)

    # Construct the intersection (dark AND up) reprsenting actual visibility
    intersection = dark_intervals.intersect([up_intervals])

    # Print some summary info
    print_req_summary(req, rs_dark_intervals, rs_up_intervals, intersection)

    return intersection



def construct_compound_reservation(compound_request, dt_intervals_list, sem_start):

    idx = 0
    reservations = []
    for dark_up_intervals in dt_intervals_list:
        # Convert timepoints into normalised epoch time
        epoch_intervals = dt_to_epoch_intervals(dark_up_intervals, sem_start)

        # Construct Reservations
        # Each Reservation represents the set of available windows of opportunity
        # The resource is governed by the timepoint.resource attribute
        request = compound_request.requests[idx]
        reservations.append( Reservation(request.priority, request.duration,
                                         request.telescope.name, epoch_intervals) )

        # Store the original request for recovery after scheduling
        # TODO: Do this with a field provided for this purpose, not this hack
        reservations[-1].original_request = compound_request

        idx += 1

    # Combine Reservations into CompoundReservations
    # Each CompoundReservation represents an actual request to do something
    compound_res = CompoundReservation(reservations, compound_request.res_type)


    return compound_res



def get_block_datetimes(block, semester_start):

    epoch_start   = datetime_to_epoch(semester_start)
    dt_start      = normalised_epoch_to_datetime(block.scheduled_start, epoch_start)
    scheduled_end = block.scheduled_start + block.scheduled_quantum
    dt_end        = normalised_epoch_to_datetime(scheduled_end, epoch_start)

    return dt_start, dt_end



def datetime_to_epoch(dt):
    return calendar.timegm(dt.timetuple())

def datetime_to_normalised_epoch(dt, dt_start):
    return normalise(datetime_to_epoch(dt), datetime_to_epoch(dt_start))

def epoch_to_datetime(epoch_time):
    return datetime.utcfromtimestamp(epoch_time)

def normalised_epoch_to_datetime(epoch_time, epoch_start):
    unnormed_epoch = unnormalise(epoch_time, epoch_start)
    return epoch_to_datetime(unnormed_epoch)

def normalise(value, start):
    '''Normalise any value to a positive range, starting at zero.'''
    return value - start

def unnormalise(value, start):
    return value + start
