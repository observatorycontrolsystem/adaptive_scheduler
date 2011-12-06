#!/usr/bin/env python

'''
input.py - Read and marshal scheduling input (targets, telescopes, etc)

description

Author: Eric Saunders
November 2011
'''

from adaptive_scheduler.model import Telescope, Target, Request
from rise_set.sky_coordinates import RightAscension, Declination
from rise_set.angle           import Angle

from adaptive_scheduler.kernel.timepoint import Timepoint
from adaptive_scheduler.kernel.intervals import Intervals
from adaptive_scheduler.kernel.reservation_v2 import Reservation_v2 as Reservation
from adaptive_scheduler.kernel.reservation_v2 import CompoundReservation_v2 as CompoundReservation

import ast
import calendar


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


def build_requests(filename, targets, telescopes, semester_start, semester_end):
    # TODO: Currently we assume all windows are the width of the semester. Allow
    # user-specified windows.
    requests = []
    request_dicts = file_to_dicts(filename)

    for d in request_dicts:

        requests.append(
                         Request(
                                  target    = targets[ d['target_name'] ],
                                  telescope = telescopes[ d['telescope'] ],
                                  priority  = d['priority'],
                                  duration  = d['duration'],
                                  res_type  = d['res_type'],
                                  windows   = [semester_start, semester_end],
                                )
                       )

    return requests


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



def dt_to_epoch_timepoints(timepoints, earliest):
    # Convert into epoch time for a consistent linear scale
    earliest = datetime_to_epoch(earliest)

    epoch_timepoints = []
    for tp in timepoints:
        epoch_time = normalise(datetime_to_epoch(tp.time), earliest)
        epoch_timepoints.append(Timepoint(epoch_time, tp.type, tp.resource))

    return epoch_timepoints



def construct_compound_reservation(request, dt_timepoints, sem_start):
    # Convert timepoints into normalised epoch time
    epoch_timepoints = dt_to_epoch_timepoints(dt_timepoints, sem_start)

    # Construct Reservations
    # Each Reservation represents the set of available windows of opportunity
    # The resource is governed by the timepoint.resource attribute
    res = Reservation(request.priority, request.duration, epoch_timepoints)

    # Combine Reservations into CompoundReservations
    # Each CompoundReservation represents an actual request to do something
    compound_res = CompoundReservation([res], request.res_type)

    return compound_res



def datetime_to_epoch(dt):
    return calendar.timegm(dt.timetuple())



def normalise(value, start):
    '''Normalise any value to a positive range, starting at zero.'''

    return value - start
