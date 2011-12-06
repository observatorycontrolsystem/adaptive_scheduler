#!/usr/bin/env python

'''
as.py - Skeleton adaptive scheduler

Author: Eric Saunders
November 2011
'''

# Required for true (non-integer) division
from __future__ import division

from adaptive_scheduler.input import (build_telescopes, build_targets,
                                      build_requests,
                                      target_to_rise_set_target,
                                      telescope_to_rise_set_telescope,
                                      rise_set_to_kernel_intervals,
                                      dt_to_epoch_timepoints,
                                      construct_compound_reservation)

from adaptive_scheduler.model import Request
from adaptive_scheduler.kernel.fullscheduler_v1 import FullScheduler_v1 as FullScheduler

from rise_set.astrometry import calc_rise_set
from rise_set.visibility import Visibility

from datetime import datetime


def print_req_summary(req, rs_dark_intervals, rs_up_intervals, intersection):
    print "Target %s, observed from %s, between %s and %s" % (req.target.name,
                                                              req.telescope.name,
                                                              req.windows[0],
                                                              req.windows[1])
    for interval in rs_dark_intervals:
        print "Darkness from %s to %s" % (interval[0], interval[1])
    for interval in rs_up_intervals:
        print "Target above horizon from %s to %s" % (interval[0], interval[1])

    print "Calculated intersections are:"

    for i in intersection.timepoints:
        print "    %s (%s)" % (i.time, i.type)


# Configuration files
tel_file     = 'telescopes.dat'
target_file  = 'targets_to_schedule.dat'
request_file = 'requests.dat'

# TODO: Replace with config file (from laptop)
semester_start = datetime(2011, 11, 1, 0, 0, 0)
semester_end   = datetime(2011, 11, 8, 0, 0, 0)

# Create telescopes, targets, and requests
tels     = build_telescopes(tel_file)
targets  = build_targets(target_file)
requests = build_requests(request_file, targets, tels, semester_start, semester_end)

visibility_from = {}
for tel_name, tel in tels.iteritems():
    rs_telescope = telescope_to_rise_set_telescope(tel)
    visibility_from[tel_name] = Visibility(rs_telescope, semester_start, semester_end)

# Construct resource windows for the kernel
resource_windows = {}
for tel_name, visibility in visibility_from.iteritems():
    rs_dark_intervals = visibility.get_dark_intervals()
    dark_intervals    = rise_set_to_kernel_intervals(rs_dark_intervals)
    ep_dark_intervals = dt_to_epoch_timepoints(dark_intervals.timepoints, semester_start)
    resource_windows[tel_name] = ep_dark_intervals


to_schedule = []
for req in requests:
    rs_target    = target_to_rise_set_target(req.target)
    visibility   = visibility_from[req.telescope.name]

    rs_dark_intervals = visibility.get_dark_intervals()
    rs_up_intervals   = visibility.get_target_intervals(rs_target)

    # Find the available set of observing windows
    dark_intervals = rise_set_to_kernel_intervals(rs_dark_intervals)
    up_intervals   = rise_set_to_kernel_intervals(rs_up_intervals)

    # Construct the intersection of both interval lists
    intersection = dark_intervals.intersect([up_intervals])

    # Print some summary info
    print_req_summary(req, rs_dark_intervals, rs_up_intervals, intersection)

    compound_res = construct_compound_reservation(req, intersection.timepoints,
                                                  semester_start)

    to_schedule.append(compound_res)



print "Finished constructing compound reservations..."
print "There are %d CompoundReservations to schedule:" % (len(to_schedule))


scheduler = FullScheduler(to_schedule, resource_windows,
                          contractual_obligation_list=[])

scheduler.schedule_all()
