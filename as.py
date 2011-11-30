#!/usr/bin/env python

'''
as.py - Skeleton adaptive scheduler

Author: Eric Saunders
November 2011
'''

from adaptive_scheduler.input import (build_telescopes, build_targets,
                                      target_to_rise_set_target,
                                      telescope_to_rise_set_telescope)

from rise_set.astrometry import calc_rise_set
from rise_set.visibility import Visibility

from datetime import datetime

# Configuration files
tel_file    = 'telescopes.dat'
target_file = 'targets_to_schedule.dat'


# Create telescopes
tels    = build_telescopes(tel_file)
targets = build_targets(target_file)


# TODO: Replace with config file (from laptop)
semester_start = datetime(2011, 11, 1, 0, 0, 0)
semester_end   = datetime(2011, 11, 8, 0, 0, 0)


# Pick the first telescope and target, and convert to rise_set format
rs_telescope = telescope_to_rise_set_telescope(tels[0])
rs_target    = target_to_rise_set_target(targets[0])

# Find the visible periods for the first telescope
tel_0_visibility = Visibility(rs_telescope, semester_start, semester_end)
dark_intervals   = tel_0_visibility.get_dark_intervals()
up_intervals     = tel_0_visibility.get_target_intervals(rs_target)

# Print the times of darkness...
for interval in dark_intervals:
    print "Darkness from %s to %s" % (interval[0], interval[1])

# Print when the target is visible...
for interval in up_intervals:
    print "Target above horizon from %s to %s" % (interval[0], interval[1])
    interval[0].interval_type = 'up'
    interval[1].interval_type = 'up'


# Flatten the lists, merge them, and sort them
all_intervals = []
for interval in dark_intervals:
    all_intervals.append(interval[0])
    all_intervals[-1].interval_type = 'start'
    all_intervals.append(interval[1])
    all_intervals[-1].interval_type = 'end'


for interval in up_intervals:
    all_intervals.append(interval[0])
    all_intervals[-1].interval_type = 'start'
    all_intervals.append(interval[1])
    all_intervals[-1].interval_type = 'end'

all_intervals.sort()

# Construct the intersection of both interval lists
intersection = []
flag    = 0
n_lists = 2
for i in all_intervals:
    if i.interval_type == 'start':
        if flag == n_lists:
            intersection.append(i)
            i.interval_type = 'end'
        flag += 1
        if flag == n_lists:
            intersection.append(i)
            i.interval_type = 'start'
    elif i.interval_type == 'end':
        if flag == n_lists:
            intersection.append(i)
            i.interval_type = 'end'
        flag -= 1
        if flag == n_lists:
            intersection.append(i)
            i.interval_type = 'start'

print "Calculated intersections are:"

for i in intersection:
    print "%s (%s)" % (i, i.interval_type)
