#!/usr/bin/env python

'''
as.py - Skeleton adaptive scheduler

Author: Eric Saunders
November 2011
'''

# Required for true (non-integer) division
from __future__ import division

from adaptive_scheduler.input import (build_telescopes, build_targets,
                                      build_compound_requests,
                                      target_to_rise_set_target,
                                      telescope_to_rise_set_telescope,
                                      rise_set_to_kernel_intervals,
                                      make_dark_up_kernel_interval,
                                      dt_to_epoch_intervals,
                                      datetime_to_normalised_epoch,
                                      epoch_to_datetime,
                                      get_block_datetimes,
                                      construct_compound_reservation)

from adaptive_scheduler.model import Request
from adaptive_scheduler.printing import ( print_reservation,
                                          print_compound_reservation,
                                          print_req_summary )
from adaptive_scheduler.kernel.fullscheduler_v1 import FullScheduler_v1 as FullScheduler

from rise_set.visibility import Visibility

from datetime import datetime

from lcogt.pond import pond_client
pond_client.configure_service('localhost', 12345)


def make_pond_block(block, semester_start):
    dt_start, dt_end = get_block_datetimes(block, semester_start)

    print "***Going to send this***"
    print_reservation(block)

    pond_block = pond_client.ScheduledBlock(
                                             start       = dt_start,
                                             end         = dt_end,
                                             site        = block.resource,
                                             observatory = block.resource,
                                             telescope   = block.resource,
                                             priority    = block.priority
                                            )
    return pond_block



def make_pond_schedule(schedule, semester_start):

    pond_blocks = []

    for resource_reservations in schedule.values():
        for res in resource_reservations:
            pond_block = make_pond_block(res, semester_start)
            pond_blocks.append(pond_block)

    for block in pond_blocks:
        block.save()



# Configuration files
tel_file     = 'telescopes.dat'
target_file  = 'targets.dat'
request_file = 'requests.dat'

# TODO: Replace with config file (from laptop)
semester_start = datetime(2011, 11, 1, 0, 0, 0)
semester_end   = datetime(2011, 11, 8, 0, 0, 0)

# Create telescopes, targets, and requests
tels              = build_telescopes(tel_file)
targets           = build_targets(target_file)
compound_requests = build_compound_requests(request_file, targets, tels,
                                            semester_start, semester_end)

visibility_from = {}
for tel_name, tel in tels.iteritems():
    rs_telescope = telescope_to_rise_set_telescope(tel)
    visibility_from[tel_name] = Visibility(rs_telescope, semester_start, semester_end,
                                           tel.horizon, twilight='nautical')

# Construct resource windows for the kernel
resource_windows = {}
for tel_name, visibility in visibility_from.iteritems():
    rs_dark_intervals = visibility.get_dark_intervals()
    dark_intervals    = rise_set_to_kernel_intervals(rs_dark_intervals)
    ep_dark_intervals = dt_to_epoch_intervals(dark_intervals, semester_start)
    resource_windows[tel_name] = ep_dark_intervals

for resource in resource_windows:
    print resource
    for i in resource_windows[resource].timepoints:
        print i.time, i.type


to_schedule = []
for c_req in compound_requests:

    # Find the dark/up intervals for each Request in this CompoundRequest
    dark_ups = []
    for req in c_req.requests:
        dark_ups.append( make_dark_up_kernel_interval(req, visibility_from) )

    # Make and store the CompoundReservation
    compound_res = construct_compound_reservation(c_req, dark_ups, semester_start)
    to_schedule.append(compound_res)



print "Finished constructing compound reservations..."
print "There are %d CompoundReservations to schedule:" % (len(to_schedule))
for compound_res in to_schedule:
    print_compound_reservation(compound_res)


#dump_requests_to_python(filename='scheduling_clashes.py', to_schedule)





scheduler = FullScheduler(to_schedule, resource_windows,
                          contractual_obligation_list=[])

schedule = scheduler.schedule_all()

epoch_start = datetime_to_normalised_epoch(semester_start, semester_start)
epoch_end   = datetime_to_normalised_epoch(semester_end, semester_start)

print "Scheduling completed. Final schedule:"

print "Scheduling for semester %s to %s" % (semester_start, semester_end)
print "Scheduling for normalised epoch %s to %s" % (epoch_start, epoch_end)
for resource_reservations in schedule.values():
    for res in resource_reservations:
        print_reservation(res)





# Test pond sending
#to_send      = schedule.values()[0][0]
#pond_block   = make_pond_block(to_send, semester_start)
#pond_block.save()

#pond_schedule = make_pond_schedule(schedule, semester_start)
#pond_schedule.save()



# make_pond_schedule(schedule, semester_start)


