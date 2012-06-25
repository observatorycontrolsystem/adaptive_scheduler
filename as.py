#!/usr/bin/env python

'''
as.py - Skeleton adaptive scheduler

Author: Eric Saunders
November 2011
'''

# Required for true (non-integer) division
from __future__ import division

from adaptive_scheduler.input           import ( get_telescope_network,
                                                 get_requests_from_files,
                                                 dump_scheduler_input)
from adaptive_scheduler.kernel_mappings import ( make_compound_reservations,
                                                 construct_resource_windows,
                                                 construct_visibilities )
from adaptive_scheduler.kernel.metricsprescheduler import ( MetricsPreSchedulerScalar,
                                                            MetricsPreSchedulerVector)
from adaptive_scheduler.utils           import ( datetime_to_normalised_epoch,
                                                 epoch_to_datetime )
from adaptive_scheduler.printing        import ( print_resource_windows,
                                                 print_compound_reservations,
                                                 print_schedule )

from adaptive_scheduler.kernel.fullscheduler_v2 import FullScheduler_v2 as FullScheduler
from adaptive_scheduler.pond import Block, send_schedule_to_pond

from lcogt.pond import pond_client
pond_client.configure_service('localhost', 12345)

from datetime import datetime


# TODO: Move this
from adaptive_scheduler.utils import datetime_to_epoch, normalised_epoch_to_datetime
def convert_coverage_to_dmy(coverage, semester_start):

    epoch_start = datetime_to_epoch(semester_start)

    dt_coverage = []
    for interval in coverage:
        dt_start = normalised_epoch_to_datetime(interval[0], epoch_start)
        dt_end   = normalised_epoch_to_datetime(interval[1], epoch_start)

        dt_interval = (dt_start, dt_end, interval[2])

        dt_coverage.append(dt_interval)

    return dt_coverage


def increment_dict_by_value(dictionary, key, value):
    if key in dictionary:
        dictionary[key] += value
    else:
        dictionary[key]  = value

    return


def sum_contended_datetimes(dt_coverage):

    contended_dts = {}
    for interval in dt_coverage:
        contended = interval[2]
        if contended:
            increment_dict_by_value(contended_dts, str(interval[0].date()), contended)
            increment_dict_by_value(contended_dts, str(interval[1].date()), contended)

    return contended_dts


import os.path
def dump_metric(metric, metric_name, dump_dir):
    dump_file = metric_name + '.dat'
    dump_path = os.path.join(dump_dir, dump_file)

    dump_fh = open(dump_path, 'w')


    timestamped_wrapper = {
                            'recorded_at' : str(datetime.utcnow()),
                            'metric_name' : metric
                          }
    print >> dump_fh, timestamped_wrapper

    dump_fh.close()

    return


# Configuration files
tel_file      = 'telescopes.dat'
target_file   = 'targets.dat'
proposal_file = 'proposals.dat'
molecule_file = 'molecules.dat'
request_file  = 'requests.dat'

# Output file summarising what is to be scheduled
scheduler_dump_file = 'to_schedule.pickle'

# TODO: Replace with config file (from laptop)
semester_start = datetime(2011, 11, 1, 0, 0, 0)
semester_end   = datetime(2011, 11, 8, 0, 0, 0)

# TODO: We should get the full network resource hierarchy, and be able to use that
#       to approve or deny requests
tels = get_telescope_network(tel_file)

# Build the requests from input files
compound_requests = get_requests_from_files(tel_file, target_file, proposal_file,
                                            molecule_file, request_file,
                                            semester_start, semester_end)

# Construct visibility objects for each telescope
visibility_from = construct_visibilities(tels, semester_start, semester_end)

# Translate when telescopes are available into kernel speak
resource_windows = construct_resource_windows(visibility_from, semester_start)

# For info, print out the details of the resource windows
print_resource_windows(resource_windows)

# Convert CompoundRequests -> CompoundReservations
to_schedule = make_compound_reservations(compound_requests, visibility_from,
                                         semester_start)

# For info, summarise the CompoundReservations available to schedule
print_compound_reservations(to_schedule)

# Dump the variables to be scheduled, for offline analysis if necessary
contractual_obligation_list = []
dump_scheduler_input(scheduler_dump_file, to_schedule, resource_windows,
                     contractual_obligation_list)


# Instantiate a scheduler
scheduler = FullScheduler(to_schedule, resource_windows,
                          contractual_obligation_list)

# Run the scheduler
schedule = scheduler.schedule_all()

# Summarise the schedule in normalised epoch (kernel) units of time
print_schedule(schedule, semester_start, semester_end)

# Calculate some metrics
pre_scalar = MetricsPreSchedulerScalar(to_schedule, resource_windows,
                                       contractual_obligation_list)
n_cr        = pre_scalar.get_number_of_compound_reservations()
n_cr_single = pre_scalar.get_number_of_compound_reservations('single')
n_cr_and    = pre_scalar.get_number_of_compound_reservations('and')
n_cr_oneof  = pre_scalar.get_number_of_compound_reservations('oneof')

print "Pre scalar: n_crs: %d"          % n_cr
print "Pre scalar: n_crs 'single': %d" % n_cr_single
print "Pre scalar: n_crs 'and': %d"    % n_cr_and
print "Pre scalar: n_crs 'oneof': %d"  % n_cr_oneof


pre_vector = MetricsPreSchedulerVector(to_schedule, resource_windows,
                                       contractual_obligation_list)

resource_contention = {}
for resource in resource_windows.keys():
    coverage = pre_vector.get_coverage_by_resource(resource, 'count')
    print "Pre vector: coverage: %s" % coverage

    dt_coverage = convert_coverage_to_dmy(coverage, semester_start)
    print "Pre vector: dt_coverage: %s" % dt_coverage

    contended_dts = sum_contended_datetimes(dt_coverage)
    print "Contended dts:"
    for dt in contended_dts:
        print "    ", dt, contended_dts[dt]

    resource_contention[resource] = contended_dts


metric_dir = '/home/esaunderslocal/projects/schedule_viewer/schedule_viewer/metrics'
dump_metric(resource_contention, 'resource_contention', metric_dir)




# Convert the kernel schedule into POND blocks, and send them to the POND
#send_schedule_to_pond(schedule, semester_start)

