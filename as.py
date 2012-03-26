#!/usr/bin/env python

'''
as.py - Skeleton adaptive scheduler

Author: Eric Saunders
November 2011
'''

# Required for true (non-integer) division
from __future__ import division

from adaptive_scheduler.input           import ( build_telescopes, build_targets,
                                                 build_proposals, build_molecules,
                                                 build_compound_requests )
from adaptive_scheduler.kernel_mappings import ( make_compound_reservations,
                                                 construct_resource_windows,
                                                 construct_visibilities)
from adaptive_scheduler.utils           import ( datetime_to_normalised_epoch,
                                                 epoch_to_datetime )
from adaptive_scheduler.printing        import ( print_resource_windows,
                                                 print_compound_reservations,
                                                 print_schedule)

from adaptive_scheduler.kernel.fullscheduler_v1 import FullScheduler_v1 as FullScheduler
from adaptive_scheduler.pond import Block, send_schedule_to_pond

from lcogt.pond import pond_client
pond_client.configure_service('localhost', 12345)

from datetime import datetime


# Configuration files
tel_file      = 'telescopes.dat'
target_file   = 'targets.dat'
proposal_file = 'proposals.dat'
molecule_file = 'molecules.dat'
request_file  = 'requests.dat'

# TODO: Replace with config file (from laptop)
semester_start = datetime(2011, 11, 1, 0, 0, 0)
semester_end   = datetime(2011, 11, 8, 0, 0, 0)

# Create telescopes, targets, proposals and requests from input files
tels      = build_telescopes(tel_file)
targets   = build_targets(target_file)
proposals = build_proposals(proposal_file)
molecules = build_molecules(molecule_file)

# Configure a preprocessor to handle telescope classes in requests


# Combine the input information to reconstitute the actual compound requests
compound_requests = build_compound_requests(request_file, targets, tels, proposals,
                                            molecules, semester_start, semester_end)

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

# Instantiate a scheduler
scheduler = FullScheduler(to_schedule, resource_windows,
                          contractual_obligation_list=[])

# Run the scheduler
schedule = scheduler.schedule_all()

# Summarise the schedule in normalised epoch (kernel) units of time
print_schedule(schedule, semester_start, semester_end)

# Convert the kernel schedule into POND blocks, and send them to the POND
send_schedule_to_pond(schedule, semester_start)

