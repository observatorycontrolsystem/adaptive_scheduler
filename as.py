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
                                                 epoch_to_datetime,
                                                 get_reservation_datetimes )
from adaptive_scheduler.printing        import ( print_resource_windows,
                                                 print_compound_reservations,
                                                 print_schedule)

from adaptive_scheduler.kernel.fullscheduler_v1 import FullScheduler_v1 as FullScheduler
from adaptive_scheduler.pond import Block

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

# Construct POND blocks, and then put them into the POND
#pond_blocks = make_simple_pond_schedule(schedule, semester_start)
#send_blocks_to_pond(pond_blocks)

# Try and build a complete POND block with what we've got
to_do = schedule['1m0a.doma.bpl'][0]
to_do_start, to_do_end = get_reservation_datetimes(to_do, semester_start)
block = Block(
               location = to_do.resource,
               start    = to_do_start,
               end      = to_do_end,
               group_id = 'PLACEHOLDER',
               priority = to_do.priority
             )

block.add_proposal(to_do.compound_request.proposal)
block.add_molecule(to_do.request.molecule)
block.add_target(to_do.request.target)

pond_block = block.send_to_pond()
