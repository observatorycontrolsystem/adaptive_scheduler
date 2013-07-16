#!/usr/bin/env python
'''
orchestrator.py - Top-level entry-point to the scheduler

This module provides main(), the top-level running function of the scheduler. It may
be called standalone for single-thread testing purposes, or embedded in a threaded
environoment for persistent execution.

Author: Eric Saunders
August 2012
'''
from __future__ import division

import sys
import json
import ast
from datetime import datetime

from adaptive_scheduler.request_parser  import TreeCollapser
from adaptive_scheduler.tree_walker     import RequestMaxDepthFinder
from adaptive_scheduler.model2          import ModelBuilder
from adaptive_scheduler.kernel_mappings import ( construct_visibilities,
                                                 construct_resource_windows,
                                                 make_compound_reservations,
                                                 prefilter_for_kernel,
                                                 construct_global_availability )
from adaptive_scheduler.input    import ( get_telescope_network, dump_scheduler_input )
from adaptive_scheduler.printing import ( print_schedule, print_compound_reservations )
from adaptive_scheduler.printing import pluralise as pl
from adaptive_scheduler.pond     import ( send_schedule_to_pond, cancel_schedule,
                                          blacklist_running_blocks )
from adaptive_scheduler.semester_service import get_semester_block, get_semester_code

from adaptive_scheduler.kernel.fullscheduler_v5 import FullScheduler_v5 as FullScheduler
from adaptive_scheduler.request_filters import filter_and_set_unschedulable_urs
from adaptive_scheduler.utils import timeit
from adaptive_scheduler.log   import UserRequestLogger, UserRequestHandler

from reqdb.client import SearchQuery, SchedulerClient
from reqdb        import request_factory


# Set up and configure a module scope logger, and a UR-specific logger
import logging
log          = logging.getLogger(__name__)
multi_ur_log = logging.getLogger('ur_logger')

ur_log = UserRequestLogger(multi_ur_log)


#TODO: Refactor - move all these functions to better locations
def get_requests(url, telescope_class):

    rc  = RetrievalClient(url)
    rc.set_location(telescope_class)

    json_req_str = rc.retrieve()
    requests     = json.loads(json_req_str)

    return requests


def get_requests_from_file(req_filename, telescope_class):

    with open(req_filename, 'r') as req_fh:
        req_data = req_fh.read()
        return ast.literal_eval(req_data)

def get_requests_from_json(req_filename, telescope_class):

    with open(req_filename, 'r') as req_fh:
        req_data = req_fh.read()
        return json.loads(req_data)

@timeit
def get_requests_from_db(url, telescope_class):
    sem_start, sem_end = get_semester_block()
    format = '%Y-%m-%d %H:%M:%S'

    search = SearchQuery()
    search.set_date(start=sem_start.strftime(format), end=sem_end.strftime(format))

    log.info("Asking DB (%s) for User Requests between %s and %s", url, sem_start, sem_end)
    sc           = SchedulerClient(url)

    ur_list = sc.retrieve(search, debug=True)

    in_fh = open('retrieved_urs.dat', 'r')
    ur_list = json.load(in_fh)

    return ur_list


def write_requests_to_file_as_json(ur_list, filename):
    out_fh = open(filename, 'w')
    json.dump(ur_list, out_fh)
    out_fh.close()


def write_requests_to_file(requests, filename):
    out_fh = open(filename, 'w')
    out_fh.write(str(requests))
    out_fh.close()


def dump_kernel_input(to_schedule, resource_windows, contractual_obligations,
                      time_slicing_dict):
    json_dump = {
                  'to_schedule' : to_schedule,
                  'resource_windows' : resource_windows,
                  'contractual_obligations' : contractual_obligations,
                  'time_slicing_dict' : time_slicing_dict
                }


    kernel_dump_file = 'kernel.dump'
    kernel_dump_fh = open(kernel_dump_file, 'w')
    kernel_dump_fh.write(jsonpickle.encode(json_dump))
    kernel_dump_fh.close()
    log.info("Wrote kernel input dump to %s", kernel_dump_file)

    return


def collapse_requests(requests):
    collapsed_reqs = []
    for i, req_dict in enumerate(requests):

        tc = TreeCollapser(req_dict)
        tc.collapse_tree()

        if tc.is_collapsible:
            log.debug("Request %d was successfully collapsed!", i)

            depth_finder = RequestMaxDepthFinder(tc.collapsed_tree)
            depth_finder.walk()

            # The scheduling kernel can't handle more than one level of nesting
            if depth_finder.max_depth > 1:
                log.debug("Request %d is still too deep (%d levels) - skipping.", i,
                                                                  depth_finder.max_depth)

            else:
                log.debug("Request %d has depth %d - continuing.", i,
                                                                  depth_finder.max_depth)
                collapsed_reqs.append(tc.collapsed_tree)

        else:
            log.debug("Request %d could not be collapsed - skipping.", i)


    return collapsed_reqs


def summarise_urs(user_reqs):
    log.debug("User Request breakdown:")
    for ur in user_reqs:
        r_nums  = [r.request_number for r in ur.requests]
        w_total = sum([r.n_windows() for r in ur.requests])
        _, w_str = pl(w_total, 'Window')
        r_total, r_str = pl(len(ur.requests), 'Request')
        r_states = [r.received_state for r in ur.requests]

        sum_str = '%s: %s (%d %s, %d %s) %s'
        log.debug(sum_str, ur.tracking_number, r_nums,
                  r_total, r_str, w_total, w_str, r_states)
        ur_log.error(sum_str % (ur.tracking_number, r_nums,
                     r_total, r_str, w_total, w_str, r_states), ur.tracking_number)
        ur_log.info('your face is dumb', ur.tracking_number)
        ur_log.error('your error face is dumb', ur.tracking_number)

    return


# TODO: Add configuration options, refactor into smaller chunks
# TODO: Remove hard-coded options
@timeit
def main(requests, sched_client, visibility_from=None, dry_run=False):
    semester_start, semester_end = get_semester_block()
#    now = datetime.utcnow()
    now = datetime(2013, 7, 9, 22, 46, 13, 870821)
    date_fmt = '%Y-%m-%d'

    log.info("Scheduling for semester %s (%s to %s)", get_semester_code(),
                                                     semester_start.strftime(date_fmt),
                                                     semester_end.strftime(date_fmt))

    log.info("Received %d %s from Request DB", *pl(len(requests), 'User Request'))

    # Collapse each request tree
    collapsed_reqs = collapse_requests(requests)

    # Configuration files
    tel_file            = 'telescopes.dat'
    scheduler_dump_file = 'to_schedule.pickle'

    mb = ModelBuilder(tel_file)

#    user_reqs = []
#    for serialised_ur in collapsed_reqs:
#        proposal_data = serialised_ur['proposal']
#        del(serialised_ur['proposal'])
#        ur = request_factory.parse(serialised_ur, proposal_data)
#        user_reqs.append(ur)

    user_reqs = []
    for serialised_req in collapsed_reqs:
        user_req = mb.build_user_request(serialised_req)
        user_reqs.append(user_req)

    # Summarise the User Requests we've received
    summarise_urs(user_reqs)

    # TODO: Swap to tels2
    tels = mb.tel_network.telescopes
    log.debug("Available telescopes:")
    for t in sorted(tels):
        log.debug(str(t))


    # Filter by window, and set UNSCHEDULABLE on the Request DB as necessary
    user_reqs = filter_and_set_unschedulable_urs(sched_client, user_reqs, now, dry_run)

    # Construct visibility objects for each telescope
    if not visibility_from:
        visibility_from = construct_visibilities(tels, semester_start, semester_end)


    # Do another check on duration and operator soundness, after dark/rise checking
    user_reqs = prefilter_for_kernel(user_reqs, visibility_from, tels,
                                     now, semester_end)

    log.info('Filtering complete. Ready to construct Reservations from %d URs.' % len(user_reqs))
    #TODO:Remove me
    debug_fh = open('run.tmp', 'w')
    debug_fh.write('After prefilter: %d URs\n' % len(user_reqs))
    summarise_urs(user_reqs)

    # Remove running blocks from consideration, and get the availability edge
    user_reqs, running_at_tel = blacklist_running_blocks(user_reqs, tels, now, semester_end)

    # Convert CompoundRequests -> CompoundReservations
    to_schedule = make_compound_reservations(user_reqs, visibility_from,
                                             semester_start)
    debug_fh.write('Made: %d compound reservations\n' % len(to_schedule))

    # Translate when telescopes are available into kernel speak
    resource_windows = construct_resource_windows(visibility_from, semester_start)

    # Intersect and mask out time where Blocks are currently running
    global_windows = construct_global_availability(now, semester_start,
                                                   running_at_tel, resource_windows)

    print_compound_reservations(to_schedule)


    if not to_schedule:
        log.info("Nothing to schedule! Skipping kernel call...")
        return


    # Instantiate and run the scheduler
    # TODO: Set alignment to start of first night on each resource
    # TODO: Move this to a config file
    time_slicing_dict = {}
    for t in tels:
        time_slicing_dict[t] = [0, 300]

    contractual_obligations = []

    args_filename = 'input_args.%s.tmp' % datetime.utcnow().strftime(format = '%Y-%m-%d_%H_%M_%S')

    args_fh = open(args_filename, 'w')
    print "Dumping kernel args to %s" % args_filename
    to_schedule_serial = [x.serialise() for x in to_schedule]
    global_windows_serial = dict([(k, v.serialise()) for k,v in global_windows.items()])

    args_fh.write(json.dumps({
                                     'to_schedule' : to_schedule_serial,
#                                     'global_windows' : global_windows_serial,
#                                     'contractual_obligations' : contractual_obligations,
#                                     'time_slicing_dict' : time_slicing_dict
                                     }))
    args_fh.close()

    #print "Size of to_schedule", len(to_schedule)
    #total_tps = 0
    #for tel in global_windows:
    #    total_tps += len(global_windows[tel].timepoints)
    #print "Size of global_windows", total_tps

    kernel   = FullScheduler(to_schedule, global_windows, contractual_obligations,
                             time_slicing_dict)
    schedule = kernel.schedule_all()
    size = 0
    for res in schedule:
        size += len(schedule[res])
    print "1st time:", size
    if size != 30:
        import ipdb; ipdb.set_trace()


    x = []
    [x.extend(a) for a in schedule.values()]
    log.info("Scheduling completed. Final schedule has %d Reservations." % len(x))

    # Summarise the schedule in normalised epoch (kernel) units of time
    print_schedule(schedule, semester_start, semester_end)

    # Clean out all existing scheduled blocks
    n_deleted = cancel_schedule(tels, now, semester_end, dry_run)

    # Convert the kernel schedule into POND blocks, and send them to the POND
    size = 0
    for res in schedule:
        size += len(schedule[res])
    print "2nd time:", size
    n_submitted = send_schedule_to_pond(schedule, semester_start, dry_run)

    log.info("------------------")
    log.info("Scheduling Summary")
    if dry_run:
        log.info("(DRY-RUN: No delete or submit took place)")
    log.info("------------------")
    log.info("Received %d %s from Request DB", *pl(len(requests), 'User Request'))
    log.info("In total, deleted %d previously scheduled %s", *pl(n_deleted, 'block'))
    log.info("Submitted %d new %s to the POND", *pl(n_submitted, 'block'))
    log.info("Scheduling complete.")

    return visibility_from

