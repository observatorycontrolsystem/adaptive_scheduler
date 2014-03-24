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
from datetime import datetime, timedelta

from adaptive_scheduler.request_parser  import TreeCollapser
from adaptive_scheduler.tree_walker     import RequestMaxDepthFinder
from adaptive_scheduler.model2          import ( filter_out_compounds, ModelBuilder,
                                                 differentiate_by_type, n_requests,
                                                  )
from adaptive_scheduler.kernel_mappings import ( construct_visibilities,
                                                 construct_resource_windows,
                                                 make_compound_reservations,
                                                 make_many_type_compound_reservations,
                                                 filter_for_kernel,
                                                 construct_global_availability )
from adaptive_scheduler.printing import ( print_schedule, print_compound_reservations,
                                          summarise_urs, log_full_ur, log_windows)
from adaptive_scheduler.printing import pluralise
from adaptive_scheduler.printing import plural_str as pl
from adaptive_scheduler.pond     import ( send_schedule_to_pond, cancel_schedule,
                                          blacklist_running_blocks,
                                          PondFacadeException )
from adaptive_scheduler.semester_service import get_semester_code

#from adaptive_scheduler.kernel.fullscheduler_v6 import FullScheduler_v6 as FullScheduler
from adaptive_scheduler.kernel.fullscheduler_gurobi import FullScheduler_gurobi as FullScheduler
from adaptive_scheduler.request_filters import filter_and_set_unschedulable_urs
from adaptive_scheduler.eventbus        import get_eventbus
from adaptive_scheduler.feedback        import TimingLogger
from adaptive_scheduler.utils import timeit
from adaptive_scheduler.log   import UserRequestLogger
from adaptive_scheduler.event_utils import report_scheduling_outcome

from reqdb.client import SearchQuery, SchedulerClient
from reqdb        import request_factory


# Set up and configure a module scope logger, and a UR-specific logger
import logging
log          = logging.getLogger(__name__)
multi_ur_log = logging.getLogger('ur_logger')

ur_log = UserRequestLogger(multi_ur_log)

event_bus = get_eventbus()

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
def get_requests_from_db(url, telescope_class, sem_start, sem_end):
    format = '%Y-%m-%d %H:%M:%S'

    search = SearchQuery()
    search.set_date(start=sem_start.strftime(format), end=sem_end.strftime(format))

    log.info("Asking DB (%s) for User Requests between %s and %s", url, sem_start, sem_end)
    sc  = SchedulerClient(url)

    ur_list = sc.retrieve(search, debug=True)

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
#    kernel_dump_fh.write(jsonpickle.encode(json_dump))
    kernel_dump_fh.close()
    log.info("Wrote kernel input dump to %s", kernel_dump_file)

    return


def dump_kernel_input2(to_schedule, global_windows, contractual_obligations, time_slicing_dict):
    args_filename = 'input_args.%s.tmp' % datetime.utcnow().strftime(format = '%Y-%m-%d_%H_%M_%S')

    args_fh = open(args_filename, 'w')
    print "Dumping kernel args to %s" % args_filename

    to_schedule_serial = [x.serialise() for x in to_schedule]
    global_windows_serial = dict([(k, v.serialise()) for k,v in global_windows.items()])

    args_fh.write(json.dumps({
                                     'to_schedule' : to_schedule_serial,
                                     'global_windows' : global_windows_serial,
                                     'contractual_obligations' : contractual_obligations,
                                     'time_slicing_dict' : time_slicing_dict
                                     }))
    args_fh.close()

    return


def open_debugger_on_unusual_run(schedule):
    size = 0
    for res in schedule:
        size += len(schedule[res])
    if size != 30:
        import ipdb; ipdb.set_trace()

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
#                log.debug("Request %d has depth %d - continuing.", i,
#                                                                  depth_finder.max_depth)
                collapsed_reqs.append(tc.collapsed_tree)

        else:
            log.debug("Request %d could not be collapsed - skipping.", i)


    return collapsed_reqs


def update_telescope_events(tels, current_events):

    for telescope_name, telescope in tels.iteritems():
        if telescope_name in current_events:
            telescope.events.extend(current_events[telescope_name])
            msg = "Found network event for '%s' - removing from consideration (%s)" % (
                                                                telescope_name,
                                                                current_events[telescope_name])
            log.info(msg)

    return




# TODO: refactor into smaller chunks
@timeit
def run_scheduler(user_reqs, sched_client, now, semester_start, semester_end, tel_file,
                  camera_mappings_file, current_events, visibility_from=None, dry_run=False,
                  no_weather=False, no_singles=False, no_compounds=False, slicesize=300, 
                  timelimit=300.0, horizon=7.0):

    start_event = TimingLogger.create_start_event(datetime.utcnow())
    event_bus.fire_event(start_event)

    ONE_MONTH = timedelta(weeks=4)
    ONE_WEEK  = timedelta(weeks=1)
    scheduling_horizon = now + timedelta(days=horizon)
    date_fmt      = '%Y-%m-%d'
    date_time_fmt = '%Y-%m-%d %H:%M:%S'

    log.info("Scheduling for semester %s (%s to %s)", get_semester_code(),
                                                     semester_start.strftime(date_fmt),
                                                     semester_end.strftime(date_fmt))
    log.info("Scheduling horizon is %s", scheduling_horizon.strftime(date_time_fmt))

    log.info("Received %s from Request DB", pl(len(user_reqs), 'User Request'))

    scheduler_dump_file = 'to_schedule.pickle'

    # Summarise the User Requests we've received
    n_urs, n_rs = n_requests(user_reqs)

    log.info("Deserialised %s (%s) from Request DB", pl(n_urs, 'User Request'),
                                                     pl(n_rs, 'Request'))

    summarise_urs(user_reqs, log_msg="Received from Request DB")
    for ur in user_reqs:
        log_full_ur(ur, now)
        log_windows(ur, log_msg="Initial windows:")


    if no_singles:
        log.info("Compound Request support (single) disabled at the command line")
        log.info("Compound Requests of type 'single' will be ignored")
        singles, others = differentiate_by_type(cr_type='single', crs=user_reqs)
        user_reqs = others

    if no_compounds:
        log.info("Compound Request support (and/oneof/many) disabled at the command line")
        log.info("Compound Requests of type 'and', 'oneof' or 'many' will be ignored")
        user_reqs = filter_out_compounds(user_reqs)

    # TODO: Swap to tels2
    mb = ModelBuilder(tel_file, camera_mappings_file)
    tels = mb.tel_network.telescopes
    log.info("Available telescopes:")
    for t in sorted(tels):
        log.info(str(t))

    # Look for weather events unless weather monitoring has been disabled
    if no_weather:
        log.info("Weather monitoring disabled on the command line")
    else:
        update_telescope_events(tels, current_events)

    # Filter by window, and set UNSCHEDULABLE on the Request DB as necessary
    log.info("Filtering for unschedulability")
    user_reqs = filter_and_set_unschedulable_urs(sched_client, user_reqs, now, dry_run)
    log.info("Completed unschedulable filters")
    summarise_urs(user_reqs, log_msg="Passed unschedulable filters:")

    for ur in user_reqs:
        log_windows(ur, log_msg="Remaining windows:")

    # Construct visibility objects for each telescope
    log.info("Constructing telescope visibilities")
    if not visibility_from:
        visibility_from = construct_visibilities(tels, semester_start, semester_end)


    # Do another check on duration and operator soundness, after dark/rise checking
    log.info("Filtering on dark/rise_set")

    for tel_name, tel in tels.iteritems():
        if tel.events:
            log.info("Bypassing visibility calcs for %s" % tel_name)

    visible_urs = filter_for_kernel(user_reqs, visibility_from, tels,
                                    now, semester_end, scheduling_horizon)


    log.info("Completed dark/rise_set filters")
    summarise_urs(visible_urs, log_msg="Passed dark/rise filters:")
    for ur in visible_urs:
        log_windows(ur, log_msg="Remaining windows:")

    log.info('Filtering complete. Ready to construct Reservations from %d URs.' % len(visible_urs))

    # Remove running blocks from consideration, and get the availability edge
    try:
        visible_urs, running_at_tel = blacklist_running_blocks(visible_urs, tels, now, semester_end)
    except PondFacadeException as e:
        log.error("Could not determine running blocks from POND - aborting run")
        return visibility_from

    # Convert CompoundRequests -> CompoundReservations
    many_urs, other_urs = differentiate_by_type('many', visible_urs)
    to_schedule_many  = make_many_type_compound_reservations(many_urs, tels, visibility_from,
                                                            semester_start)
    to_schedule_other = make_compound_reservations(other_urs, tels, visibility_from,
                                                   semester_start)
    to_schedule = to_schedule_many + to_schedule_other

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
    # TODO: Move this to a config file
    time_slicing_dict = {}
    for t in tels:
        time_slicing_dict[t] = [0,slicesize]

    contractual_obligations = []

    log.info("Instantiating and running kernel")

    kernel   = FullScheduler(to_schedule, global_windows, contractual_obligations,
                             time_slicing_dict)

    schedule = kernel.schedule_all(timelimit=timelimit)

    scheduled_reservations = []
    [scheduled_reservations.extend(a) for a in schedule.values()]
    log.info("Scheduling completed. Final schedule has %d Reservations." % len(scheduled_reservations))

    report_scheduling_outcome(to_schedule, scheduled_reservations)


    # Summarise the schedule in normalised epoch (kernel) units of time
    print_schedule(schedule, semester_start, semester_end)

    # Clean out all existing scheduled blocks
    try:
        n_deleted = cancel_schedule(tels, now, semester_end, dry_run)
    except PondFacadeException as e:
        log.error("Could not cancel schedule - aborting run")
        return visibility_from

    # Convert the kernel schedule into POND blocks, and send them to the POND
    n_submitted = send_schedule_to_pond(schedule, semester_start,
                                        camera_mappings_file, dry_run)

    log.info("------------------")
    log.info("Scheduling Summary")
    if dry_run:
        log.info("(DRY-RUN: No delete or submit took place)")
    log.info("------------------")
    log.info("Received %s (%s) from Request DB", pl(n_urs, 'User Request'),
                                                       pl(n_rs, 'Request'))
    log.info("In total, deleted %d previously scheduled %s", *pluralise(n_deleted, 'block'))
    log.info("Submitted %d new %s to the POND", *pluralise(n_submitted, 'block'))
    log.info("Scheduling complete.")

    end_event = TimingLogger.create_end_event(datetime.utcnow())
    event_bus.fire_event(end_event)


    return visibility_from

