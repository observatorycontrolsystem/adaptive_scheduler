#!/usr/bin/env python
'''
as.py - Run the adaptive scheduler on continuous loop.

Example usage (testing)
    python as.py --requestdb='http://localhost:8001/'
    python as.py  -r http://localhost:8001/ -s 10 --dry-run --now '2013-10-01 00:00:00' --telescopes=telescopes-simulated_full_1m_network.dat

Example usage (production)
    python as.py --requestdb='http://scheduler-dev.lco.gtn/requestdb/'

Author: Eric Saunders
July 2012
'''
from __future__ import division


from adaptive_scheduler.eventbus         import get_eventbus
from adaptive_scheduler.feedback         import UserFeedbackLogger, TimingLogger
from adaptive_scheduler.printing         import pluralise as pl
from adaptive_scheduler.utils            import timeit, iso_string_to_datetime
from adaptive_scheduler.monitoring.network_status import Network
from adaptive_scheduler.orchestrator     import collapse_requests
from adaptive_scheduler.model2           import ModelBuilder, RequestError, n_requests
from adaptive_scheduler.request_filters  import set_rs_to_unschedulable, set_urs_to_unschedulable
from reqdb.client import SchedulerClient, ConnectionError

import argparse
from datetime import datetime, timedelta
import logging
import logging.config
import signal
import time
import sys
from reqdb.requests import Request

VERSION = '1.0.1'

# Set up and configure an application scope logger
#logging.config.fileConfig('logging.conf')
import logger_config
log = logging.getLogger('adaptive_scheduler')

# Set up signal handling for graceful shutdown
run_flag = True

def ctrl_c_handler(signal, frame):
    global run_flag
    print 'Received Ctrl+C - terminating on loop completion.'
    run_flag = False

def kill_handler(signal, frame):
    global run_flag
    print 'Received SIGTERM (kill) - terminating on loop completion.'
    run_flag = False

#signal.signal(signal.SIGINT, ctrl_c_handler)
#signal.signal(signal.SIGTERM, kill_handler)

# TODO: Write unit tests for these methods




def parse_args(argv):
    arg_parser = argparse.ArgumentParser(
                                formatter_class=argparse.RawDescriptionHelpFormatter,
                                description=__doc__)

    arg_parser.add_argument("-l", "--timelimit", type=float, default=None,
                            help="The time limit of the scheduler kernel, in seconds; negative implies no limit")
    arg_parser.add_argument("-i", "--horizon", type=float, default=7,
                            help="The scheduler's horizon, in days")
    arg_parser.add_argument("-z", "--slicesize", type=int, default=300,
                            help="The discretization size of the scheduler, in seconds")
    arg_parser.add_argument("-s", "--sleep", type=int, default=60,
                            help="Sleep period between scheduling runs, in seconds")
    arg_parser.add_argument("-r", "--requestdb", type=str, required=True,
                            help="Request DB endpoint URL")
    arg_parser.add_argument("-d", "--dry-run", action="store_true",
                            help="Perform a trial run with no changes made")
    arg_parser.add_argument("-n", "--now", type=str,
                            help="Alternative datetime to use as 'now', for running simulations (%%Y-%%m-%%d %%H:%%M:%%S)")
    arg_parser.add_argument("-t", "--telescopes", type=str, default='telescopes.dat',
                            help="Available telescopes file (default=telescopes.dat)")
    arg_parser.add_argument("-c", "--cameras", type=str, default='camera_mappings.dat',
                            help="Instrument description file (default=camera_mappings.dat)")
    arg_parser.add_argument("-w", "--noweather", action="store_true",
                            help="Disable weather checking")
    arg_parser.add_argument("--nosingles", action="store_true",
                                help="Ignore the 'single' Request type")
    arg_parser.add_argument("--nocompounds", action="store_true",
                                help="Ignore the 'and', 'oneof' and 'many' Request types")
    arg_parser.add_argument("--notoo", action="store_true",
                                help="Treat Target of Opportunity Requests like Normal Requests")
    arg_parser.add_argument("-o", "--run-once", action="store_true",
                            help="Only run the scheduling loop once, then exit")

    # Handle command line arguments
    args = arg_parser.parse_args(argv)

    if args.dry_run:
        log.info("Running in simulation mode - no DB changes will be made")
    log.info("Using available telescopes file '%s'", args.telescopes)
    log.info("Sleep period between scheduling runs set at %ds" % args.sleep)
    
    sched_params = RequestDBSchedulerParameters(**args)

    return sched_params


def determine_scheduler_now(args, short_run=False):
    '''Use a static command line datetime if provided, or default to utcnow, with a
       little extra to cover the scheduler's run time.'''
    if args.now:
        try:
            now = iso_string_to_datetime(args.now)
        except ValueError as e:
            log.critical(e)
            log.critical("Invalid datetime provided on command line. Try e.g. '2012-03-03 09:05:00'.")
            log.critical("Aborting scheduler run.")
            sys.exit()
    # ...otherwise offset 'now' to account for the duration of the scheduling run
    else:
        now = datetime.utcnow()

    log.info("Using a 'now' of %s", now)

    return now

def get_requests(scheduler_client, now):
    from adaptive_scheduler.requestdb     import get_requests_from_db
    # Try and get the requests
    semester_start, semester_end = get_semester_block(dt=now)
    try:
        requests = get_requests_from_db(scheduler_client.url, 'dummy arg',
                                        semester_start, semester_end)
        log.info("Got %d %s from Request DB", *pl(len(requests), 'User Request'))
        return requests

    except ConnectionError as e:
        log.warn("Error retrieving Requests from DB: %s", e)
        log.warn("Skipping this scheduling cycle")
        return []


def create_new_schedule(scheduler_client, sched_params, current_events):
    from adaptive_scheduler.orchestrator import run_scheduler
    # Use a static command line datetime if provided...
    now = determine_scheduler_now(sched_params);
    estimated_scheduler_end = now + timedelta(minutes=6)
    short_estimated_scheduler_end = now + timedelta(minutes=2)

    json_user_requests = get_requests(scheduler_client, short_estimated_scheduler_end)

    # Collapse each request tree
    json_user_requests = collapse_requests(json_user_requests)
    model_builder = sched_params.get_model_builder()

    all_user_requests = []
    for json_user_request in json_user_requests:
        try:
            user_request = model_builder.build_user_request(json_user_request)
            all_user_requests.append(user_request)
        except RequestError as e:
            log.warn(e)

    normal_user_requests = []
    too_user_requests    = []
    for ur in all_user_requests:
        if not sched_params.no_too and ur.has_target_of_opportunity():
            too_user_requests.append(ur)
        else:
            normal_user_requests.append(ur)

    log.info("Received %d ToO User Requests" % len(too_user_requests))
    log.info("Received %d Normal User Requests" % len(normal_user_requests))

    user_requests_dict = {
                          Request.NORMAL_OBSERVATION_TYPE : normal_user_requests,
                          Request.TARGET_OF_OPPORTUNITY : too_user_requests
                          }
    
    semester_start, semester_end = get_semester_block(dt=short_estimated_scheduler_end)
    if too_user_requests:
        log.info("Start ToO Scheduling")
        user_requests_dict['type'] = Request.TARGET_OF_OPPORTUNITY
        n_urs, n_rs = n_requests(too_user_requests)
        
        try:
            scheduler_run = PondSchedulerRun(sched_params, now, short_estimated_scheduler_end, semester_end)
            
            
            visibility_from, new_schedule, tels_to_cancel, unschedulable_ur_numbers, unschedulable_r_numbers = run_scheduler(user_requests_dict,
                                            now, short_estimated_scheduler_end,
                                            semester_start, semester_end,
                                            current_events, visibility_from,
                                            scheduler_run)
            
            if not sched_params.dry_run:
                # Set the states of the Requests and User Requests
                set_rs_to_unschedulable(scheduler_client, unschedulable_r_numbers)
                set_urs_to_unschedulable(scheduler_client, unschedulable_ur_numbers)
            
            # Delete old schedule
            n_deleted = scheduler_run.cancel(short_estimated_scheduler_end, semester_end, sched_params.dry_run, tels_to_cancel)
            
            # Write new schedule
            n_submitted = scheduler_run.save(new_schedule, semester_start, sched_params.camreras_file, sched_params.dry_run)
            write_scheduling_log(n_urs, n_rs, n_deleted, n_submitted, sched_params.dry_run)
        except ScheduleException, pfe:
            log.error(pfe, "aborting run")
            
        log.info("End ToO Scheduling")

    # Run the scheduling loop, if there are any User Requests
    if normal_user_requests:
        log.info("Start Normal Scheduling")
        user_requests_dict['type'] = Request.NORMAL_OBSERVATION_TYPE

        visibility_from, schedule = run_scheduler(user_requests_dict, scheduler_client,
                                        now, estimated_scheduler_end,
                                        semester_start, semester_end,
                                        args.telescopes, args.cameras,
                                        current_events, visibility_from,
                                        dry_run=args.dry_run,
                                        no_weather=args.noweather,
                                        no_singles=args.nosingles,
                                        no_compounds=args.nocompounds,
                                        slicesize=args.slicesize,
                                        timelimit=args.timelimit,
                                        horizon=args.horizon)
        log.info("End Normal Scheduling")

    else:
        log.warn("Received no User Requests! Skipping this scheduling cycle")
    sys.stdout.flush()


    return visibility_from


def write_scheduling_log(n_urs, n_rs, n_deleted, n_submitted, dry_run=False):
    log.info("------------------")
    log.info("Scheduling Summary")
    if dry_run:
        log.info("(DRY-RUN: No delete or submit took place)")
    log.info("------------------")
    log.info("Received %s (%s) from Request DB", pl(n_urs, 'User Request'),
                                                       pl(n_rs, 'Request'))
    log.info("In total, deleted %d previously scheduled %s", *pl(n_deleted, 'block'))
    log.info("Submitted %d new %s to the POND", *pl(n_submitted, 'block'))
    log.info("Scheduling complete.")










def main(argv):
    sched_params = parse_args(argv)

    log.info("Starting Adaptive Scheduler, version {v}".format(v=VERSION))

    if sched_params.dry_run:
        import lcogtpond
        lcogtpond._service_host = 'localhost'

    event_bus = get_eventbus()
    user_feedback_logger = UserFeedbackLogger()
    timing_logger        = TimingLogger()
    event_bus.add_listener(user_feedback_logger, persist=True)
    event_bus.add_listener(timing_logger, persist=True,
                           event_type=TimingLogger._StartEvent)
    event_bus.add_listener(timing_logger, persist=True,
                           event_type=TimingLogger._EndEvent)
    
    scheduler = RequestDBScheduler(sched_params, Network())
    scheduler.run()


if __name__ == '__main__':
    main(sys.argv[1:])

