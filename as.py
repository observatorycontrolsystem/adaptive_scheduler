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
from adaptive_scheduler.semester_service import get_semester_block
from adaptive_scheduler.monitoring.network_status import Network
from adaptive_scheduler.orchestrator     import collapse_requests
from adaptive_scheduler.model2           import ModelBuilder, RequestError
from reqdb.client import SchedulerClient, ConnectionError

import argparse
from datetime import datetime, timedelta
import logging
import logging.config
import signal
import time
import sys

VERSION = '1.2.2'

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

@timeit
def get_dirty_flag(scheduler_client):
    dirty_response = dict(dirty=False)
    try:
        dirty_response = scheduler_client.get_dirty_flag()
    except ConnectionError as e:
        log.warn("Error retrieving dirty flag from DB: %s", e)
        log.warn("Skipping this scheduling cycle")

    #TODO: HACK to handle not a real error returned from Request DB
    if request_db_dirty_flag_is_invalid(dirty_response):
        dirty_response = dict(dirty=False)

    if dirty_response['dirty'] is False:
        log.info("Request DB is still clean - nothing has changed")

    else:
        msg  = "Got dirty flag (DB needs reading) with timestamp"
        msg += " %s (last updated %s)" % (dirty_response['timestamp'],
                                          dirty_response['last_updated'])
        log.info(msg)

    return dirty_response


def parse_args(argv):
    arg_parser = argparse.ArgumentParser(
                                formatter_class=argparse.RawDescriptionHelpFormatter,
                                description=__doc__)

    arg_parser.add_argument("-l", "--timelimit", type=int, default=300,
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
    arg_parser.add_argument("-o", "--run-once", action="store_true",
                            help="Only run the scheduling loop once, then exit")

    # Handle command line arguments
    args = arg_parser.parse_args(argv)

    if args.dry_run:
        log.info("Running in simulation mode - no DB changes will be made")
    log.info("Using available telescopes file '%s'", args.telescopes)
    log.info("Sleep period between scheduling runs set at %ds" % args.sleep)

    return args


def determine_scheduler_now(args):
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
        now = datetime.utcnow() + timedelta(minutes=6)

    log.info("Using a 'now' of %s", now)

    return now


def request_db_dirty_flag_is_invalid(dirty_response):
    try:
        dirty_response['dirty']
        return False
    except TypeError as e:
        log.critical("Request DB could not update internal state. Aborting current scheduling loop.")
        return True


def clear_dirty_flag(scheduler_client, args):
    # Clear the dirty flag
    log.info("Clearing dirty flag")
    try:
        scheduler_client.clear_dirty_flag()
        return True
    except ConnectionError as e:
        log.critical("Error clearing dirty flag on DB: %s", e)
        log.critical("Aborting current scheduling loop.")
        log.info(" Sleeping for %d seconds", args.sleep)
        time.sleep(args.sleep)

        return False


def get_requests(scheduler_client, now):
    from adaptive_scheduler.orchestrator     import get_requests_from_db
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


def create_new_schedule(scheduler_client, args, visibility_from, current_events):
    from adaptive_scheduler.orchestrator import run_scheduler
    # Use a static command line datetime if provided...
    now = determine_scheduler_now(args)

    json_user_requests = get_requests(scheduler_client, now)

    # Collapse each request tree
    json_user_requests = collapse_requests(json_user_requests)
    mb = ModelBuilder(args.telescopes, args.cameras)

    all_user_requests = []
    for json_user_request in json_user_requests:
        try:
            user_request = mb.build_user_request(json_user_request)
            all_user_requests.append(user_request)
        except RequestError as e:
            log.warn(e)

    normal_user_requests = []
    too_user_requests    = []
    for ur in all_user_requests:
        if ur.has_target_of_opportunity():
            too_user_requests.append(ur)
        else:
            normal_user_requests.append(ur)

    log.info("Received %d ToO User Requests" % len(too_user_requests))
    log.info("Received %d Normal User Requests" % len(normal_user_requests))

    if too_user_requests:
        # TODO: Do a pre run scheduling all too requests first
        pass

    # Run the scheduling loop, if there are any User Requests
    if normal_user_requests:
        semester_start, semester_end = get_semester_block(dt=now)
        visibility_from = run_scheduler(normal_user_requests, scheduler_client, now,
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
    else:
        log.warn("Received no User Requests! Skipping this scheduling cycle")
    sys.stdout.flush()


    return visibility_from


def was_dirty_and_cleared(scheduler_client, args):
    dirty_response = get_dirty_flag(scheduler_client)

    if dirty_response['dirty'] is True:
        if clear_dirty_flag(scheduler_client, args):
            return True

    return False


def scheduler_rerun_required(scheduler_client, args, network):
    db_is_dirty         = False
    network_has_changed = False

    if was_dirty_and_cleared(scheduler_client, args):
        log.info("Dirty flag was found set and cleared.")
        db_is_dirty = True

    if network.has_changed():
        log.info("Telescope network events were found.")
        network_has_changed = True

    return db_is_dirty or network_has_changed




def main(argv):
    global run_flag
    args = parse_args(argv)

    log.info("Starting Adaptive Scheduler, version {v}".format(v=VERSION))

    if args.dry_run:
        import lcogtpond
        lcogtpond._service_host = 'localhost'

    request_db_url   = args.requestdb
    scheduler_client = SchedulerClient(request_db_url)

    network = Network()

    event_bus = get_eventbus()
    user_feedback_logger = UserFeedbackLogger()
    timing_logger        = TimingLogger()
    event_bus.add_listener(user_feedback_logger, persist=True)
    event_bus.add_listener(timing_logger, persist=True,
                           event_type=TimingLogger._StartEvent)
    event_bus.add_listener(timing_logger, persist=True,
                           event_type=TimingLogger._EndEvent)

    # Force a reschedule when first started
    scheduler_client.set_dirty_flag()

    visibility_from = {}
    while run_flag:
        current_events = []
        if not args.noweather:
            current_events = network.update()

        if scheduler_rerun_required(scheduler_client, args, network):
            visibility_from = create_new_schedule(scheduler_client, args,
                                                  visibility_from, current_events)

        if args.run_once:
            run_flag = False

        log.info("Sleeping for %d seconds", args.sleep)
        time.sleep(args.sleep)



if __name__ == '__main__':
    main(sys.argv[1:])

