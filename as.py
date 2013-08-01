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


from adaptive_scheduler.orchestrator     import main, get_requests_from_db
from adaptive_scheduler.printing         import pluralise as pl
from adaptive_scheduler.utils            import timeit, iso_string_to_datetime
from adaptive_scheduler.semester_service import get_semester_block
from reqdb.client import SchedulerClient, ConnectionError

import argparse
from datetime import datetime, timedelta
import logging
import logging.config
import signal
import time
import sys

VERSION = '1.2.1'
DRY_RUN = False

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


@timeit
def get_dirty_flag():
    dirty_response = dict(dirty=False)
    try:
        dirty_response = scheduler_client.get_dirty_flag()
    except ConnectionError as e:
        log.warn("Error retrieving dirty flag from DB: %s", e)
        log.warn("Skipping this scheduling cycle")

    return dirty_response


if __name__ == '__main__':
#    arg_parser = argparse.ArgumentParser(description="Run the Adaptive Scheduler")
    arg_parser = argparse.ArgumentParser(
                                formatter_class=argparse.RawDescriptionHelpFormatter,
                                description=__doc__)
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

    # Handle command line arguments
    args = arg_parser.parse_args()

    log.info("Starting Adaptive Scheduler, version {v}".format(v=VERSION))

    sleep_duration = args.sleep
    log.info("Sleep period between scheduling runs set at %ds" % sleep_duration)
    DRY_RUN = args.dry_run

    if DRY_RUN:
        log.info("Running in simulation mode - no DB changes will be made")

    log.info("Using available telescopes file '%s'", args.telescopes)


    request_db_url = args.requestdb
    scheduler_client = SchedulerClient(request_db_url)

    # Force a reschedule when first started
    scheduler_client.set_dirty_flag()


    visibility_from = {}
    while run_flag:
        dirty_response = get_dirty_flag()

        # Use a static command line datetime if provided...
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

        semester_start, semester_end = get_semester_block(dt=now)

        #TODO: HACK to handle not a real error returned from Request DB
        try:
            if dirty_response['dirty'] is True:
                log.critical("hi. Please fix me.")
        except TypeError as e:
            log.critical("Request DB could not update internal state. Aborting current scheduling loop.")
            log.info(" Sleeping for %d seconds", sleep_duration)
            time.sleep(sleep_duration)
            continue

        if dirty_response['dirty'] is True:
            msg  = "Got dirty flag (DB needs reading) with timestamp"
            msg += " %s (last updated %s)" % (dirty_response['timestamp'],
                                              dirty_response['last_updated'])
            log.info(msg)

            log.info("Clearing dirty flag")
            try:
                scheduler_client.clear_dirty_flag()
            except ConnectionError as e:
                log.critical("Error clearing dirty flag on DB: %s", e)
                log.critical("Aborting current scheduling loop.")
                log.info(" Sleeping for %d seconds", sleep_duration)
                time.sleep(sleep_duration)
                continue

            try:
                requests = get_requests_from_db(scheduler_client.url, 'dummy arg',
                                                semester_start, semester_end)
                log.info("Got %d %s from Request DB", *pl(len(requests), 'User Request'))

                # TODO: What about if we don't get stuff successfully - need to set flag

                # Run the scheduling loop, if there are any User Requests
                if len(requests):
                    visibility_from = main(requests, scheduler_client, now,
                                           semester_start, semester_end,
                                           args.telescopes, visibility_from, dry_run=DRY_RUN)
                else:
                    log.warn("Received no User Requests! Skipping this scheduling cycle")
                sys.stdout.flush()
            except ConnectionError as e:
                log.warn("Error retrieving Requests from DB: %s", e)
                log.warn("Skipping this scheduling cycle")
        else:
            log.info("Request DB is still clean - nothing has changed")

        log.info("Sleeping for %d seconds", sleep_duration)
        time.sleep(sleep_duration)

