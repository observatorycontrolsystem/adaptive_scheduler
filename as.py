#!/usr/bin/env python
'''
as.py - Run the adaptive scheduler in single use, non-persistent mode.

Author: Eric Saunders
July 2012
'''
from __future__ import division


from adaptive_scheduler.orchestrator import main, get_requests_from_db
from adaptive_scheduler.printing     import pluralise as pl
from reqdb.client import SchedulerClient, ConnectionError

from optparse import OptionParser
from datetime import datetime
import logging
import logging.config
import signal
import time
import sys

VERSION = '1.0.0'

# Set up and configure an application scope logger
logging.config.fileConfig('logging.conf')
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



if __name__ == '__main__':


    log.info("Starting Adaptive Scheduler, version {v}".format(v=VERSION))
    sleep_duration = 60

    # Acquire and collapse the requests
#    request_db_url = 'http://pluto.lco.gtn:8001/'
#    request_db_url = 'http://localhost:8001/'
#    request_db_url = 'http://zwalker-linux.lco.gtn:8001/'
    request_db_url = 'http://scheduler-dev.lco.gtn/requestdb/'

    scheduler_client = SchedulerClient(request_db_url)

    scheduler_client.set_dirty_flag()


    while run_flag:
        dirty_response = dict(dirty=False)
        try:
            dirty_response = scheduler_client.get_dirty_flag()
        except ConnectionError as e:
            log.warn("Error retrieving dirty flag from DB: %s", e)
            log.warn("Skipping this scheduling cycle")

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

#            raw_input("DEBUG: Press enter to continue")

            # TODO: Log request receiving errors
            log.info("Clearing dirty flag")
            scheduler_client.clear_dirty_flag()

            try:
                requests = get_requests_from_db(scheduler_client.url, 'dummy arg')
                log.info("Got %d %s from Request DB", *pl(len(requests), 'User Request'))

                # TODO: What about if we don't get stuff successfully - need to set flag

                # Run the scheduling loop, if there are any User Requests
                if len(requests):
                    main(requests, scheduler_client)
                else:
                    log.warn("Recieved no User Requests! Skipping this scheduling cycle")
                sys.stdout.flush()
            except ConnectionError as e:
                log.warn("Error retrieving Requests from DB: %s", e)
                log.warn("Skipping this scheduling cycle")
        else:
            log.info("Request DB is still clean - nothing has changed")
            log.info("Sleeping for %d seconds", sleep_duration)
            time.sleep(sleep_duration)

