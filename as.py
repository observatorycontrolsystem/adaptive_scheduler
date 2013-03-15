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

signal.signal(signal.SIGINT, ctrl_c_handler)
signal.signal(signal.SIGTERM, kill_handler)



if __name__ == '__main__':


    log.info("Starting Adaptive Scheduler, version {v}".format(v=VERSION))
    sleep_duration = 20

    # Acquire and collapse the requests
#    request_db_url = 'http://pluto.lco.gtn:8001/'
    request_db_url = 'http://localhost:8001/'
#    request_db_url = 'http://zwalker-linux.lco.gtn:8001/'

    scheduler_client = SchedulerClient(request_db_url)

    scheduler_client.set_dirty_flag()


    while run_flag:
        dirty_response = dict(dirty=False)
        try:
            dirty_response = scheduler_client.get_dirty_flag()
        except ConnectionError as e:
            log.warn("Error retrieving dirty flag from DB: %s", e)
            log.warn("Skipping this scheduling cycle")

        if dirty_response['dirty'] is True:
            msg  = "Got dirty flag (DB needs reading) with timestamp"
            msg += " %s (last updated %s)" % (dirty_response['timestamp'],
                                              dirty_response['last_updated'])
            log.info(msg)

            # TODO: Log request receiving errors
            log.info("Clearing dirty flag")
            scheduler_client.clear_dirty_flag()
            try:
                requests = get_requests_from_db(scheduler_client.url, 'dummy arg')
                log.info("Got %d %s from Request DB", *pl(len(requests), 'User Request'))

                # TODO: What about if we don't get stuff successfully - need to set flag

                # Run the scheduling loop
                main(requests, scheduler_client)
                sys.stdout.flush()
            except ConnectionError as e:
                log.warn("Error retrieving Requests from DB: %s", e)
                log.warn("Skipping this scheduling cycle")
        else:
            log.info("Request DB is still clean (or unreachable) - nothing has changed.")
            log.info(" Sleeping for %d seconds.", sleep_duration)
            time.sleep(sleep_duration)

