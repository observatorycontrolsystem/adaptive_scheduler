#!/usr/bin/env python
'''
as2.py - Run the adaptive scheduler in single use, non-persistent mode.

Author: Eric Saunders
July 2012
'''
from __future__ import division

from adaptive_scheduler.orchestrator import main, get_requests_from_db
from adaptive_scheduler.printing     import cowcud as pl
from reqdb.client import SchedulerClient

from optparse import OptionParser
from datetime import datetime
import logging
import signal
import time
import sys

VERSION = '1.0.0'

# Set up and configure an application scope logger
logging.config.fileConfig('logging.conf')
log = logging.getLogger('Scheduler')

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
    request_db_url = 'http://localhost:8001/'
#    request_db_url = 'http://zwalker-linux.lco.gtn:8001/'

    scheduler_client = SchedulerClient(request_db_url)

    scheduler_client.set_dirty_flag()


    while run_flag:
        dirty_response = scheduler_client.get_dirty_flag()

        if dirty_response['dirty'] is True:
            msg  = "Got dirty flag (DB needs reading) with timestamp"
            msg += " %s (last updated %s)" % (dirty_response['timestamp'],
                                              dirty_response['last_updated'])
            log.info(msg)

            # TODO: Log request receiving errors
            log.info("Clearing dirty flag")
            scheduler_client.clear_dirty_flag()
            requests = get_requests_from_db(scheduler_client.url, 'dummy arg')
            log.info("Got %d %s from Request DB", *pl(len(requests), 'User Request'))

            # TODO: What about if we don't get stuff successfully - need to set flag

            # Run the scheduling loop
            main(requests, scheduler_client)
            sys.stdout.flush()
        else:
            msg  = "Request DB is still clean - nothing has changed."
            msg += " Sleeping for %d seconds." % sleep_duration
            log.info(msg)
            time.sleep(sleep_duration)

