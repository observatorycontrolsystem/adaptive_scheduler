#!/usr/bin/env python
'''
as2.py - Run the adaptive scheduler in single use, non-persistent mode.

Author: Eric Saunders
July 2012
'''
from __future__ import division


from reqdb.client import SchedulerClient
from adaptive_scheduler.orchestrator import ( main, get_requests_from_file,
                                              get_requests_from_db )

from datetime import datetime
import signal
import time
import sys

run_flag = True

# Define signal handlers
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
    sleep_duration = 2

    # Acquire and collapse the requests
    request_db_url = 'http://localhost:8001/'
    scheduler_client = SchedulerClient(request_db_url)

    scheduler_client.set_dirty_flag()


    while run_flag:
        dirty_response = scheduler_client.get_dirty_flag()

        if dirty_response['dirty'] is True:
            msg  = "Got dirty flag (DB needs reading) with timestamp"
            msg += " %s (last updated %s)" % (dirty_response['timestamp'],
                                              dirty_response['last_updated'])
            print msg

            # TODO: Log request receiving errors
            requests = get_requests_from_db(scheduler_client.url, 'dummy arg')

            print "Received %d User Requests from Request DB" % len(requests)
            print "Clearing dirty flag"
            scheduler_client.clear_dirty_flag()

            # Run the scheduling loop
            main(requests, scheduler_client)
            sys.stdout.flush()
        else:
            msg  = "Request DB is still clean - nothing has changed."
            msg += " Sleeping for %d seconds." % sleep_duration
            time.sleep(sleep_duration)

            print msg
