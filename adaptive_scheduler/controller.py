#!/usr/bin/env python
''' scheduler/controller.py - Main controller and application entry point.

This package contains the main entry point for the scheduler.

'''

# Required for true (non-integer) division
from __future__ import division

# Standard library imports
from Queue import Queue

# Internal imports
from adaptive_scheduler.log          import create_logger
from adaptive_scheduler.monitor      import create_monitor
from adaptive_scheduler.monitor      import create_database_syncronizer
from adaptive_scheduler.monitor      import DBSyncronizeEvent, RequestUpdateEvent
from adaptive_scheduler.orchestrator import main as schedule

import threading

import signal
import sys

def ctrl_c_handler(signal, frame):
        print 'Received Ctrl+C - terminating.'
        sys.exit(0)

def kill_handler(signal, frame):
        print 'Received SIGTERM (kill) - terminating.'
        sys.exit(0)

signal.signal(signal.SIGINT, ctrl_c_handler)
signal.signal(signal.SIGTERM, kill_handler)

# Module scope logger
logger = create_logger("controller")

class MyFred(threading.Thread):
    def __init__(self, queue, name="Event Handler Thread"):
        super(MyFred, self).__init__(name=name)
        self.queue = queue
        self.event = threading.Event()
        self.daemon = True

    def run(self):
        while True: handle_event(self.queue.get(block=True))

    def stop(self):
        self.event.set()



def run_controller(request_polling, syncronization_interval):
    ''' Run the scheduler controller. '''
    logger.info("Starting controller")


    # Create queue
    queue = Queue()

    # Start monitor
    monitor = create_monitor(request_polling, queue)
    monitor.start()

    # Start db syncronizer
    syncronizer = create_database_syncronizer(syncronization_interval, queue)
    syncronizer.start()

    # Process events TODO: maybe we want to shutdown/restart
    event_handler = MyFred(queue)
    event_handler.start()

    while True:
        signal.pause()


def handle_event(event):
    ''' Handle event. '''
    logger.info("Got event %r" % (event,))

    handler = handler_map.get(event.__class__, handle_unknown_event)
    handler(event)

def handle_syncronize_db(event):
    # Call the syncdb view here
    logger.info("Syncronizing databases")

def handle_request_update(event):
    schedule(event.requests)

def handle_unknown_event(event):
    logger.warning("Received an unknown event of type %s", event.__class__)

# Handler map
handler_map = {
                DBSyncronizeEvent  : handle_syncronize_db,
                RequestUpdateEvent : handle_request_update,
              }

if __name__ == '__main__':
    run_controller()
