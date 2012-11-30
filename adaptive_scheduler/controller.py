#!/usr/bin/env python
''' scheduler/controller.py - Main controller and application entry point.

This package contains the main entry point for the scheduler.

'''

# Required for true (non-integer) division
from __future__ import division

# Standard library imports
from Queue import Queue

# Internal imports
from adaptive_scheduler.log import create_logger
from adaptive_scheduler.monitor import create_monitor
from adaptive_scheduler.monitor import create_database_syncronizer
from adaptive_scheduler.monitor import DBSyncronizeEvent, RequestUpdateEvent
from adaptive_scheduler.orchestrator import main as schedule

# Module scope logger
logger = create_logger("controller")


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
    while True: handle_event(queue.get(block=True))

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
