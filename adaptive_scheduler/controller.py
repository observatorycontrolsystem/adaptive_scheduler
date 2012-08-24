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
from adaptive_scheduler.orchestrator import main as schedule

# Module scope logger
logger = create_logger("controller")

def run_controller(interval):
    ''' Run the scheduler controller. '''
    logger.info("Starting controller")

    # Create queue
    queue = Queue()

    # Start monitor
    monitor = create_monitor(interval, queue)
    monitor.start()

    # Process events TODO: maybe we want to shutdown/restart
    while True: handle_event(queue.get(block=True))

def handle_event(event):
    ''' Handle event. '''
    logger.info("Got event %r" % (event,))

    # Handle events here
    schedule(event.requests)

if __name__ == '__main__':
    run_controller()
