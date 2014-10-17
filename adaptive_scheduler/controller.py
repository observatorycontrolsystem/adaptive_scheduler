#!/usr/bin/env python
'''
controller.py - Main controller and application entry point

This package contains the main entry point for the scheduler.

Author: Martin Norbury
        Eric Saunders
January 2013
'''


# Required for true (non-integer) division
from __future__ import division

# Standard library imports
from Queue import Queue
import threading
import signal
import sys

# Internal imports
from adaptive_scheduler.log          import create_logger
from adaptive_scheduler.monitor      import create_monitor
from adaptive_scheduler.monitor      import create_database_syncronizer
from adaptive_scheduler.monitor      import DBSyncronizeEvent, RequestUpdateEvent
from adaptive_scheduler.orchestrator import main as schedule
from reqdb.client                    import SchedulerClient


# Define signal handlers
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


def run_controller(request_polling, syncronization_interval, request_db_url):
    '''Execute the main scheduler thread.'''

    logger.info("Starting controller")

    # Create the Queue through which all the threads communicate
    queue = Queue()

    # Periodically monitor the Request DB for new Requests
    sc = SchedulerClient(request_db_url)
    monitor = create_monitor(request_polling, queue, sc)
    monitor.start()

    # Periodically update the Request DB with POND block status
#    syncronizer = create_database_syncronizer(syncronization_interval, queue)
#    syncronizer.start()

    # Process events from the other threads as they arrive
    event_handler = EventHandlerThread(queue, sc)
    event_handler.start()

    # Put the main thread to sleep unless interrupted by a signal
    while True:
        signal.pause()



class EventHandlerThread(threading.Thread):
    '''Thread to process events placed on the Queue from other threads.'''

    def __init__(self, queue, sched_client, name="Event Handler Thread"):
        super(EventHandlerThread, self).__init__(name=name)
        self.queue         = queue
        self.event_handler = EventHandler(sched_client)

        # Thread configuration
        self.event  = threading.Event()
        self.daemon = True


    def run(self):
        while True: self.event_handler.handle(self.queue.get(block=True))

    def stop(self):
        self.event.set()



class EventHandler(object):
    '''Resolves and executes the handler method for each type of Event.'''

    def __init__(self, sched_client):
        self.handler_map = {
                             DBSyncronizeEvent  : self._handle_syncronize_db,
                             RequestUpdateEvent : self._handle_request_update,
                           }
        self.sched_client = sched_client


    def handle(self, event):
        '''Call the relevant handler method based on the type of Event.'''
        logger.info("Got event %r" % (event,))

        handler = self.handler_map.get(event.__class__, self._handle_unknown_event)
        handler(event)


    def _handle_syncronize_db(self, event):
        # Call the syncdb view here
        logger.info("Syncronizing databases")

    def _handle_request_update(self, event):
        schedule(event.requests, self.sched_client)
        sys.stdout.flush()

    def _handle_unknown_event(self, event):
        logger.warning("Received an unknown event of type %s", event.__class__)



if __name__ == '__main__':
    run_controller()
