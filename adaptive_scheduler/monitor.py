#!/usr/bin/env python

'''
monitor.py - Threads to update the Request DB state and poll for new Requests

This class provides

    * DBSyncronizeThread - update the state of Requests in the Request DB based
                           on the state of blocks in the pond
    * MonitoringThread   - poll the Request DB for a change triggering a schedule
                           recompute

Author: Martin Norbury
        Eric Saunders
January 2013
'''

import log
import threading

from adaptive_scheduler.requestdb import get_requests_from_db as get_requests

# Create module log 
logger = log.create_logger("monitor")


# Public factory methods
def create_monitor(period, queue, scheduler_client):
    '''Factory for creating a Request DB monitoring thread.'''
    return _MonitoringThread(period, queue, scheduler_client)

def create_database_syncronizer(period, queue):
    '''Factory for creating a Request status synchronisation thread.'''
    return _DBSyncronizeThread(period, queue)


class DBSyncronizeEvent(object):
    def __repr__(self):
        return '%s' % (self.__class__,)

class RequestUpdateEvent(object):
    def __init__(self, requests):
        self.requests = requests
    def __repr__(self):
        return '%s(%r)' % (self.__class__, self.__dict__)


class _PollingThread(threading.Thread):
    '''Superclass for periodic polling threads.'''
    def __init__(self, period, name="Timer Thread"):
        super(_PollingThread, self).__init__(name=name)
        self.period = period
        self.event = threading.Event()
        self.daemon = True

    def run(self):
        logger.info("Starting polling")
        while not self.event.is_set():
            self.action()
            self.event.wait(self.period)
        logger.info("Stopping polling")

    def stop(self):
        self.event.set()

    def action(self):
        raise NotImplementedError("Override this in sub-class")


class _DBSyncronizeThread(_PollingThread):
    '''Update the state of Requests in the Request DB based on the state of
       blocks in the pond.'''

    def __init__(self, period, queue, name="DB Syncronization Thread"):
        super(_DBSyncronizeThread, self).__init__(period)
        self.queue = queue

    def action(self):
        logger.info("Syncronizing databases")
        self.queue.put(DBSyncronizeEvent())


class _MonitoringThread(_PollingThread):
    '''Poll the Request DB for a change triggering a schedule recompute.'''

    def __init__(self, period, queue, scheduler_client, name="Monitoring Thread"):
        super(_MonitoringThread, self).__init__(period)
        self.queue = queue
        self.scheduler_client = scheduler_client

    def action(self):
        logger.info("Getting latest requests")

        # Do periodic stuff here
        dirty_response = self.scheduler_client.get_dirty_flag()
        #if 'TRUE' in dirty_response['dirty'].upper():
        if dirty_response['dirty'] is True:
            msg  = "Got dirty flag (DB needs reading) with timestamp"
            msg += " %s (last updated %s)" % (dirty_response['timestamp'],
                                              dirty_response['last_updated'])
            print msg
            requests = get_requests(self.scheduler_client.url, 'dummy arg')
            # TODO: Log errors here
            self.scheduler_client.clear_dirty_flag()
            print "Requests received, clearing dirty flag"
            logger.info("Received %d User Requests from Request DB" % len(requests))

            # Post results to controller
            self.queue.put(RequestUpdateEvent(requests))
        else:
            msg = "Request DB is still clean - nothing has changed. Going back to sleep..."
            print msg

