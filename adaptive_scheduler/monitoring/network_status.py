'''
network_status.py - Retrieve and identify changes in the observing capabilities
                    of the network.

This module provides Network, an object that tracks the presence of Events
that affect telescope observing readiness. Helper methods allow comparison between
the state of the network last time, and the current state, allowing a client to
take action based on events appearing or disappearing.

Example usage:
    network = Network()
    events = network.update()    # Refresh and return the current network state

    ... (later)
    events = network.update()
    network.has_changed()  # Flag indicating network change since previous update

Author: Martin Norbury
        Eric Saunders
May 2013
'''
from adaptive_scheduler.monitoring.monitors import (ScheduleTimestampMonitor,
                                                    NotOkToOpenMonitor,
                                                    OfflineResourceMonitor,
                                                    SequencerEnableMonitor,
                                                    EnclosureInterlockMonitor,
                                                    AvailableForScheduling)
from adaptive_scheduler.monitoring.telemetry import ConnectionError

import datetime as dt
import socket
import requests
from retry import retry
import collections

DEFAULT_MONITORS = [OfflineResourceMonitor, AvailableForScheduling]

import logging

log = logging.getLogger(__name__)


def _log_event_differences(current_events, previous_events):
    cur_set = set(flatten(current_events))
    prev_set = set(flatten(previous_events))

    new_events = cur_set - prev_set
    cleared_events = prev_set - cur_set

    if new_events:
        log.info("New events found since last check: %s", new_events)
    if cleared_events:
        log.info("Events that cleared since last check: %s", cleared_events)


def flatten(events_dict):
    '''Convert the resource-keyed events dict into a list of tuples, for easy
       comparison and differencing. IMPORTANT NOTE: We throw away all time
       information here, so differencing only works on changing type, or the
       presence or absence of events.
    '''
    flattened_list = []

    for resource, events in events_dict.iteritems():
        for event in events:
            flattened_list.append((resource, event.type))

    return flattened_list


class Network(object):
    ''' Represent the state of the telescope network (e.g. weather,
        technical issues), and determine when that state changes.
    '''

    def __init__(self, configdb_interface, monitors=None):
        '''
            monitors (optional) - The list of specific monitors to check for
                                  Events.
        '''
        # Use default monitors if not specified
        if monitors:
            self.monitors = monitors
        else:
            self.monitors = []
            for monitor in DEFAULT_MONITORS:
                self.monitors.append(monitor(configdb_interface))
        self.current_events = {}
        self.previous_events = {}

    def update(self):
        ''' Try and get the current network state. If we can't, return the
            last known state (since we don't know if anything changed).
            Previous state is stored for later comparison.
        '''
        self.previous_events = self.current_events

        try:
            self.current_events = self._status()
        except ConnectionError as e:
            msg = "Unable to retrieve current network status: %s" % e
            log.warn(msg)

            return self.previous_events

        return self.current_events

    def has_changed(self):
        ''' Return True if the current network state is different from
            the previous network state.
        '''
        if flatten(self.current_events) == flatten(self.previous_events):
            return False

        _log_event_differences(self.current_events, self.previous_events)
        return True

    def _status(self):
        ''' Retrieve the network status by querying and collating each monitor.
            Return a list of monitoring.monitors.Events.
        '''
        events = {}
        for monitor in self.monitors:
            new_events = monitor.monitor()

            for resource, event in new_events.iteritems():
                events.setdefault(resource, []).append(event)

        return events
