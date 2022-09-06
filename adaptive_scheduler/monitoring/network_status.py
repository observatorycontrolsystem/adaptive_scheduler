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
from adaptive_scheduler.monitoring.monitors import OfflineResourceMonitor, AvailableForScheduling
from adaptive_scheduler.monitoring.opensearch_telemetry import OSConnectionError

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

    for resource, events in events_dict.items():
        for event in events:
            flattened_list.append((resource, event.reason))

    return flattened_list


class Network(object):
    ''' Represent the state of the telescope network (e.g. weather,
        technical issues), and determine when that state changes.
    '''

    def __init__(self, configdb_interface, scheduler_params=None, monitors=None):
        '''
            monitors (optional) - The list of specific monitors to check for
                                  Events.
        '''
        # Use default monitors if not specified
        if monitors:
            self.monitors = monitors
        else:
            self.monitors = [
                OfflineResourceMonitor(configdb_interface),
            ]
            if scheduler_params.opensearch_url and scheduler_params.opensearch_index:
                self.monitors.append(AvailableForScheduling(
                    configdb_interface,
                    scheduler_params.opensearch_url,
                    scheduler_params.opensearch_index,
                    scheduler_params.opensearch_excluded_observatories
                ))

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
        except OSConnectionError as e:
            msg = "Unable to retrieve current network status: %s" % e
            log.warn(msg)

            return self.previous_events

        return self.current_events

    def has_changed(self):
        ''' Return True if the current network state is different from
            the previous network state.
        '''
        _log_event_differences(self.current_events, self.previous_events)
        if self.current_events.keys() == self.previous_events.keys():
            return False

        return True

    def _status(self):
        ''' Retrieve the network status by querying and collating each monitor.
            Return a list of monitoring.monitors.Events.
        '''
        events = {}
        for monitor in self.monitors:
            new_events = monitor.monitor()

            for resource, event in new_events.items():
                events.setdefault(resource, []).append(event)

        return events
