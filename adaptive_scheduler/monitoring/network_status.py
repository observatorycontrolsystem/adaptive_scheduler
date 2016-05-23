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
import collections

DEFAULT_MONITORS = [ScheduleTimestampMonitor(),
                    NotOkToOpenMonitor(),
                    OfflineResourceMonitor(),
                    SequencerEnableMonitor(),
                    EnclosureInterlockMonitor(),
                    AvailableForScheduling()]

DATE_FORMATTER = '%Y-%m-%d %H:%M:%S'

import logging
log = logging.getLogger(__name__)

def _log_event_differences(current_events, previous_events):
    cur_set  = set(flatten(current_events))
    prev_set = set(flatten(previous_events))

    new_events     = cur_set - prev_set
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

    def __init__(self, monitors=None, es_endpoint=None):
        '''
            monitors (optional) - The list of specific monitors to check for
                                  Events.
        '''
        # Use default monitors if not specified
        self.monitors = monitors or DEFAULT_MONITORS
        self.current_events  = {}
        self.previous_events = {}
        self.es_endpoint = es_endpoint


    def update(self):
        ''' Try and get the current network state. If we can't, return the
            last known state (since we don't know if anything changed).
            Previous state is stored for later comparison.
        '''
        self.previous_events = self.current_events

        try:
            self.current_events  = self._status()
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
                # send the event to ES for indexing and storing
                event_dict = self._construct_event_dict(resource, event)
                self.send_event_to_es(event_dict)

        return events


    def _construct_event_dict(self, telescope_name, event):
        split_name = telescope_name.split('.')
        event_dict = {'type': event.type.replace(' ', '_'),
                      'reason': event.reason,
                      'name': telescope_name,
                      'telescope': split_name[0],
                      'enclosure': split_name[1],
                      'site': split_name[2],
                      'timestamp': dt.datetime.utcnow().strftime(DATE_FORMATTER),
                      'hostname': socket.gethostname()}
        if event.start_time:
            event_dict['start_time'] = event.start_time.strftime(DATE_FORMATTER)

        if event.end_time:
            event_dict['end_time'] = event.end_time.strftime(DATE_FORMATTER)

        return event_dict


    def _construct_available_event_dict(self, telescope_name):
        event = collections.namedtuple('Event',['type', 'reason', 'start_time', 'end_time'])(type= 'AVAILABLE',
                      reason= 'Available for scheduling',
                      start_time= dt.datetime.utcnow(),
                      end_time= dt.datetime.utcnow())

        return self._construct_event_dict(telescope_name, event)


    def send_telescope_available_state_events(self, telescope_name_list):
        for telescope_name in telescope_name_list:
            event_dict = self._construct_available_event_dict(telescope_name)
            self.send_event_to_es(event_dict)


    def send_event_to_es(self, event_dict):
        if self.es_endpoint:
            try:
                sanitized_timestamp = event_dict['timestamp'].replace(' ', '_').replace(':', '_')
                requests.post(self.es_endpoint + event_dict['name'] + '_' + event_dict['type'] + '_' + sanitized_timestamp,
                          json=event_dict).raise_for_status()
            except Exception as e:
                log.error('Exception storing telescope status event in elasticsearch: {}'.format(repr(e)))

