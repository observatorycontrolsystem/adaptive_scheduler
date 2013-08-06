'''
network_status.py - Module for retrieving the network status.

description

Author: Martin Norbury
May 2013
'''
from adaptive_scheduler.monitoring.monitors import (ScheduleTimestampMonitor, NotOkToOpenMonitor,
                                                    OfflineResourceMonitor)

DEFAULT_MONITORS = [ScheduleTimestampMonitor(),
                    NotOkToOpenMonitor(),
                    OfflineResourceMonitor()]

def network_status(monitors=None):
    ''' Retrieve the network status

        monitors (optional) - The list of monitors to derive a network status
                              from.
    '''

    # Use default monitors if not specified
    monitors = monitors or DEFAULT_MONITORS

    events = {}
    for monitor in monitors:
        new_events = monitor.monitor()
        for resource, event in new_events.iteritems():
            events.setdefault(resource, []).append(event)

    return events
