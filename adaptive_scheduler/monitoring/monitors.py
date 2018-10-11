'''
monitors.py - Module containing network monitors.

description

Author: Martin Norbury
May 2013
'''
import abc
from datetime import datetime, timedelta
import collections
import itertools

from lcogt import dateutil
from adaptive_scheduler.monitoring.telemetry import get_datum

# Set up and configure a module scope logger
import logging

log = logging.getLogger(__name__)

Event = collections.namedtuple('Event', ['type', 'reason', 'start_time',
                                         'end_time'])


class NetworkStateMonitor(object):
    '''Abstract class for monitoring changes to the telescope network.'''

    __metaclass__ = abc.ABCMeta

    def __init__(self, configdb_interface):
        self.configdb_interface = configdb_interface

    def monitor(self):
        ''' Monitor for changes. '''
        data = self.retrieve_data()
        events = []
        for datum in data:
            if self.is_an_event(datum):
                event = self.create_event(datum)
                resource_list = self.create_resource(datum)

                if isinstance(resource_list, basestring):
                    resource_list = (resource_list,)
                for resource in resource_list:
                    events.append((resource, event))

        # This assumes we only have one event per resource (the last one wins)
        return dict(events)

    @abc.abstractmethod
    def retrieve_data(self):
        ''' Retrieve the data from data source. '''
        pass

    @abc.abstractmethod
    def create_event(self, datum):
        ''' Convert raw data source into standard event. '''
        pass

    def create_resource(self, datum):
        ''' Create resource from data source. '''
        columns = ['telescope', 'observatory', 'site']
        return '.'.join([datum[col] for col in columns])

    @abc.abstractmethod
    def is_an_event(self, datum):
        ''' Return true if this datum means a resource unuseable. '''
        pass


class OfflineResourceMonitor(NetworkStateMonitor):
    ''' Monitor resource ONLINE/OFFLINE state. '''

    def __init__(self, configdb_interface):
        super(OfflineResourceMonitor, self).__init__(configdb_interface=configdb_interface)

    def retrieve_data(self):
        return self.configdb_interface.get_telescope_info().values()

    def create_event(self, datum):
        event = Event(
            type="OFFLINE",
            reason="Telescope not available",
            start_time=None,
            end_time=None,
        )

        return event

    def create_resource(self, datum):
        return datum['name']

    def is_an_event(self, datum):
        return datum['status'].lower() == 'offline'


class FlattenDataMonitor(NetworkStateMonitor):
    @staticmethod
    def _sort_by_site_and_observatory(datum_tuple):
        datum_name, datum = datum_tuple
        return datum.site, datum.observatory

    @staticmethod
    def _sort_by_site_and_observatory_and_telescope(datum_tuple):
        datum_name, datum = datum_tuple
        return datum.site, datum.observatory, datum.telescope

    @staticmethod
    def _datum_name_to_key(datum_name):
        return datum_name.lower().replace(' ', '_')

    def _flatten_data(self, datum_names):
        for datum_name in datum_names:
            for datum in get_datum(datum_name, instance=1):
                yield self._datum_name_to_key(datum_name), datum

        return


class AvailableForScheduling(FlattenDataMonitor):
    def retrieve_data(self):
        datum_names = "Available For Scheduling", "Available For Scheduling Reason"

        sorted_by_observatory = sorted(self._flatten_data(datum_names),
                                       key=self._sort_by_site_and_observatory_and_telescope)

        return [dict(value) for key, value
                in itertools.groupby(sorted_by_observatory, key=self._sort_by_site_and_observatory_and_telescope)]

    def create_event(self, datum):
        start_time = datetime.utcnow()
        end_time = None
        if 'available_for_scheduling' in datum:
            start_time = datum['available_for_scheduling'].timestamp_changed
            end_time = datum['available_for_scheduling'].timestamp_measured

        reason = 'No Reason Found'
        if 'available_for_scheduling_reason' in datum:
            reason = datum['available_for_scheduling_reason'].value
            if (datetime.utcnow() - datum['available_for_scheduling_reason'].timestamp_recorded) > timedelta(minutes=15):
                reason += ". Telemetry: Out of date"

        event = Event(
            type="NOT AVAILABLE",
            reason=reason,
            start_time=start_time,
            end_time=end_time
        )

        return event

    def create_resource(self, datum):
        dat = datum['available_for_scheduling']
        return '.'.join((dat.telescope, dat.observatory, dat.site))

    def is_an_event(self, datum):
        if 'available_for_scheduling' not in datum:
            return False
        dat = datum['available_for_scheduling']
        return 'false'.lower() == dat.value
