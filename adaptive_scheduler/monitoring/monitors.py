'''
monitors.py - Module containing network monitors.

description

Author: Martin Norbury
May 2013
'''
import abc
import ast
from datetime   import datetime, timedelta
from contextlib import closing
import collections
import itertools

from adaptive_scheduler.monitoring           import resources
from lcogt                                   import dateutil
from adaptive_scheduler.monitoring.telemetry import get_datum

Event = collections.namedtuple('Event', ['type', 'reason', 'start_time',
                                         'end_time'])

# Set up and configure a module scope logger
import logging
log = logging.getLogger(__name__)

class NetworkStateMonitor(object):
    '''Abstract class for monitoring changes to the telescope network.'''

    __metaclass__ = abc.ABCMeta

    def __init__(self):
        pass

    def monitor(self):
        ''' Monitor for changes. '''
        data = self.retrieve_data()
        events = []
        for datum in data:
            if self.is_an_event(datum):
                event         = self.create_event(datum)
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
        return '.'.join([ datum[col] for col in columns ])

    @abc.abstractmethod
    def is_an_event(self, datum):
        ''' Return true if this datum means a resource unuseable. '''
        pass

class SequencerEnableMonitor(NetworkStateMonitor):
    ''' Monitor the sequencer enable state. '''

    def __init__(self):
        super(SequencerEnableMonitor, self).__init__()
        self.reason = None


    def retrieve_data(self):
        return get_datum("Sequencer Enable State")


    def create_event(self, datum):
        reason = self.reason or "Sequencer in %s state" % datum.value
        event  = Event(
                        type       = "SEQUENCER DISABLED",
                        reason     = reason,
                        start_time = datum.timestamp_changed,
                        end_time   = datum.timestamp_measured
                      )

        return event


    def create_resource(self, datum):
        return '.'.join((datum.telescope,datum.observatory,datum.site))


    def is_an_event(self, datum):
        return datum.value != 'AUTOMATIC'



class ScheduleTimestampMonitor(NetworkStateMonitor):
    ''' Monitor the scheduler last update timestamp. '''

    def __init__(self):
        super(ScheduleTimestampMonitor, self).__init__()
        self.reason = None


    def retrieve_data(self):
        return get_datum("Site Agent Schedule Timestamp", persistence_model='STATUS')


    def create_event(self, datum):
        reason = self.reason or "No update since %s" % datum.value
        event = Event(
                      type       = "SITE AGENT UNRESPONSIVE",
                      reason     = reason,
                      start_time = datum.timestamp_changed,
                      end_time   = datum.timestamp_measured
                     )

        return event


    def create_resource(self, datum):
        site = datum.site
        telescope, observatory = datum.instance.split('.')

        resource = '.'.join((telescope, observatory, site))

        return resource


    def is_an_event(self, datum):
        try:
            timestamp        = dateutil.parse(datum.value)
            ten_minute_delta = timedelta(minutes=10)
            return datetime.utcnow() - timestamp > ten_minute_delta
        except dateutil.ParseException as e:
            self.reason = str(e)
            return True



class NotOkToOpenMonitor(NetworkStateMonitor):
    ''' Monitor the OK_TO_OPEN flag. '''

    def __init__(self):
        super(NotOkToOpenMonitor, self).__init__()

    def retrieve_data(self):
        ok_to_open = get_datum('Weather Ok To Open', 1, persistence_model='STATUS')
        countdown  = get_datum('Weather Count Down To Open', 1, persistence_model='TEN_SEC')
        interlock  = get_datum('Weather Failure Reason', 1, persistence_model='COUNT')
        overridden = get_datum('Enclosure Weather Override Active', 1, persistence_model='STATUS')

        # Sort by site
        site_sorter= lambda x: x.site
        ok_to_open.sort(key=site_sorter)
        countdown.sort(key=site_sorter)
        interlock.sort(key=site_sorter)

        self._overriden_observatories = ['%s.%s' % (x.observatory, x.site)
                                         for x in overridden if x.value in ('true', 'True')]

        # Check datum lists have same size
        if not (len(ok_to_open) == len(countdown) == len(interlock)):
             log.error('Telemetry query returns different number of sites')
             return []

        # Check that the sites agree for each list
        site_sorted_data_list = zip(ok_to_open, countdown, interlock)
        for ok_entry, countdown_entry, interlock_entry in site_sorted_data_list:
            if not(ok_entry.site == countdown_entry.site == interlock_entry.site):
                 log.error('Telemetry query returns inconsistent site names')
                 return []

        WeatherInterlock = collections.namedtuple('WeatherInterlock',
                                            ['ok_to_open', 'countdown', 'interlock'])

        return [ WeatherInterlock(*datum) for datum in site_sorted_data_list ]


    def create_event(self, datum):
        ok_to_open, countdown, interlock = datum

        timestamp_measured = ok_to_open.timestamp_measured
        try:
            delta_time = timedelta(seconds=float(countdown.value))
        except ValueError as e:
            log.warn("Garbage data in incoming event: %s", e)
            delta_time = timedelta(seconds=0)

        end_time = timestamp_measured + delta_time
        event = Event(
                       type       = "NOT OK TO OPEN",
                       reason     = interlock.value,
                       start_time = ok_to_open.timestamp_changed,
                       end_time   = end_time,
                     )

        return event


    def create_resource(self, datum):
        '''
        Create resource list from datum.

        :param datum: Datum used to generate resource list.
        :return: Create a list of resources with the current event. Exclude any resource that is currently overridden.
        '''
        site_resources = resources.get_site_resources(datum.ok_to_open.site)
        return [resource for resource in site_resources if not self._is_resource_overridden(resource)]


    def is_an_event(self, datum):
        ok_to_open, countdown, interlock = datum
        unable_to_open = ok_to_open.value.lower() == 'false'
        night          = interlock.value.lower() != 'Sun Up'.lower()
        return unable_to_open and night

    def _is_resource_overridden(self, resource):
        '''
        :param resource: Telescope resource e.g. 1m0a.doma.lsc
        :return: True if telescope resource is in an observatory that has it weather overridden.
        '''
        return any([observatory for observatory in self._overriden_observatories if observatory in resource])



class OfflineResourceMonitor(NetworkStateMonitor):
    ''' Monitor resource ONLINE/OFFLINE state. '''

    def __init__(self, filename='telescopes.dat'):
        super(OfflineResourceMonitor, self).__init__()
        self._filename = filename


    def retrieve_data(self):
        try:
            resource_file = open(self._filename)
        except TypeError:
            resource_file = closing(self._filename)

        with resource_file as filep:
            raw_data = filep.read()
        return ast.literal_eval(raw_data)


    def create_event(self, datum):
        event = Event(
                        type       = "OFFLINE",
                        reason     = "Telescope not available",
                        start_time = None,
                        end_time   = None,
                     )

        return event


    def create_resource(self, datum):
        return datum['name']


    def is_an_event(self, datum):
        return datum['status'].lower() == 'offline'


class EnclosureInterlockMonitor(NetworkStateMonitor):

    @staticmethod
    def _sort_by_site_and_observatory(datum_tuple):
        datum_name, datum = datum_tuple
        return datum.site, datum.observatory

    @staticmethod
    def _datum_name_to_key(datum_name):
        return datum_name.lower().replace(' ', '_')

    def is_an_event(self, datum):
        result = True
        if datum['enclosure_interlocked'].value.lower() == 'true':
            if not datum['enclosure_interlocked_reason'].value:
                return result

        if 'enclosure_interlocked_reason' in datum:
            result = any([x in datum['enclosure_interlocked_reason'].value.lower() for x in ('enclosure_flapping', 'power')])
        return result

    def retrieve_data(self):
        datum_names = "Enclosure Interlocked", "Enclosure Interlocked Reason"

        sorted_by_observatory = sorted(self._flatten_data(datum_names), key=self._sort_by_site_and_observatory)

        return [dict(value) for key, value
                in itertools.groupby(sorted_by_observatory, key=self._sort_by_site_and_observatory)]

    def create_event(self, datum):

        start_time = datetime.utcnow()
        if 'enclosure_interlocked' in datum:
            start_time = datum['enclosure_interlocked'].timestamp_changed

        reason = 'No Reason Found'
        if 'enclosure_interlocked_reason' in datum and datum['enclosure_interlocked_reason'].value:
            reason = datum['enclosure_interlocked_reason'].value

        event = Event(type       = 'ENCLOSURE INTERLOCK',
                      reason     = reason,
                      start_time = start_time,
                      end_time   = None)

        return event

    def create_resource(self, datum):
        interlocked = datum['enclosure_interlocked']
        site        = interlocked.site
        observatory = interlocked.observatory

        return resources.get_observatory_resources(site, observatory)

    def _flatten_data(self, datum_names):
        for datum_name in datum_names:
            for datum in get_datum(datum_name, "1", persistence_model="Status"):
                yield self._datum_name_to_key(datum_name), datum

        return
