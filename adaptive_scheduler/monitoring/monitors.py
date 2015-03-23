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
import sys

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

        # Sort by site
        site_sorter= lambda x: x.site
        ok_to_open.sort(key=site_sorter)
        countdown.sort(key=site_sorter)
        interlock.sort(key=site_sorter)

        ok_sites = ok_to_open.__len__()
        countdown_sites = countdown.__len__()
        interlock_sites = interlock.__len__()

        # Check datum lists have same size
        if (ok_sites != countdown_sites) | (ok_sites != interlock_sites):
             log.error("Database query returns different number of sites")
             return []

        # Check that the sites agree for each list
        for index in range(0,ok_sites-1):
             ok_site = ok_to_open[index].site
             countdown_site = countdown[index].site
             interlock_site = interlock[index].site

             if  (ok_site != countdown_site) | (ok_site != interlock_site):
                 log.error("Database query returns inconsistent site names")
                 return []

        WeatherInterlock = collections.namedtuple('WeatherInterlock',
                                            ['ok_to_open', 'countdown', 'interlock'])

        return [ WeatherInterlock(*datum) for datum in zip(ok_to_open, countdown, interlock) ]


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
        return resources.get_site_resources(datum.ok_to_open.site)


    def is_an_event(self, datum):
        ok_to_open, countdown, interlock = datum
        unable_to_open = ok_to_open.value.lower() == 'false'
        night          = interlock.value.lower() != 'Sun Up'.lower()
        return unable_to_open and night



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

