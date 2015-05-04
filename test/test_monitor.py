'''
test_monitors.py - Test cases for the monitors module.

description

Author: Martin Norbury
May 2013
'''

from nose.tools import eq_, assert_false, assert_true, assert_equals
from datetime import datetime, timedelta
import mock
import unittest
from StringIO import StringIO

from adaptive_scheduler.monitoring.telemetry import Datum
from adaptive_scheduler.monitoring.monitors import (ScheduleTimestampMonitor,
                                                    NotOkToOpenMonitor,
                                                    OfflineResourceMonitor,
                                                    SequencerEnableMonitor,
                                                    EnclosureInterlockMonitor)


class OfflineResourceMonitorTest(unittest.TestCase):

    def test_telescope_is_offline(self):
        monitor = OfflineResourceMonitor(self._create_resource('offline'))
        event   = monitor.monitor()

        eq_(event['0m8a.doma.sqa'].type, 'OFFLINE')

    def test_telescope_is_online(self):
        monitor = OfflineResourceMonitor(self._create_resource('online'))
        event   = monitor.monitor()

        assert_false(event)

    def _create_resource(self,state):
        resource_string = """[ { 'name':'0m8a.doma.sqa', 'status':'%s' } ]"""
        return StringIO(resource_string % (state))

class NotOkToOpenMonitorTest(unittest.TestCase):

    def setUp(self):
        self.monitor = NotOkToOpenMonitor()

    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_no_event_if_we_are_okay_to_open(self,mock_get_datum):
        mock_get_datum.side_effect = self._create_events('true','0','')

        event = self.monitor.monitor()

        assert_false(event)

    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_event_if_we_are_not_okay_to_open(self,mock_get_datum):
        mock_get_datum.side_effect = self._create_events('false','0','')

        event = self.monitor.monitor()

        assert_true(event)

    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_event_expands_to_all_resource_on_site(self,mock_get_datum):
        mock_get_datum.side_effect = self._create_events('false','0','')

        event = self.monitor.monitor()

        resources = sorted(['1m0a.doma.lsc','1m0a.domb.lsc','1m0a.domc.lsc'])
        eq_(resources, sorted(event.keys()))

    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_event_gives_reason(self,mock_get_datum):
        mock_get_datum.side_effect = self._create_events('false','0','Dew Point')

        event = self.monitor.monitor()

        eq_(event['1m0a.doma.lsc'].reason, 'Dew Point')

    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_no_event_if_sun_up(self,mock_get_datum):
        mock_get_datum.side_effect = self._create_events('false','0','Sun Up')

        event = self.monitor.monitor()

        assert_false(event)

    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_no_event_if_sun_up_lowercase(self,mock_get_datum):
        mock_get_datum.side_effect = self._create_events('false','0','sun up')

        event = self.monitor.monitor()

        assert_false(event)

    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_no_event_if_ok_to_open_flag_uppercase(self,mock_get_datum):
        mock_get_datum.side_effect = self._create_events('FaLsE','0','dew')

        event = self.monitor.monitor()

        assert_true(event)

    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_end_time_includes_countdown(self,mock_get_datum):
        mock_get_datum.side_effect = self._create_events('false','400','Dew')

        event = self.monitor.monitor()

        expected_time = datetime(2013,04,26) + timedelta(seconds=400)
        eq_(event['1m0a.doma.lsc'].end_time, expected_time)


    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_no_events_if_inconsistent_size_data_lists(self, mock_get_datum):
        mock_get_datum.side_effect = _mocked_get_datum_inconsistent_sizes

        event = self.monitor.monitor()

        eq_(0, len(event))

    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_no_events_if_inconsistent_sites_in_data_lists(self, mock_get_datum):
        mock_get_datum.side_effect = _mocked_get_datum_inconsistent_sites

        event = self.monitor.monitor()

        eq_(0, len(event))

    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_event_if_consistent_sites_in_data_lists(self, mock_get_datum):
        mock_get_datum.side_effect = _mocked_get_datum_consistent

        event = self.monitor.monitor()

        eq_('NOT OK TO OPEN', event.get('1m0a.doma.elp').type)


    def _create_events(self,oktoopen,countdown,reason):
        return [[_create_event(self, oktoopen),],
                [_create_event(self, countdown),],
                [_create_event(self, reason),],]



def _create_event(self, value, site='lsc'):
    return Datum(site               = site,
                 observatory        = site,
                 telescope          = site,
                 instance           = '1',
                 timestamp_changed  = datetime(2013,04,26,0,0,0),
                 timestamp_measured = datetime(2013,04,26,0,0,0),
                 value              = value,
                 persistence_model  = 'STATUS')

def _mocked_get_datum_inconsistent_sizes(datum, instance=None, engine=None, persistence_model=None):
    if datum == 'Weather Ok To Open':
        return [_create_event(object, 'true',  site='lsc'),
                _create_event(object, 'false', site='elp')]

    elif datum =='Weather Count Down To Open':
        return [_create_event(object,True, site='lsc')]

    else: # Reason
        return [_create_event(object, 'None', site='lsc'),
                _create_event(object, 'None', site='elp')]

def _mocked_get_datum_inconsistent_sites(datum, instance=None, engine=None, persistence_model=None):
    if datum == 'Weather Ok To Open':
        return [_create_event(object, 'true',  site='lsc'),
                _create_event(object, 'false', site='elp')]

    elif datum =='Weather Count Down To Open':
        return [_create_event(object, '10', site='lsc'),
                _create_event(object, '10', site='coj')]

    else: # Reason
        return [_create_event(object, 'None', site='lsc'),
                _create_event(object, 'None', site='elp')]

def _mocked_get_datum_consistent(datum, instance=None, engine=None, persistence_model=None):
    if datum == 'Weather Ok To Open':
        return [_create_event(object,'true', site='lsc'),
                _create_event(object,'false',site='elp')]

    elif datum =='Weather Count Down To Open':
        return [_create_event(object, '10', site='lsc'),
                _create_event(object, '10', site='elp')]

    else: # Reason
        return [_create_event(object, 'None', site='elp'),
                _create_event(object, 'None', site='lsc')]

class ScheduleTimestampMonitorTest(unittest.TestCase):

    def setUp(self):
        self.monitor = ScheduleTimestampMonitor()

    @mock.patch('adaptive_scheduler.monitoring.monitors.datetime')
    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_no_events_if_timetamp_within_tolerance(self,mock_get_datum,mock_dt):
        mock_dt.utcnow.return_value = datetime(2013,04,26)
        mock_get_datum.return_value = [self._create_event(),]

        event   = self.monitor.monitor()

        assert_false(event)

    @mock.patch('adaptive_scheduler.monitoring.monitors.datetime')
    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_event_returned_if_timetamp_out_of_tolerance(self,mock_get_datum,mock_dt):
        mock_dt.utcnow.return_value = datetime(2013,04,26)+timedelta(minutes=15)
        mock_get_datum.return_value = [self._create_event(),]

        event   = self.monitor.monitor()

        assert( '1m0a.doma.bpl' in event.keys() )
        eq_('No update since 2013-04-26T00:00:00', event['1m0a.doma.bpl'].reason)


    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_garbled_timestamp(self, mock_get_datum):
        garbled_datetime = 'gobbledegook'
        mock_get_datum.return_value = [self._create_event(dt_value=garbled_datetime),]

        event   = self.monitor.monitor()
        eq_("Unable to create datetime from 'gobbledegook'", event['1m0a.doma.bpl'].reason)



    def _create_event(self, dt_value='2013-04-26T00:00:00'):
        return Datum(site                 = 'bpl',
                    observatory          = 'bpl',
                    telescope            = 'bpl',
                    instance             = '1m0a.doma',
                    timestamp_changed    = datetime(2013,04,26,0,0,0),
                    timestamp_measured   = datetime(2013,04,26,0,0,0),
                    value                = dt_value,
                    persistence_model    = 'STATUS')


class SequencerEnableMonitorTest(unittest.TestCase):

    def setUp(self):
        self.monitor = SequencerEnableMonitor()


    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_no_events_when_sequencer_automatic(self, mock_get_datum):
        mock_get_datum.return_value = [self._create_event('AUTOMATIC'),]

        events = self.monitor.monitor()

        assert_false(events)


    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_event_when_sequencer_disabled(self, mock_get_datum):
        mock_get_datum.return_value = [self._create_event('DISABLED'),]

        events = self.monitor.monitor()

        assert_true(events)


    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_event_resource_is_returned(self, mock_get_datum):
        mock_get_datum.return_value = [self._create_event('DISABLED'),]

        events = self.monitor.monitor()

        assert_true( '1m0a.doma.bpl' in events.keys() )


    def _create_event(self, value):
        return Datum(site                 = 'bpl',
                     observatory          = 'doma',
                     telescope            = '1m0a',
                     instance             = '1',
                     timestamp_changed    = datetime(2013,04,26,0,0,0),
                     timestamp_measured   = datetime(2013,04,26,0,0,0),
                     value                = value,
                     persistence_model    = 'STATUS')


class EnclosureInterlockMonitorTest(unittest.TestCase):

    def setUp(self):
        self.monitor = EnclosureInterlockMonitor()

    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_event_when_enclosure_is_interlocked(self, mock_get_datum):
        interlocks = [('lsc', 'doma', 'True'), ('lsc', 'domb', 'True'), ('lsc', 'domc', 'True')]
        reasons = [('lsc', 'doma', 'WEATHER'), ('lsc', 'domb', 'POWER'), ('lsc', 'domc', 'FLAPPING')]
        results = [[self._create_event(*y) for y in x] for x in [interlocks, reasons]]

        mock_get_datum.side_effect = results

        events = self.monitor.monitor()

        assert_equals(set(events.keys()), set(['1m0a.doma.lsc', '1m0a.domb.lsc', '1m0a.domc.lsc']))

    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_no_event_when_enclosure_is_not_interlocked(self, mock_get_datum):
        interlocks = [('lsc', 'doma', 'False'), ('lsc', 'domb', 'False'), ('lsc', 'domc', 'False')]
        reasons = [('lsc', 'doma', ''), ('lsc', 'domb', ''), ('lsc', 'domc', '')]
        results = [[self._create_event(*y) for y in x] for x in [interlocks, reasons]]

        mock_get_datum.side_effect = results

        events = self.monitor.monitor()

        assert_false(events)

    def _create_event(self, site, observatory, value):
        return Datum(site                 = site,
                     observatory          = observatory,
                     telescope            = observatory,
                     instance             = '1',
                     timestamp_changed    = datetime(2013, 04, 26, 0, 0, 0),
                     timestamp_measured   = datetime(2013, 04, 26, 0, 0, 0),
                     value                = value,
                     persistence_model    = 'STATUS')
