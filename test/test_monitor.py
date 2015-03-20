'''
test_monitors.py - Test cases for the monitors module.

description

Author: Martin Norbury
May 2013
'''

from nose.tools import eq_, assert_false, assert_true
from datetime import datetime, timedelta
import mock
import unittest
from StringIO import StringIO

from adaptive_scheduler.monitoring.telemetry import Datum
from adaptive_scheduler.monitoring.monitors import (ScheduleTimestampMonitor,
                                                    NotOkToOpenMonitor,
                                                    OfflineResourceMonitor,
                                                    SequencerEnableMonitor)


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
    def test_error_if_inconsistent_datum_lists(self,mock_get_datum):
        ok = self._ok_list()
        cd = self._count_down_list()
        fr = self._failure_reason_list()
        list_of_return_values=[ok,cd,fr]
        def side_effect():
            return list_of_return_values.pop()

        mock_get_datum.side_effect = side_effect

        #mock_get_datum('Weather Ok To Open', 1).return_value = self._ok_list()
        #mock_get_datum('Weather Count Down To Open', 1).return_value = self._count_down_list()
        #mock_get_datum('Weather Failure Reason', 1).return_value = self._failure_reason_list()

        event   = self.monitor.monitor()

        assert( '1m0a.doma.bpl' in event.keys() )
        eq_('No update since 2013-04-26T00:00:00', event['1m0a.doma.bpl'].reason)

    def _create_events(self,oktoopen,countdown,reason):
        return [[self._create_event(oktoopen),],
                [self._create_event(countdown),],
                [self._create_event(reason),],]

    def _create_event(self,value):
        return Datum(site                = 'lsc',
                    observatory          = 'lsc',
                    telescope            = 'lsc',
                    instance             = '1',
                    timestamp_changed    = datetime(2013,04,26,0,0,0),
                    timestamp_measured   = datetime(2013,04,26,0,0,0),
                    value                 = value,
                    persistence_model    = 'STATUS')

    def _ok_list(self):
        return [Datum(site                 = 'bpl',
                    observatory          = 'bpl',
                    telescope            = 'bpl',
                    instance             = '1m0a.doma',
                    timestamp_changed    = datetime(2013,04,26,0,0,0),
                    timestamp_measured   = datetime(2013,04,26,0,0,0),
                    value                = 'false',
                    persistence_model    = 'STATUS'),

                Datum(site                 = 'ogg',
                    observatory          = 'ogg',
                    telescope            = 'ogg',
                    instance             = '1m0a.doma',
                    timestamp_changed    = datetime(2013,04,26,0,0,0),
                    timestamp_measured   = datetime(2013,04,26,0,0,0),
                    value                = 'false',
                    persistence_model    = 'STATUS'),

                Datum(site                 = 'coj',
                    observatory          = 'coj',
                    telescope            = 'coj',
                    instance             = '1m0a.doma',
                    timestamp_changed    = datetime(2013,04,26,0,0,0),
                    timestamp_measured   = datetime(2013,04,26,0,0,0),
                    value                = 'false',
                    persistence_model    = 'STATUS')]


    def _count_down_list(self):
        return [Datum(site                 = 'bpl',
                    observatory          = 'bpl',
                    telescope            = 'bpl',
                    instance             = '1m0a.doma',
                    timestamp_changed    = datetime(2013,04,26,0,0,0),
                    timestamp_measured   = datetime(2013,04,26,0,0,0),
                    value                = 900,
                    persistence_model    = 'STATUS'),

                Datum(site                 = 'ogg',
                    observatory          = 'ogg',
                    telescope            = 'ogg',
                    instance             = '1m0a.doma',
                    timestamp_changed    = datetime(2013,04,26,0,0,0),
                    timestamp_measured   = datetime(2013,04,26,0,0,0),
                    value                = 900,
                    persistence_model    = 'STATUS'),

                Datum(site                 = 'coj',
                    observatory          = 'coj',
                    telescope            = 'coj',
                    instance             = '1m0a.doma',
                    timestamp_changed    = datetime(2013,04,26,0,0,0),
                    timestamp_measured   = datetime(2013,04,26,0,0,0),
                    value                = 900,
                    persistence_model    = 'STATUS')]

    def _failure_reason_list(self):
        return [Datum(site                 = 'bpl',
                    observatory          = 'bpl',
                    telescope            = 'bpl',
                    instance             = '1m0a.doma',
                    timestamp_changed    = datetime(2013,04,26,0,0,0),
                    timestamp_measured   = datetime(2013,04,26,0,0,0),
                    value                = 'It is broke',
                    persistence_model    = 'STATUS'),

                Datum(site                 = 'ogg',
                    observatory          = 'ogg',
                    telescope            = 'ogg',
                    instance             = '1m0a.doma',
                    timestamp_changed    = datetime(2013,04,26,0,0,0),
                    timestamp_measured   = datetime(2013,04,26,0,0,0),
                    value                = 'Unknown',
                    persistence_model    = 'STATUS'),

                Datum(site                 = 'coj',
                    observatory          = 'coj',
                    telescope            = 'coj',
                    instance             = '1m0a.doma',
                    timestamp_changed    = datetime(2013,04,26,0,0,0),
                    timestamp_measured   = datetime(2013,04,26,0,0,0),
                    value                = 'Unknown',
                    persistence_model    = 'STATUS')]

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
