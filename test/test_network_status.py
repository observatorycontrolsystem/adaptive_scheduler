'''
test_network_status.py - Test cases for the network_status module.

description

Author: Martin Norbury
        Eric Saunders
May 2013
'''
from nose.tools import assert_true, assert_false, eq_
from nose import SkipTest
import mock

import datetime

from adaptive_scheduler.monitoring.monitors       import NetworkStateMonitor, Event
from adaptive_scheduler.monitoring.network_status import Network


class TestNetworkStatus(object):

    def setup(self):
        self.mock_monitor1 = mock.MagicMock()
        self.mock_monitor2 = mock.MagicMock()
        self.network = Network([self.mock_monitor1, self.mock_monitor2])
        self.e1= Event(
                            type       = "NOT OK TO OPEN",
                            reason     = "DEWPOINT",
                            start_time = datetime.datetime(2013, 8, 14, 0, 0, 0),
                            end_time   = datetime.datetime(2013, 8, 14, 0, 15, 0),
                          )
        self.e1_later = Event(
                            type       = "NOT OK TO OPEN",
                            reason     = "DEWPOINT",
                            start_time = datetime.datetime(2013, 8, 14, 0, 0, 0),
                            end_time   = datetime.datetime(2013, 8, 14, 0, 25, 0),
                          )
        self.e2 = Event(
                            type       = "SITE AGENT UNRESPONSIVE",
                            reason     = "No update since 2013-06-01T00:00:00",
                            start_time = datetime.datetime(2013, 8, 14, 0, 0, 0),
                            end_time   = datetime.datetime(2013, 8, 14, 0, 25, 0),
                          )


    def test_monitor_called(self):
        self.network.update()
        assert_true(self.mock_monitor1.monitor.called)


    def test_monitor_events_sorted_by_resource(self):
        self.mock_monitor1.monitor.return_value = {'1m0a.doma.bpl':self.e1}
        self.mock_monitor2.monitor.return_value = {'1m0a.doma.bpl':self.e2}

        events       = self.network.update()
        eq_( events, {'1m0a.doma.bpl':[self.e1,self.e2]})


    def test_flag_is_dirty_if_first_time(self):
        self.mock_monitor1.monitor.return_value = {'1m0a.doma.bpl':self.e1}

        self.network.update()
        assert_true(self.network.has_changed())


    def test_flag_is_clean_if_no_change(self):
        self.mock_monitor1.monitor.return_value = {'1m0a.doma.bpl':self.e1}

        self.network.update()
        assert_true(self.network.has_changed())

        self.network.update()
        assert_false(self.network.has_changed())


    def test_flag_is_clean_if_no_change(self):
        self.mock_monitor1.monitor.return_value = {'1m0a.doma.bpl':self.e1}

        self.network.update()
        assert_true(self.network.has_changed())

        self.mock_monitor1.monitor.return_value = {'1m0a.doma.bpl':self.e2}

        self.network.update()
        assert_true(self.network.has_changed())


    def test_changing_timestamps_are_ignored_in_comparison(self):
        self.mock_monitor1.monitor.return_value = {'1m0a.doma.bpl' : self.e1}

        self.network.update()
        assert_true(self.network.has_changed(), 'Expected a network change')

        self.mock_monitor1.monitor.return_value = {'1m0a.doma.bpl' : self.e1_later}

        self.network.update()
        assert_false(self.network.has_changed(), 'Expected no network change')


    def test_network_change_detected_on_new_event(self):
        self.mock_monitor1.monitor.return_value = {}

        self.network.update()
        assert_false(self.network.has_changed(), 'Expected no network change')

        self.mock_monitor1.monitor.return_value = {'1m0a.doma.bpl' : self.e1}

        self.network.update()
        assert_true(self.network.has_changed(), 'Expected a network change')


    def test_network_change_detected_on_event_clear(self):
        self.mock_monitor1.monitor.return_value = {'1m0a.doma.bpl' : self.e1}

        self.network.update()
        assert_true(self.network.has_changed(), 'Expected a network change')

        self.network.update()
        assert_false(self.network.has_changed(), 'Expected no network change')

        self.mock_monitor1.monitor.return_value = {}

        self.network.update()
        assert_true(self.network.has_changed(), 'Expected a network change')


    def test_network_change_detected_on_second_resource_event(self):
        self.mock_monitor1.monitor.return_value = {'1m0a.doma.bpl' : self.e1}

        self.network.update()
        assert_true(self.network.has_changed(), 'Expected a network change')

        self.mock_monitor2.monitor.return_value = {'1m0a.domb.bpl' : self.e2}

        self.network.update()
        assert_true(self.network.has_changed(), 'Expected a network change')
