'''
test_network_status.py - Test cases for the network_status module.

description

Author: Martin Norbury
May 2013
'''
from nose.tools import assert_true, eq_
import mock

from adaptive_scheduler.monitoring.monitors       import NetworkStateMonitor
from adaptive_scheduler.monitoring.network_status import network_status

def test_monitor_called():
    mock_monitor = mock.MagicMock()
    network_status([mock_monitor])
    assert_true(mock_monitor.monitor.called)

def test_monitor_events_sorted_by_resource():
    mock_monitor1 = mock.MagicMock()
    mock_monitor1.monitor.return_value = {'1m0a.doma.bpl':1}
    mock_monitor2 = mock.MagicMock()
    mock_monitor2.monitor.return_value = {'1m0a.doma.bpl':2}

    events       = network_status([mock_monitor1,mock_monitor2])
    eq_( events, {'1m0a.doma.bpl':[1,2]})
