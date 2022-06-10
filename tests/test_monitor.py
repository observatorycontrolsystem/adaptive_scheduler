'''
test_monitors.py - Test cases for the monitors module.

description

Author: Martin Norbury
May 2013
'''
import os
from datetime import datetime
import mock
from io import StringIO

from adaptive_scheduler.monitoring.opensearch_telemetry import Datum
from adaptive_scheduler.monitoring.monitors import (OfflineResourceMonitor,
                                                    AvailableForScheduling)
from adaptive_scheduler.configdb_connections import ConfigDBInterface

SRC_DIR = os.path.dirname(__file__)


class TestOfflineResourceMonitor(object):

    def test_telescope_is_offline(self):
        monitor = OfflineResourceMonitor(configdb_interface=ConfigDBInterface(configdb_url='', telescope_classes=[],
                                                                              telescopes_file=f'{SRC_DIR}/telescopes_sqa_offline.json',
                                                                              active_instruments_file=f'{SRC_DIR}/active_instruments.json'))
        event = monitor.monitor()

        assert event['0m8a.doma.sqa'].type == 'OFFLINE'

    def test_telescope_is_online(self):
        monitor = OfflineResourceMonitor(configdb_interface=ConfigDBInterface(configdb_url='', telescope_classes=[],
                                                                              telescopes_file=f'{SRC_DIR}/telescopes.json',
                                                                              active_instruments_file=f'{SRC_DIR}/active_instruments.json'))
        event = monitor.monitor()

        assert not event

    def _create_resource(self, state):
        resource_string = """[ { 'name':'0m8a.doma.sqa', 'status':'%s' } ]"""
        return StringIO(resource_string % (state))


class TestAvailableForSchedulingMonitor(object):

    def setup(self):
        self.monitor = AvailableForScheduling(
            configdb_interface=ConfigDBInterface(configdb_url='', telescope_classes=[],
                                                 telescopes_file=f'{SRC_DIR}/telescopes.json',
                                                 active_instruments_file=f'{SRC_DIR}/active_instruments.json'),
            opensearch_url='',
            os_index='',
            os_excluded_observatories=[]
        )

    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_no_event_if_we_are_okay_to_open(self, mock_get_datum):
        mock_get_datum.side_effect = self._create_events('true', '')

        event = self.monitor.monitor()

        assert not event

    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_event_if_we_are_not_okay_to_open(self, mock_get_datum):
        mock_get_datum.side_effect = self._create_events('false', 'There is a reason')

        event = self.monitor.monitor()

        assert event

    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_event_gives_reason(self, mock_get_datum):
        mock_get_datum.side_effect = self._create_events('false', 'There is a reason')

        event = self.monitor.monitor()

        assert event['lsc.lsc.lsc'].reason == 'There is a reason'

    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_event_if_consistent_sites_in_data_lists(self, mock_get_datum):
        mock_get_datum.side_effect = _mocked_get_datum_consistent

        event = self.monitor.monitor()

        assert 'NOT AVAILABLE' == event.get('elp.elp.elp').type

    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_no_events_when_available_for_scheduling(self, mock_get_datum):
        mock_get_datum.side_effect = self._create_events('true', '')

        events = self.monitor.monitor()

        assert not events

    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_event_when_not_available_for_scheduling(self, mock_get_datum):
        mock_get_datum.side_effect = self._create_events('false', 'There is a reason')

        events = self.monitor.monitor()

        assert events

    @mock.patch('adaptive_scheduler.monitoring.monitors.get_datum')
    def test_event_resource_is_returned(self, mock_get_datum):
        mock_get_datum.side_effect = self._create_events('false', 'There is a reason')

        events = self.monitor.monitor()

        assert 'lsc.lsc.lsc' in list(events.keys())

    def _create_events(self, available, reason):
        return [[_create_event(self, available), ],
                [_create_event(self, reason), ], ]


def _create_event(self, value, site='lsc', observatory=None, telescope=None):
    observatory = observatory if observatory else site
    telescope = telescope if telescope else observatory
    return Datum(**{'site': site,
                    'observatory': observatory,
                    'telescope': telescope,
                    'instance': '1',
                    'timestamp_changed': datetime(2013, 4, 26, 0, 0, 0),
                    'timestamp_measured': datetime(2013, 4, 26, 0, 0, 0),
                    'timestamp_recorded': datetime.utcnow(),
                    'value': value})


def _mocked_get_datum_consistent(datum, os_url='', os_index='', os_excluded_obs=None, instance=None):
    if datum == 'Available For Scheduling':
        return [_create_event(object, 'true', site='lsc'),
                _create_event(object, 'false', site='elp')]

    elif datum == 'Available For Scheduling Reason':
        return [_create_event(object, '', site='lsc'),
                _create_event(object, 'There is a reason', site='elp')]
