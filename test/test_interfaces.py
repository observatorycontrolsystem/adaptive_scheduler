from datetime import datetime, timedelta
from mock import Mock, MagicMock, patch
from adaptive_scheduler.valhalla_connections import ValhallaInterface
from adaptive_scheduler.interfaces import RunningUserRequest, RunningRequest, ResourceUsageSnapshot, NetworkInterface
from time_intervals.intervals import Intervals
from adaptive_scheduler.monitoring.network_status import Network, DATE_FORMATTER

import responses
import re
import socket
import json

from nose.tools import assert_equal, assert_true


class TestResourceUsageSnapshot(object):
    
    def test_running_intervals(self):
        start = datetime.utcnow()
        end = start + timedelta(minutes=10)
        running_r = RunningRequest('1m0a.doma.elp', 1, start, end)
        running_ur = RunningUserRequest(1, running_r)
        snapshot = ResourceUsageSnapshot(datetime.utcnow(), [running_ur], [])
        assert_equal(Intervals([]), snapshot.running_intervals('1m0a.doma.lsc'))
        assert_equal(Intervals([{'time': start, 'type': 'start'}, {'time': end, 'type': 'end'}]), snapshot.running_intervals('1m0a.doma.elp'))
    
    
    def test_running_requests_for_resources_returns_empty_list_no_resources_available(self):
        running_request = RunningRequest('1m0a.doma.elp', '0000000001', Mock(), Mock())
        running_ur = RunningUserRequest('0000000001', running_request)
        resource_usage_snapshot = ResourceUsageSnapshot(datetime.utcnow(), [running_ur], {})
        running_requests = resource_usage_snapshot.running_requests_for_resources([])
        assert_equal([], running_requests)
        
        running_request1 = RunningRequest('1m0a.doma.elp', '0000000001', Mock(), Mock())
        running_ur1 = RunningUserRequest('0000000001', running_request1)
        running_request2 = RunningRequest('1m0a.doma.elp', '0000000002', Mock(), Mock())
        running_ur2 = RunningUserRequest('0000000002', running_request2)
        resource_usage_snapshot = ResourceUsageSnapshot(datetime.utcnow(), [running_ur1, running_ur2], {})
        running_requests = resource_usage_snapshot.running_requests_for_resources([])
        assert_equal([], running_requests)
        
    def test_running_requests_for_resources_returns_empty_list_no_requests_are_running(self):
        resource_usage_snapshot = ResourceUsageSnapshot(datetime.utcnow(), [], {})
        running_requests = resource_usage_snapshot.running_requests_for_resources(['1m0a.doma.elp'])
        assert_equal([], running_requests)
        
        running_request = RunningRequest('1m0a.doma.elp', '0000000001', Mock(), Mock())
        running_ur = RunningUserRequest('0000000001', running_request)
        resource_usage_snapshot = ResourceUsageSnapshot(datetime.utcnow(), [running_ur], {})
        running_requests = resource_usage_snapshot.running_requests_for_resources(['1m0a.doma.lsc'])
        assert_equal([], running_requests)
        
    def test_running_requests_for_resources_returns_running_requests(self):
#         import ipdb; ipdb.set_trace();
        running_request1 = RunningRequest('1m0a.doma.elp', '0000000001', Mock(), Mock())
        running_ur1 = RunningUserRequest('0000000001', running_request1)
        running_request2 = RunningRequest('1m0a.doma.lsc', '0000000002', Mock(), Mock())
        running_ur2 = RunningUserRequest('0000000002', running_request2)
        resource_usage_snapshot = ResourceUsageSnapshot(datetime.utcnow(), [running_ur1, running_ur2], {})
        running_requests = resource_usage_snapshot.running_requests_for_resources(['1m0a.doma.elp', '1m0a.doma.lsc'])
        assert_equal(2, len(running_requests))
        assert_true(running_request1 in running_requests)
        assert_true(running_request2 in running_requests)
        
        
#     def test_blacklist_running_sub_requests_only(self):
#         '''Test that the not running children of a MANY user request are not prevented
#         from being scheduled by another child request that is running
#         '''
#         self.assert_equal(True, False)

class TestNetworkInterface(object):

    @responses.activate
    def test_send_telescope_available_state_events_to_es(self):
        es_endpoint = 'http://test-es/document/'
        es_endpoint_re = re.compile(r'http://test-es/document/.*')
        responses.add(responses.POST, es_endpoint_re, body='{"success":"yay"}', status=200)
        opentsdb_endpoint_re = re.compile(r'http://opentsdbdev.lco.gtn:4242/api/put.*')
        responses.add(responses.POST, opentsdb_endpoint_re, body='{"success":"yay"}', status=200)

        network_state = Network(MagicMock(), es_endpoint=es_endpoint)
        network_interface = NetworkInterface(MagicMock(), MagicMock(), network_state, MagicMock())

        telescope_name_list = ['1m0a.doma.tst']
        network_interface.send_available_telescope_state_events(telescope_name_list)

        event_dict = json.loads(responses.calls[0].request.body)

        event1_dict = {'type': 'AVAILABLE',
                      'reason': 'Available for scheduling',
                      'start_time': event_dict['start_time'],
                      'end_time': event_dict['end_time'],
                      'name': '1m0a.doma.tst',
                      'telescope': '1m0a',
                      'enclosure': 'doma',
                      'site': 'tst',
                      'timestamp': event_dict['timestamp'],
                      'hostname': socket.gethostname()}

        assert_equal(event_dict, event1_dict)
