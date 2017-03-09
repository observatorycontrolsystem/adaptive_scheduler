from datetime import datetime, timedelta
from mock import Mock, MagicMock, patch
from reqdb.client import ConnectionError, RequestDBError, SchedulerClient
from adaptive_scheduler.requestdb import RequestDBInterface
from adaptive_scheduler.interfaces import RunningUserRequest, RunningRequest, ResourceUsageSnapshot, NetworkInterface
from adaptive_scheduler.kernel.timepoint import Timepoint
from adaptive_scheduler.kernel.intervals import Intervals
from adaptive_scheduler.monitoring.network_status import Network, DATE_FORMATTER

import responses
import re
import socket
import json

from nose.tools import assert_equal, assert_true
# class TestRequestDBInterface(object):
#
#     def __init__(self):
#         self.requestdb_client = SchedulerClient('http://localhost:8001/')
#         self.requestdb_client.set_dirty_flag = Mock()
#         self.rdbi = RequestDBInterface(self.requestdb_client)
#         self.log_mock = Mock()
#         self.rdbi.log = self.log_mock
#
#     def test_set_rs_to_unschedulable_1(self):
#         client = Mock()
#         r_numbers  = []
#         exception_str = 'foo'
#         self.requestdb_client.set_request_state = Mock(side_effect=ConnectionError(exception_str))
#         self.rdbi.set_requests_to_unschedulable(r_numbers)
#
#         msg = "Problem setting Request states to UNSCHEDULABLE: %s" % exception_str
#         self.log_mock.error.assert_called_with(msg)
#
#
#     def test_set_rs_to_unschedulable_2(self):
#         client = Mock()
#         r_numbers  = []
#         exception_str = 'foo'
#         self.requestdb_client.set_request_state = Mock(side_effect=RequestDBError(exception_str))
#         self.rdbi.set_requests_to_unschedulable(r_numbers)
#
#         msg = "Internal RequestDB error when setting UNSCHEDULABLE Request states: %s" % exception_str
#         self.log_mock.error.assert_called_with(msg)
#
#
#     def test_set_urs_to_unschedulable_1(self):
#         client = Mock()
#         ur_numbers = []
#         exception_str = 'bar'
#         self.requestdb_client.set_user_request_state = Mock(side_effect=ConnectionError(exception_str))
#         self.rdbi.set_user_requests_to_unschedulable(ur_numbers)
#
#         msg = "Problem setting User Request states to UNSCHEDULABLE: %s" % exception_str
#         self.log_mock.error.assert_called_with(msg)
#
#
#     def test_set_urs_to_unschedulable_2(self):
#         client = Mock()
#         ur_numbers = []
#         exception_str = 'bar'
#         self.requestdb_client.set_user_request_state = Mock(side_effect=RequestDBError(exception_str))
#         self.rdbi.set_user_requests_to_unschedulable(ur_numbers)
#
#         msg = "Internal RequestDB error when setting UNSCHEDULABLE User Request states: %s" % exception_str
#         self.log_mock.error.assert_called_with(msg)
#
#
#     def test_request_db_dirty_flag_is_invalid(self):
#         dirty_response = 'lalalala'
#
#         mock_client = Mock()
#         requestdb_interface = RequestDBInterface(mock_client)
#         assert_equal(requestdb_interface._request_db_dirty_flag_is_invalid(dirty_response), True)
#
#
class TestResourceUsageSnapshot(object):
    
    def test_running_intervals(self):
        start = datetime.utcnow()
        end = start + timedelta(minutes=10)
        running_r = RunningRequest('1m0a.doma.elp', 1, start, end)
        running_ur = RunningUserRequest(1, running_r)
        snapshot = ResourceUsageSnapshot(datetime.utcnow(), [running_ur], [])
        assert_equal(Intervals([]), snapshot.running_intervals('1m0a.doma.lsc'))
        assert_equal(Intervals([Timepoint(start, 'start'), Timepoint(end, 'end')]), snapshot.running_intervals('1m0a.doma.elp'))
    
    
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
        responses.add(responses.POST, es_endpoint_re, body='{"success":"yay"}',
                      status=200)

        network_state = Network(es_endpoint=es_endpoint)
        network_interface = NetworkInterface(MagicMock(), MagicMock(), network_state)

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
