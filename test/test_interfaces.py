from datetime import datetime, timedelta
from mock import Mock, MagicMock, patch
from adaptive_scheduler.observation_portal_connections import ObservationPortalInterface
from adaptive_scheduler.interfaces import RunningRequestGroup, RunningRequest, ResourceUsageSnapshot, NetworkInterface
from time_intervals.intervals import Intervals
from adaptive_scheduler.monitoring.network_status import Network

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
        running_ur = RunningRequestGroup(1, running_r)
        snapshot = ResourceUsageSnapshot(datetime.utcnow(), [running_ur], [])
        assert_equal(Intervals([]), snapshot.running_intervals('1m0a.doma.lsc'))
        assert_equal(Intervals([{'time': start, 'type': 'start'}, {'time': end, 'type': 'end'}]), snapshot.running_intervals('1m0a.doma.elp'))
    
    
    def test_running_requests_for_resources_returns_empty_list_no_resources_available(self):
        running_request = RunningRequest('1m0a.doma.elp', '0000000001', Mock(), Mock())
        running_ur = RunningRequestGroup('0000000001', running_request)
        resource_usage_snapshot = ResourceUsageSnapshot(datetime.utcnow(), [running_ur], {})
        running_requests = resource_usage_snapshot.running_requests_for_resources([])
        assert_equal([], running_requests)
        
        running_request1 = RunningRequest('1m0a.doma.elp', '0000000001', Mock(), Mock())
        running_ur1 = RunningRequestGroup('0000000001', running_request1)
        running_request2 = RunningRequest('1m0a.doma.elp', '0000000002', Mock(), Mock())
        running_ur2 = RunningRequestGroup('0000000002', running_request2)
        resource_usage_snapshot = ResourceUsageSnapshot(datetime.utcnow(), [running_ur1, running_ur2], {})
        running_requests = resource_usage_snapshot.running_requests_for_resources([])
        assert_equal([], running_requests)
        
    def test_running_requests_for_resources_returns_empty_list_no_requests_are_running(self):
        resource_usage_snapshot = ResourceUsageSnapshot(datetime.utcnow(), [], {})
        running_requests = resource_usage_snapshot.running_requests_for_resources(['1m0a.doma.elp'])
        assert_equal([], running_requests)
        
        running_request = RunningRequest('1m0a.doma.elp', '0000000001', Mock(), Mock())
        running_ur = RunningRequestGroup('0000000001', running_request)
        resource_usage_snapshot = ResourceUsageSnapshot(datetime.utcnow(), [running_ur], {})
        running_requests = resource_usage_snapshot.running_requests_for_resources(['1m0a.doma.lsc'])
        assert_equal([], running_requests)
        
    def test_running_requests_for_resources_returns_running_requests(self):
#         import ipdb; ipdb.set_trace();
        running_request1 = RunningRequest('1m0a.doma.elp', '0000000001', Mock(), Mock())
        running_ur1 = RunningRequestGroup('0000000001', running_request1)
        running_request2 = RunningRequest('1m0a.doma.lsc', '0000000002', Mock(), Mock())
        running_ur2 = RunningRequestGroup('0000000002', running_request2)
        resource_usage_snapshot = ResourceUsageSnapshot(datetime.utcnow(), [running_ur1, running_ur2], {})
        running_requests = resource_usage_snapshot.running_requests_for_resources(['1m0a.doma.elp', '1m0a.doma.lsc'])
        assert_equal(2, len(running_requests))
        assert_true(running_request1 in running_requests)
        assert_true(running_request2 in running_requests)
