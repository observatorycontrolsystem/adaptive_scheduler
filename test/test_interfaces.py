from datetime import datetime, timedelta
from mock import Mock
from reqdb.client import ConnectionError, RequestDBError, SchedulerClient
from adaptive_scheduler.requestdb import RequestDBInterface
from adaptive_scheduler.interfaces import RunningUserRequest, RunningRequest, ResourceUsageSnapshot
from adaptive_scheduler.kernel.timepoint import Timepoint
from adaptive_scheduler.kernel.intervals import Intervals

from nose.tools import assert_equal
class TestRequestDBInterface(object):
    
    def __init__(self):
        self.requestdb_client = SchedulerClient('http://localhost:8001/')
        self.requestdb_client.set_dirty_flag = Mock()
        self.rdbi = RequestDBInterface(self.requestdb_client)
        self.log_mock = Mock()
        self.rdbi.log = self.log_mock
    
    def test_set_rs_to_unschedulable_1(self):
        client = Mock()
        r_numbers  = []
        exception_str = 'foo'
        self.requestdb_client.set_request_state = Mock(side_effect=ConnectionError(exception_str))
        self.rdbi.set_requests_to_unschedulable(r_numbers)

        msg = "Problem setting Request states to UNSCHEDULABLE: %s" % exception_str
        self.log_mock.error.assert_called_with(msg)


    def test_set_rs_to_unschedulable_2(self):
        client = Mock()
        r_numbers  = []
        exception_str = 'foo'
        self.requestdb_client.set_request_state = Mock(side_effect=RequestDBError(exception_str))
        self.rdbi.set_requests_to_unschedulable(r_numbers)

        msg = "Internal RequestDB error when setting UNSCHEDULABLE Request states: %s" % exception_str
        self.log_mock.error.assert_called_with(msg)


    def test_set_urs_to_unschedulable_1(self):
        client = Mock()
        ur_numbers = []
        exception_str = 'bar'
        self.requestdb_client.set_user_request_state = Mock(side_effect=ConnectionError(exception_str))
        self.rdbi.set_user_requests_to_unschedulable(ur_numbers)

        msg = "Problem setting User Request states to UNSCHEDULABLE: %s" % exception_str
        self.log_mock.error.assert_called_with(msg)


    def test_set_urs_to_unschedulable_2(self):
        client = Mock()
        ur_numbers = []
        exception_str = 'bar'
        self.requestdb_client.set_user_request_state = Mock(side_effect=RequestDBError(exception_str))
        self.rdbi.set_user_requests_to_unschedulable(ur_numbers)

        msg = "Internal RequestDB error when setting UNSCHEDULABLE User Request states: %s" % exception_str
        self.log_mock.error.assert_called_with(msg)
        
    
    def test_request_db_dirty_flag_is_invalid(self):
        dirty_response = 'lalalala'
        
        mock_client = Mock()
        requestdb_interface = RequestDBInterface(mock_client)
        assert_equal(requestdb_interface._request_db_dirty_flag_is_invalid(dirty_response), True)
        
        
class TestResourceUsageSnapshot(object):
    
    def test_running_intervals(self):
        start = datetime.utcnow()
        end = start + timedelta(minutes=10)
        running_r = RunningRequest('1m0a.doma.elp', 1, start, end)
        running_ur = RunningUserRequest(1, running_r)
        snapshot = ResourceUsageSnapshot(datetime.utcnow(), [running_ur], {1 : 10}, [])
        assert_equal(Intervals([]), snapshot.running_intervals('1m0a.doma.lsc'))
        assert_equal(Intervals([Timepoint(start, 'start'), Timepoint(end, 'end')]), snapshot.running_intervals('1m0a.doma.elp'))
        
        