from adaptive_scheduler.scheduler import Scheduler, SchedulerParameters
from adaptive_scheduler.model2 import UserRequest
from adaptive_scheduler.kernel.timepoint import Timepoint
from adaptive_scheduler.kernel.reservation_v3 import Reservation_v3 as Reservation
from adaptive_scheduler.kernel_mappings import normalise_dt_intervals
from reqdb.requests import Request
import helpers

from mock import Mock
from nose.tools import assert_equal, assert_not_equal

from datetime import datetime, timedelta

class TestSchduler(object):
    
    def build_ur_list(self, *tracking_numbers):
        ur_list = []
        for tracking_number in tracking_numbers:
            ur = UserRequest(
                               operator='single',
                               requests=None,
                               proposal=None,
                               tracking_number=tracking_number,
                               group_id=None,
                               expires=None,
                             )

            ur_list.append(ur)
        
        return ur_list
    
    def test_blacklist_running_user_requests_returns_empty_list_when_only_request_running(self):
        scheduler = Scheduler(None, None, None)
        
        ur_tracking_numbers = ['0000000001']
        running_ur_tracking_numbers = ['0000000001']
        ur_list = self.build_ur_list(*ur_tracking_numbers)
        schedulable_urs = scheduler.blacklist_running_user_requests(ur_list, running_ur_tracking_numbers)
        assert_equal([], schedulable_urs)
        
        ur_tracking_numbers = ['0000000001']
        running_ur_tracking_numbers = ['0000000001', '0000000002']
        ur_list = self.build_ur_list(*ur_tracking_numbers)
        schedulable_urs = scheduler.blacklist_running_user_requests(ur_list, running_ur_tracking_numbers)
        assert_equal([], schedulable_urs)
        
    def test_blacklist_running_user_requests_returns_empty_list_with_empty_ur_list(self):
        
        scheduler = Scheduler(None, None, None)
        ur_tracking_numbers = []
        running_ur_tracking_numbers = []
        ur_list = self.build_ur_list(*ur_tracking_numbers)
        schedulable_urs = scheduler.blacklist_running_user_requests(ur_list, running_ur_tracking_numbers)
        assert_equal([], schedulable_urs)
        
        ur_tracking_numbers = []
        running_ur_tracking_numbers = ['0000000001']
        ur_list = self.build_ur_list(*ur_tracking_numbers)
        schedulable_urs = scheduler.blacklist_running_user_requests(ur_list, running_ur_tracking_numbers)
        assert_equal([], schedulable_urs)
        
    def test_blacklist_running_user_requests_returns_all_requests_when_none_running(self):
        
        scheduler = Scheduler(None, None, None)
        ur_tracking_numbers = []
        running_ur_tracking_numbers = []
        ur_list = self.build_ur_list(*ur_tracking_numbers)
        schedulable_urs = scheduler.blacklist_running_user_requests(ur_list, running_ur_tracking_numbers)
        assert_equal(len(ur_list), len(schedulable_urs))
        
        ur_tracking_numbers = ['0000000001']
        running_ur_tracking_numbers = []
        ur_list = self.build_ur_list(*ur_tracking_numbers)
        schedulable_urs = scheduler.blacklist_running_user_requests(ur_list, running_ur_tracking_numbers)
        assert_equal(len(ur_list), len(schedulable_urs))
        
        ur_tracking_numbers = ['0000000001']
        running_ur_tracking_numbers = ['0000000002']
        ur_list = self.build_ur_list(*ur_tracking_numbers)
        schedulable_urs = scheduler.blacklist_running_user_requests(ur_list, running_ur_tracking_numbers)
        assert_equal(len(ur_list), len(schedulable_urs))
        
        ur_tracking_numbers = ['0000000001', '0000000002']
        running_ur_tracking_numbers = []
        ur_list = self.build_ur_list(*ur_tracking_numbers)
        schedulable_urs = scheduler.blacklist_running_user_requests(ur_list, running_ur_tracking_numbers)
        assert_equal(len(ur_list), len(schedulable_urs))
        
    def test_run_scheduler_with_mocked_interfaces(self):
        event_bus_mock = Mock()
        sched_params = SchedulerParameters()
        
        user_request_dict = {
                             'type' : Request.NORMAL_OBSERVATION_TYPE,
                             Request.NORMAL_OBSERVATION_TYPE : [],
                             Request.TARGET_OF_OPPORTUNITY : [],
                             }
        network_snapshot_mock = Mock()
        network_snapshot_mock.running_tracking_numbers = Mock(return_value=[])
        intervals_mock = Mock()
        intervals_mock.timepoints = []
        network_snapshot_mock.blocked_intervals = Mock(return_value=intervals_mock)
        network_model = sched_params.get_model_builder().tel_network.telescopes
        estimated_scheduler_end = datetime.utcnow()
        
        kernel_class_mock = Mock()
        scheduler = Scheduler(kernel_class_mock, sched_params, event_bus_mock)
        scheduler_result = scheduler.run_scheduler(user_request_dict, network_snapshot_mock, network_model, estimated_scheduler_end)
        
        assert_equal(None, scheduler_result.schedule)
        assert_equal({}, scheduler_result.resource_schedules_to_cancel)
        assert_equal([], scheduler_result.unschedulable_user_request_numbers)
        assert_equal([], scheduler_result.unschedulable_request_numbers)
        
        
    def test_run_scheduler_with_schedulable_normal_single_ur(self):
        event_bus_mock = Mock()
        sched_params = SchedulerParameters()
        
        date_before_window = datetime.strptime("2013-05-22 00:00:00", '%Y-%m-%d %H:%M:%S')
        window_dict1 = {
                         'start' : "2013-05-22 19:00:00",
                         'end'   : "2013-05-22 20:00:00",
                       }
        windows = [ (window_dict1,) ]
        
        normal_single_ur, normal_single_ur_window_list = helpers.create_user_request(windows, operator='single', resource_name='1m0a.doma.elp')
        
        user_request_dict = {
                             'type' : Request.NORMAL_OBSERVATION_TYPE,
                             Request.NORMAL_OBSERVATION_TYPE : [normal_single_ur],
                             Request.TARGET_OF_OPPORTUNITY : [],
                             }
        network_snapshot_mock = Mock()
        network_snapshot_mock.running_tracking_numbers = Mock(return_value=[])
        intervals_mock = Mock(timepoints=[])
        network_snapshot_mock.blocked_intervals = Mock(return_value=intervals_mock)
        network_model = sched_params.get_model_builder().tel_network.telescopes
        estimated_scheduler_end = date_before_window
        
        kernel_class_mock = Mock()
        scheduler = Scheduler(kernel_class_mock, sched_params, event_bus_mock)
        scheduler.apply_window_filters = Mock(side_effect = (lambda *args : args[0]) )
        compound_reservation = self.build_compound_reservation(normal_single_ur, windows[0])
        scheduler.prepare_for_kernel = Mock(return_value=[compound_reservation])
        scheduler.on_new_schedule = Mock()
        scheduler_result = scheduler.run_scheduler(user_request_dict, network_snapshot_mock, network_model, estimated_scheduler_end)
        
        assert_not_equal(None, scheduler_result.schedule)
        assert_equal(network_model, scheduler_result.resource_schedules_to_cancel)
        assert_equal([], scheduler_result.unschedulable_user_request_numbers)
        assert_equal([], scheduler_result.unschedulable_request_numbers)
        
        
    def build_compound_reservation(self, ur, windows=[], resource='1m0a.doma.elp', normalize_windows_to=None, duration=60):
        
        # default 
        begining_of_epoch = datetime.utcnow() - timedelta(days=365)
        if normalize_windows_to:
            begining_of_epoch = normalize_windows_to
        
        timepoints = []
        for window in windows:
            start = datetime.strptime(window['start'], '%Y-%m-%d %H:%M:%S')
            end = datetime.strptime(window['end'], '%Y-%m-%d %H:%M:%S')
            start_timepoint = Timepoint(time=start, type='start')
            end_timepoint = Timepoint(time=end, type='end')
            timepoints.append(start_timepoint)
            timepoints.append(end_timepoint)
        epoch_timepoints = Mock(timepoints=timepoints)
        
        intervals = normalise_dt_intervals(epoch_timepoints, begining_of_epoch)
        
        window_dict = {}
        window_dict[resource] = intervals
        
        res = Reservation(ur.priority, duration, window_dict)
        compound_reservation_mock = Mock(reservation_list=[res])
        
        return compound_reservation_mock

