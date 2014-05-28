from adaptive_scheduler.scheduler import Scheduler, SchedulerParameters
from adaptive_scheduler.model2 import UserRequest
from adaptive_scheduler.kernel.timepoint import Timepoint
from adaptive_scheduler.kernel.reservation_v3 import Reservation_v3 as Reservation
from adaptive_scheduler.kernel_mappings import normalise_dt_intervals
from reqdb.requests import Request
# import helpers

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
        
# TODO: Use patch instead of side_effect mock for patched scheduler class methods
#     @patch.object(adaptive_scheduler.kernel.fullscheduler_v6.FullScheduler_v6, 'prepare_for_kernel')    
    def test_run_scheduler_with_schedulable_normal_single_ur(self):
        from adaptive_scheduler.kernel.fullscheduler_v6 import FullScheduler_v6
        
        event_bus_mock = Mock()
        scheduler_run_date = "2013-05-22 00:00:00"
        normalize_windows_to = datetime.strptime(scheduler_run_date, '%Y-%m-%d %H:%M:%S')
        
        sched_params = SchedulerParameters() 
        sched_params.simulate_now = scheduler_run_date
        sched_params.timelimit_seconds = 5
        
        date_before_window = datetime.strptime("2013-05-22 00:00:00", '%Y-%m-%d %H:%M:%S')
        window_dict1 = {
                         'start' : "2013-05-22 19:00:00",
                         'end'   : "2013-05-22 20:00:00",
                       }
        windows = (window_dict1,)
        
#         normal_single_ur, normal_single_ur_window_list = self.create_user_request(windows, operator='single', resource_name='1m0a.doma.elp')
        request_duration_seconds = 60
        request = self.create_request(1, duration=request_duration_seconds, windows=windows) 
        normal_single_ur = self.create_user_request(10, [request], 'single')
        
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
        
#         kernel_class_mock = Mock()
        scheduler = Scheduler(FullScheduler_v6, sched_params, event_bus_mock)
        # Just return all input user requests
        scheduler.apply_unschedulable_filters = Mock(side_effect = (lambda *args : (args[0], [])))
        # Just return all input user requests unchanged
        scheduler.apply_window_filters = Mock(side_effect = (lambda *args : args[0]) )
        # No unschedulable request in this case
        scheduler.filter_unscheduleable_child_requests = Mock(side_effect = (lambda *args : []))
#         import ipdb; ipdb.set_trace();
        compound_reservation = self.build_compound_reservation(normal_single_ur, normalize_windows_to=normalize_windows_to, resources=['1m0a.doma.elp'])
        scheduler.prepare_for_kernel = Mock(return_value=[compound_reservation])
        
        available_start = datetime.strptime("2013-05-22 19:30:00", '%Y-%m-%d %H:%M:%S')
        available_end = datetime.strptime("2013-05-22 19:40:00", '%Y-%m-%d %H:%M:%S')
        available_intervals = {
                               '1m0a.doma.elp' : self.build_intervals([(available_start, available_end),], normalize_windows_to)
                               } 
        scheduler.prepare_available_windows_for_kernel = Mock(return_value=available_intervals)
        
        scheduler.on_new_schedule = Mock()
        scheduler_result = scheduler.run_scheduler(user_request_dict, network_snapshot_mock, network_model, estimated_scheduler_end)
        
        # TODO: Move this into a method that checks that a specific request was scheduled
        assert_equal(1, len(scheduler_result.schedule['1m0a.doma.elp']))
        assert_equal(1, scheduler_result.schedule['1m0a.doma.elp'][0].request.request_number)
        assert_equal(True, scheduler_result.schedule['1m0a.doma.elp'][0].scheduled)
        assert_equal(True, normalize_windows_to + timedelta(seconds=scheduler_result.schedule['1m0a.doma.elp'][0].scheduled_start) >= available_start)
        assert_equal(True, normalize_windows_to + timedelta(seconds=scheduler_result.schedule['1m0a.doma.elp'][0].scheduled_start) <= available_end - timedelta(seconds=request_duration_seconds))
        
        assert_equal(network_model, scheduler_result.resource_schedules_to_cancel)
        assert_equal([], scheduler_result.unschedulable_user_request_numbers)
        assert_equal([], scheduler_result.unschedulable_request_numbers)
        
        
    def build_compound_reservation(self, ur, normalize_windows_to, resources):
        reservation_list = []
        for request in ur.requests:
            start_end_tuples = []
            for window in request.windows:
                start = datetime.strptime(window['start'], '%Y-%m-%d %H:%M:%S')
                end = datetime.strptime(window['end'], '%Y-%m-%d %H:%M:%S')
                start_end_tuples.append((start, end))
            
            intervals = self.build_intervals(start_end_tuples, normalize_windows_to)
        
            window_dict = {}
            for resource in resources:
                window_dict[resource] = intervals
            
            res = Reservation(ur.priority, request.duration, window_dict)
            res.request = request
            reservation_list.append(res)
            
        compound_reservation_mock = Mock(reservation_list=[res])
        
        return compound_reservation_mock
    
    
    def build_intervals(self, start_end_tuples, normailze_to):
        timepoints = []
        for start, end in start_end_tuples:
            start_timepoint = Timepoint(time=start, type='start')
            end_timepoint = Timepoint(time=end, type='end')
            timepoints.append(start_timepoint)
            timepoints.append(end_timepoint)
            
        epoch_timepoints = Mock(timepoints=timepoints)
        intervals = normalise_dt_intervals(epoch_timepoints, normailze_to)
        return intervals
    
    
    def create_user_request(self, priority, requests, operator):#window_dicts, operator='and', resource_name='Martin', target=None, molecules=None, proposal=create_mock_proposal(), expires=None, duration=60):
        
        mock_user_request = Mock(priority=priority, requests=requests, operator=operator)
        mock_user_request.n_requests = Mock(return_value=len(requests))
        
        return mock_user_request

    
    def create_request(self, request_number, duration, windows):
        mock_request = Mock(request_number=request_number, duration=duration, windows=windows)
        mock_request.get_duration = Mock(return_value=duration)
        mock_request.n_windows = Mock(return_value=len(windows))
        
        return mock_request

#         t1 = Telescope(
#                         name = resource_name
#                       )
# 
#         req_list = []
#         window_list = []
#         for req_windows in window_dicts:
#             windows = Windows()
#             for window_dict in req_windows:
#                 w = Window(
#                             window_dict = window_dict,
#                             resource    = t1
#                           )
#                 windows.append(w)
#                 window_list.append(w)
# 
#             Request.duration = PropertyMock(return_value=duration)
#             r  = Request(
#                           target         = target,
#                           molecules      = molecules,
#                           windows        = windows,
#                           constraints    = None,
#                           request_number = '0000000005'
#                         )
#             
#             r.get_duration = Mock(return_value=duration) 
#                 
#             
#             req_list.append(r)
# 
#         if len(req_list) == 1:
#             operator = 'single'
# 
#         if expires:
#             UserRequest.expires = PropertyMock(return_value=expires)
#         else:
#             UserRequest.expires = PropertyMock(return_value=datetime.utcnow() + timedelta(days=365))
#         ur1 = UserRequest(
#                            operator        = operator,
#                            requests        = req_list,
#                            proposal        = proposal,
#                            expires         = None,
#                            tracking_number = '0000000005',
#                            group_id        = None
#                          )
# 
#         return ur1, window_list
        

