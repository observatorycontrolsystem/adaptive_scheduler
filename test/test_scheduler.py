from adaptive_scheduler.scheduler import Scheduler, SchedulerParameters
from adaptive_scheduler.model2 import UserRequest, Window
from adaptive_scheduler.interfaces import RunningRequest, RunningUserRequest
from adaptive_scheduler.kernel.timepoint import Timepoint
from adaptive_scheduler.kernel.reservation_v3 import Reservation_v3 as Reservation
from adaptive_scheduler.kernel_mappings import normalise_dt_intervals
from reqdb.requests import Request
# import helpers

from mock import Mock, patch
from nose.tools import assert_equal, assert_not_equal, assert_true

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
    
    
    @patch.object(Scheduler, 'on_new_schedule')
    @patch.object(Scheduler, 'prepare_available_windows_for_kernel')
    @patch.object(Scheduler, 'prepare_for_kernel')    
    @patch.object(Scheduler, 'filter_unscheduleable_child_requests')
    @patch.object(Scheduler, 'apply_window_filters')
    @patch.object(Scheduler, 'apply_unschedulable_filters')    
    def test_run_scheduler_normal_mode_with_schedulable_normal_single_ur(self, apply_unschedulable_filters_mock, apply_window_filters_mock,
                                                             filter_unscheduleable_child_requests_mock, prepare_for_kernel_mock,
                                                             prepare_available_windows_for_kernel_mock, on_new_schedule_mock):
        '''Should schedule a single normal request
        '''
        from adaptive_scheduler.kernel.fullscheduler_v6 import FullScheduler_v6
        
        ##### Start Scheduler interface mocks
        # Just return all input user requests
        apply_unschedulable_filters_mock.side_effect = (lambda *args : (args[0], []))
        # Just return all input user requests unchanged
        apply_window_filters_mock.side_effect = (lambda *args : args[0])
        # No unschedulable request in this case
        filter_unscheduleable_child_requests_mock.side_effect = (lambda *args : [])
        # Build mock reservation list
        scheduler_run_date = "2013-05-22 00:00:00"
        normalize_windows_to = datetime.strptime(scheduler_run_date, '%Y-%m-%d %H:%M:%S')
        window_dict1 = {
                         'start' : "2013-05-22 19:00:00",
                         'end'   : "2013-05-22 20:00:00",
                       }
        windows = (window_dict1,)
        request_duration_seconds = 60
        priority = 10
        tracking_number = 1
        request_number = 1
        request = self.create_request(request_number, duration=request_duration_seconds, windows=windows, possible_telescopes=['1m0a.doma.elp']) 
        normal_single_ur = self.create_user_request(tracking_number, priority, [request], 'single')
#         compound_reservation = self.build_compound_reservation(normal_single_ur, normalize_windows_to=normalize_windows_to, resources=['1m0a.doma.elp'])
#         prepare_for_kernel_mock.return_value = [compound_reservation]
        prepare_for_kernel_mock.side_effect = (lambda *args : [self.build_compound_reservation(ur, normalize_windows_to=normalize_windows_to, resources=['1m0a.doma.elp']) for ur in args[0]])
        available_start = datetime.strptime("2013-05-22 19:30:00", '%Y-%m-%d %H:%M:%S')
        available_end = datetime.strptime("2013-05-22 19:40:00", '%Y-%m-%d %H:%M:%S')
        available_intervals = {
                               '1m0a.doma.elp' : self.build_intervals([(available_start, available_end),], normalize_windows_to)
                               } 
        prepare_available_windows_for_kernel_mock.return_value = available_intervals
        ##### End Scheduler interface mocks
        
        ##### Start Scheduler parameter mocks
        event_bus_mock = Mock()
        network_snapshot_mock = Mock()
        network_snapshot_mock.running_tracking_numbers = Mock(return_value=[])
        intervals_mock = Mock(timepoints=[])
        network_snapshot_mock.blocked_intervals = Mock(return_value=intervals_mock)
        ##### End Scheduler parameter mocks
        
        ##### Start unmocked Scheduler parameters
        sched_params = SchedulerParameters() 
        sched_params.simulate_now = scheduler_run_date
        sched_params.timelimit_seconds = 5
        sched_params.slicesize_seconds = 300
        network_model = sched_params.get_model_builder().tel_network.telescopes
        scheduler_run_end = datetime.strptime("2013-05-22 00:00:00", '%Y-%m-%d %H:%M:%S')
        
        user_request_dict = {
                             'type' : Request.NORMAL_OBSERVATION_TYPE,
                             Request.NORMAL_OBSERVATION_TYPE : [normal_single_ur],
                             Request.TARGET_OF_OPPORTUNITY : [],
                             }
        ##### End unmocked Scheduler parameters
        
        ##### Start scheduler run
        scheduler = Scheduler(FullScheduler_v6, sched_params, event_bus_mock)
        scheduler_result = scheduler.run_scheduler(user_request_dict, network_snapshot_mock, network_model, scheduler_run_end)
        ##### End scheduler run
        
        assert_true(self.is_scheduled(1, scheduler_result.schedule))
        assert_equal(1, self.number_of_times_scheduled(request_number, scheduler_result.schedule))
        assert_true(self.is_schedule_on_resource(request_number, scheduler_result.schedule, '1m0a.doma.elp'))
        assert_true(self.doesnt_start_before(request_number, scheduler_result.schedule, available_start, normalize_windows_to))
        assert_true(self.doesnt_start_after(request_number, scheduler_result.schedule, available_end - timedelta(seconds=request_duration_seconds), normalize_windows_to))
        assert_true(self.scheduled_duration_is(request_number, scheduler_result.schedule, sched_params.slicesize_seconds, request_duration_seconds))
        
        assert_equal(sorted(network_model.keys()), sorted(scheduler_result.resource_schedules_to_cancel))
        assert_equal([], scheduler_result.unschedulable_user_request_numbers)
        assert_equal([], scheduler_result.unschedulable_request_numbers)
        
        
    @patch.object(Scheduler, 'on_new_schedule')
    @patch.object(Scheduler, 'prepare_available_windows_for_kernel')
    @patch.object(Scheduler, 'prepare_for_kernel')    
    @patch.object(Scheduler, 'filter_unscheduleable_child_requests')
    @patch.object(Scheduler, 'apply_window_filters')
    @patch.object(Scheduler, 'apply_unschedulable_filters')    
    def test_run_scheduler_normal_mode_with_schedulable_too_single_ur(self, apply_unschedulable_filters_mock, apply_window_filters_mock,
                                                             filter_unscheduleable_child_requests_mock, prepare_for_kernel_mock,
                                                             prepare_available_windows_for_kernel_mock, on_new_schedule_mock):
        '''Should not schedule anything since the scheduler run is for normal
        request and all that is present is a too request
        '''
        from adaptive_scheduler.kernel.fullscheduler_v6 import FullScheduler_v6
        
        ##### Start Scheduler interface mocks
        # Just return all input user requests
        apply_unschedulable_filters_mock.side_effect = (lambda *args : (args[0], []))
        # Just return all input user requests unchanged
        apply_window_filters_mock.side_effect = (lambda *args : args[0])
        # No unschedulable request in this case
        filter_unscheduleable_child_requests_mock.side_effect = (lambda *args : [])
        # Build mock reservation list
        scheduler_run_date = "2013-05-22 00:00:00"
        normalize_windows_to = datetime.strptime(scheduler_run_date, '%Y-%m-%d %H:%M:%S')
        window_dict1 = {
                         'start' : "2013-05-22 19:00:00",
                         'end'   : "2013-05-22 20:00:00",
                       }
        windows = (window_dict1,)
        request_duration_seconds = 60
        priority = 10
        tracking_number = 1
        request_number = 1
        request = self.create_request(request_number, duration=request_duration_seconds, windows=windows, possible_telescopes=['1m0a.doma.elp']) 
        normal_single_ur = self.create_user_request(tracking_number, priority, [request], 'single')
#         compound_reservation = self.build_compound_reservation(normal_single_ur, normalize_windows_to=normalize_windows_to, resources=['1m0a.doma.elp'])
#         prepare_for_kernel_mock.return_value = [compound_reservation]
        prepare_for_kernel_mock.side_effect = (lambda *args : [self.build_compound_reservation(ur, normalize_windows_to=normalize_windows_to, resources=['1m0a.doma.elp']) for ur in args[0]])
        available_start = datetime.strptime("2013-05-22 19:30:00", '%Y-%m-%d %H:%M:%S')
        available_end = datetime.strptime("2013-05-22 19:40:00", '%Y-%m-%d %H:%M:%S')
        available_intervals = {
                               '1m0a.doma.elp' : self.build_intervals([(available_start, available_end),], normalize_windows_to)
                               } 
        prepare_available_windows_for_kernel_mock.return_value = available_intervals
        ##### End Scheduler interface mocks
        
        ##### Start Scheduler parameter mocks
        event_bus_mock = Mock()
        network_snapshot_mock = Mock()
        network_snapshot_mock.running_tracking_numbers = Mock(return_value=[])
        intervals_mock = Mock(timepoints=[])
        network_snapshot_mock.blocked_intervals = Mock(return_value=intervals_mock)
        ##### End Scheduler parameter mocks
        
        ##### Start unmocked Scheduler parameters
        sched_params = SchedulerParameters() 
        sched_params.simulate_now = scheduler_run_date
        sched_params.timelimit_seconds = 5
        sched_params.slicesize_seconds = 300
        network_model = sched_params.get_model_builder().tel_network.telescopes
        scheduler_run_end = datetime.strptime("2013-05-22 00:00:00", '%Y-%m-%d %H:%M:%S')
        
        user_request_dict = {
                             'type' : Request.NORMAL_OBSERVATION_TYPE,
                             Request.NORMAL_OBSERVATION_TYPE : [],
                             Request.TARGET_OF_OPPORTUNITY : [normal_single_ur],
                             }
        ##### End unmocked Scheduler parameters
        
        ##### Start scheduler run
        scheduler = Scheduler(FullScheduler_v6, sched_params, event_bus_mock)
        scheduler_result = scheduler.run_scheduler(user_request_dict, network_snapshot_mock, network_model, scheduler_run_end)
        ##### End scheduler run
        
        
        assert_equal(None, scheduler_result.schedule)
        assert_equal({}, scheduler_result.resource_schedules_to_cancel)
        assert_equal([], scheduler_result.unschedulable_user_request_numbers)
        assert_equal([], scheduler_result.unschedulable_request_numbers)
        
        
    @patch.object(Scheduler, 'on_new_schedule')
    @patch.object(Scheduler, 'prepare_available_windows_for_kernel')
    @patch.object(Scheduler, 'prepare_for_kernel')    
    @patch.object(Scheduler, 'filter_unscheduleable_child_requests')
    @patch.object(Scheduler, 'apply_window_filters')
    @patch.object(Scheduler, 'apply_unschedulable_filters')    
    def test_run_scheduler_too_mode_with_schedulable_too_single_ur(self, apply_unschedulable_filters_mock, apply_window_filters_mock,
                                                             filter_unscheduleable_child_requests_mock, prepare_for_kernel_mock,
                                                             prepare_available_windows_for_kernel_mock, on_new_schedule_mock):
        '''Should schedule a single too request
        '''
        from adaptive_scheduler.kernel.fullscheduler_v6 import FullScheduler_v6
        
        ##### Start Scheduler interface mocks
        # Just return all input user requests
        apply_unschedulable_filters_mock.side_effect = (lambda *args : (args[0], []))
        # Just return all input user requests unchanged
        apply_window_filters_mock.side_effect = (lambda *args : args[0])
        # No unschedulable request in this case
        filter_unscheduleable_child_requests_mock.side_effect = (lambda *args : [])
        # Build mock reservation list
        scheduler_run_date = "2013-05-22 00:00:00"
        normalize_windows_to = datetime.strptime(scheduler_run_date, '%Y-%m-%d %H:%M:%S')
        window_dict1 = {
                         'start' : "2013-05-22 19:00:00",
                         'end'   : "2013-05-22 20:00:00",
                       }
        windows = (window_dict1,)
        request_duration_seconds = 60
        priority = 10
        tracking_number = 1
        request_number = 1
        request = self.create_request(request_number, duration=request_duration_seconds, windows=windows, possible_telescopes=['1m0a.doma.elp']) 
        normal_single_ur = self.create_user_request(tracking_number, priority, [request], 'single')
#         compound_reservation = self.build_compound_reservation(normal_single_ur, normalize_windows_to=normalize_windows_to, resources=['1m0a.doma.elp'])
#         prepare_for_kernel_mock.return_value = [compound_reservation]
        prepare_for_kernel_mock.side_effect = (lambda *args : [self.build_compound_reservation(ur, normalize_windows_to=normalize_windows_to, resources=['1m0a.doma.elp']) for ur in args[0]])
        available_start = datetime.strptime("2013-05-22 19:30:00", '%Y-%m-%d %H:%M:%S')
        available_end = datetime.strptime("2013-05-22 19:40:00", '%Y-%m-%d %H:%M:%S')
        available_intervals = {
                               '1m0a.doma.elp' : self.build_intervals([(available_start, available_end),], normalize_windows_to)
                               } 
        prepare_available_windows_for_kernel_mock.return_value = available_intervals
        ##### End Scheduler interface mocks
        
        ##### Start Scheduler parameter mocks
        event_bus_mock = Mock()
        network_snapshot_mock = Mock()
        network_snapshot_mock.running_tracking_numbers = Mock(return_value=[])
        intervals_mock = Mock(timepoints=[])
        network_snapshot_mock.blocked_intervals = Mock(return_value=intervals_mock)
        ##### End Scheduler parameter mocks
        
        ##### Start unmocked Scheduler parameters
        sched_params = SchedulerParameters() 
        sched_params.simulate_now = scheduler_run_date
        sched_params.timelimit_seconds = 5
        sched_params.slicesize_seconds = 300
        network_model = sched_params.get_model_builder().tel_network.telescopes
        scheduler_run_end = datetime.strptime("2013-05-22 00:00:00", '%Y-%m-%d %H:%M:%S')
        
        user_request_dict = {
                             'type' : Request.TARGET_OF_OPPORTUNITY,
                             Request.NORMAL_OBSERVATION_TYPE : [],
                             Request.TARGET_OF_OPPORTUNITY : [normal_single_ur],
                             }
        ##### End unmocked Scheduler parameters
        
        ##### Start scheduler run
        network_snapshot_mock.user_request_for_telescope = Mock(return_value=None)
        scheduler = Scheduler(FullScheduler_v6, sched_params, event_bus_mock)
        scheduler_result = scheduler.run_scheduler(user_request_dict, network_snapshot_mock, network_model, scheduler_run_end)
        ##### End scheduler run
        
        assert_true(self.is_scheduled(1, scheduler_result.schedule))
        assert_equal(1, self.number_of_times_scheduled(request_number, scheduler_result.schedule))
        assert_true(self.is_schedule_on_resource(request_number, scheduler_result.schedule, '1m0a.doma.elp'))
        assert_true(self.doesnt_start_before(request_number, scheduler_result.schedule, available_start, normalize_windows_to))
        assert_true(self.doesnt_start_after(request_number, scheduler_result.schedule, available_end - timedelta(seconds=request_duration_seconds), normalize_windows_to))
        assert_true(self.scheduled_duration_is(request_number, scheduler_result.schedule, sched_params.slicesize_seconds, request_duration_seconds))
        
        assert_equal(['1m0a.doma.elp'], scheduler_result.resource_schedules_to_cancel)
        assert_equal([], scheduler_result.unschedulable_user_request_numbers)
        assert_equal([], scheduler_result.unschedulable_request_numbers)
        
        
    @patch.object(Scheduler, 'on_new_schedule')
    @patch.object(Scheduler, 'prepare_available_windows_for_kernel')
    @patch.object(Scheduler, 'prepare_for_kernel')    
    @patch.object(Scheduler, 'filter_unscheduleable_child_requests')
    @patch.object(Scheduler, 'apply_window_filters')
    @patch.object(Scheduler, 'apply_unschedulable_filters')    
    def test_run_scheduler_too_mode_with_schedulable_too_single_ur_with_confilct(self, apply_unschedulable_filters_mock, apply_window_filters_mock,
                                                             filter_unscheduleable_child_requests_mock, prepare_for_kernel_mock,
                                                             prepare_available_windows_for_kernel_mock, on_new_schedule_mock):
        '''Should schedule a single too request on open telescope when available
        '''
        from adaptive_scheduler.kernel.fullscheduler_v6 import FullScheduler_v6
        
        ##### Start Scheduler interface mocks
        # Just return all input user requests
        apply_unschedulable_filters_mock.side_effect = (lambda *args : (args[0], []))
        # Just return all input user requests unchanged
        apply_window_filters_mock.side_effect = (lambda *args : args[0])
        # No unschedulable request in this case
        filter_unscheduleable_child_requests_mock.side_effect = (lambda *args : [])
        # Build mock reservation list
        scheduler_run_date = "2013-05-22 00:00:00"
        normalize_windows_to = datetime.strptime(scheduler_run_date, '%Y-%m-%d %H:%M:%S')
        window_dict1 = {
                         'start' : "2013-05-22 19:00:00",
                         'end'   : "2013-05-22 20:00:00",
                       }
        windows = (window_dict1,)
        request_duration_seconds = 60
        priority = 10
        too_tracking_number = 1
        too_request_number = 1
        normal_tracking_number = 2
        normal_request_number = 2
        too_request = self.create_request(too_request_number, duration=request_duration_seconds, windows=windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc']) 
        too_single_ur = self.create_user_request(too_tracking_number, priority, [too_request], 'single')
        
        normal_request = self.create_request(normal_request_number, duration=request_duration_seconds, windows=windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc']) 
        normal_single_ur = self.create_user_request(normal_tracking_number, priority, [normal_request], 'single')
#         compound_reservation = self.build_compound_reservation(normal_single_ur, normalize_windows_to=normalize_windows_to, resources=['1m0a.doma.elp'])
#         prepare_for_kernel_mock.return_value = [compound_reservation]
        prepare_for_kernel_mock.side_effect = (lambda *args : [self.build_compound_reservation(ur, normalize_windows_to=normalize_windows_to, resources=['1m0a.doma.elp', '1m0a.doma.lsc']) for ur in args[0]])
        available_start = datetime.strptime("2013-05-22 19:30:00", '%Y-%m-%d %H:%M:%S')
        available_end = datetime.strptime("2013-05-22 19:40:00", '%Y-%m-%d %H:%M:%S')
        available_intervals = {
                               '1m0a.doma.elp' : self.build_intervals([(available_start, available_end),], normalize_windows_to),
                               } 
        prepare_available_windows_for_kernel_mock.return_value = available_intervals
        ##### End Scheduler interface mocks
        
        ##### Start Scheduler parameter mocks
        event_bus_mock = Mock()
        network_snapshot_mock = Mock()
        network_snapshot_mock.running_tracking_numbers = Mock(return_value=[])
        intervals_mock = Mock(timepoints=[])
        network_snapshot_mock.blocked_intervals = Mock(return_value=intervals_mock)
        ##### End Scheduler parameter mocks
        
        ##### Start unmocked Scheduler parameters
        sched_params = SchedulerParameters() 
        sched_params.simulate_now = scheduler_run_date
        sched_params.timelimit_seconds = 5
        sched_params.slicesize_seconds = 300
        network_model = sched_params.get_model_builder().tel_network.telescopes
        scheduler_run_end = datetime.strptime("2013-05-22 00:00:00", '%Y-%m-%d %H:%M:%S')
        
        user_request_dict = {
                             'type' : Request.TARGET_OF_OPPORTUNITY,
                             Request.NORMAL_OBSERVATION_TYPE : [normal_single_ur],
                             Request.TARGET_OF_OPPORTUNITY : [too_single_ur],
                             }
        ##### End unmocked Scheduler parameters
        
        ##### Start scheduler run
        # Set up a normal running request at lsc which should prefer scheduling at elp
#         import ipdb; ipdb.set_trace();
        running_request = RunningRequest('1m0a.doma.lsc', normal_request_number)
        running_user_request = RunningUserRequest(normal_tracking_number, running_request)
        network_snapshot_mock.user_request_for_telescope = Mock(side_effect=(lambda tel : running_user_request if tel == '1m0a.doma.lsc' else None))
#         network_snapshot_mock.user_request_for_telescope = Mock(return_value=None)
        
        scheduler = Scheduler(FullScheduler_v6, sched_params, event_bus_mock)
        scheduler_result = scheduler.run_scheduler(user_request_dict, network_snapshot_mock, network_model, scheduler_run_end)
        ##### End scheduler run
        
        # This checks to see that windows are only created for the correct telescope resources
        assert_equal(1, prepare_for_kernel_mock.call_count)
        assert_equal(['1m0a.doma.elp'], prepare_for_kernel_mock.call_args[0][1].keys())
        assert_equal(1, prepare_available_windows_for_kernel_mock.call_count)
        assert_equal(['1m0a.doma.elp'], prepare_available_windows_for_kernel_mock.call_args[0][0].keys())
        
        assert_true(self.is_scheduled(1, scheduler_result.schedule))
        assert_equal(1, self.number_of_times_scheduled(too_request_number, scheduler_result.schedule))
        assert_true(self.is_schedule_on_resource(too_request_number, scheduler_result.schedule, '1m0a.doma.elp'))
        assert_true(self.doesnt_start_before(too_request_number, scheduler_result.schedule, available_start, normalize_windows_to))
        assert_true(self.doesnt_start_after(too_request_number, scheduler_result.schedule, available_end - timedelta(seconds=request_duration_seconds), normalize_windows_to))
        assert_true(self.scheduled_duration_is(too_request_number, scheduler_result.schedule, sched_params.slicesize_seconds, request_duration_seconds))
        
        assert_equal(['1m0a.doma.elp'], scheduler_result.resource_schedules_to_cancel)
        assert_equal([], scheduler_result.unschedulable_user_request_numbers)
        assert_equal([], scheduler_result.unschedulable_request_numbers)
        
    
    def is_scheduled(self, request_number, schedule):
        for resource, reservations in schedule.iteritems():
            for reservation in reservations:
                if reservation.request.request_number == request_number:
                    return True
        
        return False 
    
    
    def number_of_times_scheduled(self, request_number, schedule):
        times_scheduled = 0
        for resource, reservations in schedule.iteritems():
            for reservation in reservations:
                if reservation.request.request_number == request_number:
                    times_scheduled += 1
        
        return times_scheduled
        
    
    def is_schedule_on_resource(self, request_number, schedule, resource):
        for reservation in schedule.get(resource, []):
            if reservation.request.request_number == request_number:
                return True
        
        return False
    
    
    def doesnt_start_before(self, request_number, schedule, when, normalize_to):
        for resource, reservations in schedule.iteritems():
            for reservation in reservations:
                if reservation.request.request_number == request_number:
                    if normalize_to + timedelta(seconds=reservation.scheduled_start) < when:
                        return False
        
        return True
    
    
    def doesnt_start_after(self, request_number, schedule, when, normalize_to):
        for resource, reservations in schedule.iteritems():
            for reservation in reservations:
                if reservation.request.request_number == request_number:
                    if normalize_to + timedelta(seconds=reservation.scheduled_start) > when:
                        return False
        
        return True
    
    def scheduled_duration_is(self, request_number, schedule, slice_size, request_duration):
        expected_duration = (((request_duration - 1) / slice_size) + 1) * slice_size
        for resource, reservations in schedule.iteritems():
            for reservation in reservations:
                if reservation.request.request_number == request_number:
                    if reservation.scheduled_quantum != expected_duration:
                        return False
        
        return True
    
        
    def build_compound_reservation(self, ur, normalize_windows_to, resources):
        reservation_list = []
        for request in ur.requests:
            start_end_tuples = []
            for window in request.windows:
#                 start = datetime.strptime(window.start, '%Y-%m-%d %H:%M:%S')
#                 end = datetime.strptime(window.end, '%Y-%m-%d %H:%M:%S')
                start_end_tuples.append((window.start, window.end))
            
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
    
    
    def create_user_request(self, tracking_number, priority, requests, operator):#window_dicts, operator='and', resource_name='Martin', target=None, molecules=None, proposal=create_mock_proposal(), expires=None, duration=60):
        
        mock_user_request = Mock(tracking_number=tracking_number, priority=priority, requests=requests, operator=operator)
        mock_user_request.n_requests = Mock(return_value=len(requests))
        mock_user_request.get_priority = Mock(return_value=priority)
        
        return mock_user_request

    
    def create_request(self, request_number, duration, windows, possible_telescopes):
        model_windows = []
        for window in windows:
            for telescope in possible_telescopes:
                model_windows.append(Window(window, telescope))
        mock_request = Mock(request_number=request_number, duration=duration, windows=model_windows)
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
        

