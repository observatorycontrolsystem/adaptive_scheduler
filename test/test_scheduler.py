from adaptive_scheduler.scheduler import Scheduler, SchedulerParameters, LCOGTNetworkScheduler, SchedulerRunner
from adaptive_scheduler.model2 import UserRequest, Window
from adaptive_scheduler.interfaces import RunningRequest, RunningUserRequest
from adaptive_scheduler.kernel.timepoint import Timepoint
from adaptive_scheduler.kernel.reservation_v3 import Reservation_v3 as Reservation
from adaptive_scheduler.kernel_mappings import normalise_dt_intervals
from adaptive_scheduler.kernel.fullscheduler_v6 import FullScheduler_v6
from reqdb.requests import Request
# import helpers

from mock import Mock, patch
from nose.tools import assert_equal, assert_not_equal, assert_true, assert_false

from datetime import datetime, timedelta

class TestSchduler(object):
    
    def __init__(self):
        self.scheduler_run_date = "2013-05-22 00:00:00"
        self.normalize_windows_to = datetime.strptime(self.scheduler_run_date, '%Y-%m-%d %H:%M:%S')
        
        self.sched_params = SchedulerParameters() 
        self.sched_params.simulate_now = self.scheduler_run_date
        self.sched_params.timelimit_seconds = 5
        self.sched_params.slicesize_seconds = 300
        self.network_model = self.sched_params.get_model_builder().tel_network.telescopes.keys()
        
        self.event_bus_mock = Mock()
        self.network_snapshot_mock = Mock()
        self.network_snapshot_mock.running_tracking_numbers = Mock(return_value=[])
        self.intervals_mock = Mock(timepoints=[])
        self.network_snapshot_mock.blocked_intervals = Mock(return_value=self.intervals_mock)
        
        
    
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
        scheduler = LCOGTNetworkScheduler(None, None, None)
        
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
        
        scheduler = LCOGTNetworkScheduler(None, None, None)
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
        
        scheduler = LCOGTNetworkScheduler(None, None, None)
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
        network_model = sched_params.get_model_builder().tel_network.telescopes.keys()
        estimated_scheduler_end = datetime.utcnow()
        
        kernel_class_mock = Mock()
        scheduler = Scheduler(kernel_class_mock, sched_params, event_bus_mock)
        scheduler_result = scheduler.run_scheduler(user_request_dict, network_snapshot_mock, network_model, estimated_scheduler_end)
        
        assert_equal(None, scheduler_result.schedule)
        assert_equal({}, scheduler_result.resource_schedules_to_cancel)
        assert_equal([], scheduler_result.unschedulable_user_request_numbers)
        assert_equal([], scheduler_result.unschedulable_request_numbers)
    
    
    
    def prepare_for_kernel_side_effect_factory(self, normailze_to_date):
        
        def side_effect(user_requests, resources_to_schedule, estimated_scheduler_end):
            return [self.build_compound_reservation(ur, normalize_windows_to=normailze_to_date, resources=resources_to_schedule) for ur in user_requests]
        
        return side_effect
    
    
    @patch.object(Scheduler, 'prepare_available_windows_for_kernel')
    @patch.object(Scheduler, 'prepare_for_kernel')    
    def test_run_scheduler_normal_mode_with_schedulable_normal_single_ur(self, prepare_for_kernel_mock, prepare_available_windows_for_kernel_mock):
        '''Should schedule a single normal request
        '''
        # Build mock user requests
        request_duration_seconds = 60
        priority = 10
        tracking_number = 1
        request_number = 1
        target_telescope = '1m0a.doma.elp' 
        request_windows = create_user_request_windows((("2013-05-22 19:00:00", "2013-05-22 20:00:00"),))
        request = create_request(request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=[target_telescope]) 
        normal_single_ur = create_user_request(tracking_number, priority, [request], 'single')
    
        # Build mock reservation list
        prepare_for_kernel_mock.side_effect = self.prepare_for_kernel_side_effect_factory(self.normalize_windows_to)
        
        # Create available intervals mock
        available_start = datetime.strptime("2013-05-22 19:30:00", '%Y-%m-%d %H:%M:%S')
        available_end = datetime.strptime("2013-05-22 19:40:00", '%Y-%m-%d %H:%M:%S')
        available_intervals = {
                               target_telescope : self.build_intervals([(available_start, available_end),], self.normalize_windows_to)
                               } 
        prepare_available_windows_for_kernel_mock.return_value = available_intervals
        
        # Create unmocked Scheduler parameters
        scheduler_run_end = datetime.strptime("2013-05-22 00:00:00", '%Y-%m-%d %H:%M:%S')
        user_request_dict = {
                             'type' : Request.NORMAL_OBSERVATION_TYPE,
                             Request.NORMAL_OBSERVATION_TYPE : [normal_single_ur],
                             Request.TARGET_OF_OPPORTUNITY : [],
                             }
        
        # Start scheduler run
        scheduler = Scheduler(FullScheduler_v6, self.sched_params, self.event_bus_mock)
        scheduler_result = scheduler.run_scheduler(user_request_dict, self.network_snapshot_mock, self.network_model, scheduler_run_end)

        # Start assertions
        assert_true(self.is_scheduled(1, scheduler_result.schedule))
        assert_equal(1, self.number_of_times_scheduled(request_number, scheduler_result.schedule))
        assert_true(self.is_schedule_on_resource(request_number, scheduler_result.schedule, target_telescope))
        assert_true(self.doesnt_start_before(request_number, scheduler_result.schedule, available_start, self.normalize_windows_to))
        assert_true(self.doesnt_start_after(request_number, scheduler_result.schedule, available_end - timedelta(seconds=request_duration_seconds), self.normalize_windows_to))
        assert_true(self.scheduled_duration_is(request_number, scheduler_result.schedule, self.sched_params.slicesize_seconds, request_duration_seconds))
        
        assert_equal(sorted(self.network_model), sorted(scheduler_result.resource_schedules_to_cancel))
        assert_equal([], scheduler_result.unschedulable_user_request_numbers)
        assert_equal([], scheduler_result.unschedulable_request_numbers)
        
        
    @patch.object(Scheduler, 'prepare_available_windows_for_kernel')
    @patch.object(Scheduler, 'prepare_for_kernel')    
    def test_run_scheduler_normal_mode_with_schedulable_too_single_ur(self, prepare_for_kernel_mock, prepare_available_windows_for_kernel_mock):
        '''Should not schedule anything since the scheduler run is for normal
        request and all that is present is a too request
        '''
        # Build mock user requests
        request_duration_seconds = 60
        priority = 10
        tracking_number = 1
        request_number = 1
        target_telescope = '1m0a.doma.elp'
        request_windows = create_user_request_windows((("2013-05-22 19:00:00", "2013-05-22 20:00:00"),))
        request = create_request(request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=[target_telescope]) 
        normal_single_ur = create_user_request(tracking_number, priority, [request], 'single')
        
        # Build mock reservation list
        prepare_for_kernel_mock.side_effect = self.prepare_for_kernel_side_effect_factory(self.normalize_windows_to)
        
        # Create available intervals mock
        available_start = datetime.strptime("2013-05-22 19:30:00", '%Y-%m-%d %H:%M:%S')
        available_end = datetime.strptime("2013-05-22 19:40:00", '%Y-%m-%d %H:%M:%S')
        available_intervals = {
                               target_telescope : self.build_intervals([(available_start, available_end),], self.normalize_windows_to)
                               } 
        prepare_available_windows_for_kernel_mock.return_value = available_intervals
        
        # Create unmocked Scheduler parameters
        scheduler_run_end = datetime.strptime("2013-05-22 00:00:00", '%Y-%m-%d %H:%M:%S')
        user_request_dict = {
                             'type' : Request.NORMAL_OBSERVATION_TYPE,
                             Request.NORMAL_OBSERVATION_TYPE : [],
                             Request.TARGET_OF_OPPORTUNITY : [normal_single_ur],
                             }
        
        # Start scheduler run
        scheduler = Scheduler(FullScheduler_v6, self.sched_params, self.event_bus_mock)
        scheduler_result = scheduler.run_scheduler(user_request_dict, self.network_snapshot_mock, self.network_model, scheduler_run_end)
        
        # Start assertions
        assert_equal(None, scheduler_result.schedule)
        assert_equal({}, scheduler_result.resource_schedules_to_cancel)
        assert_equal([], scheduler_result.unschedulable_user_request_numbers)
        assert_equal([], scheduler_result.unschedulable_request_numbers)
        
        
    @patch.object(Scheduler, 'prepare_available_windows_for_kernel')
    @patch.object(Scheduler, 'prepare_for_kernel')    
    def test_run_scheduler_too_mode_with_schedulable_too_single_ur(self, prepare_for_kernel_mock, prepare_available_windows_for_kernel_mock):
        '''Should schedule a single too request
        '''
        # Build mock user requests
        request_duration_seconds = 60
        priority = 10
        tracking_number = 1
        request_number = 1
        target_telescope = '1m0a.doma.elp'
        request_windows = create_user_request_windows((("2013-05-22 19:00:00", "2013-05-22 20:00:00"),))
        request = create_request(request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=[target_telescope]) 
        normal_single_ur = create_user_request(tracking_number, priority, [request], 'single')
        
        # Build mock reservation list
        prepare_for_kernel_mock.side_effect = self.prepare_for_kernel_side_effect_factory(self.normalize_windows_to)
        
        # Create available intervals mock
        available_start = datetime.strptime("2013-05-22 19:30:00", '%Y-%m-%d %H:%M:%S')
        available_end = datetime.strptime("2013-05-22 19:40:00", '%Y-%m-%d %H:%M:%S')
        available_intervals = {
                               target_telescope : self.build_intervals([(available_start, available_end),], self.normalize_windows_to)
                               } 
        prepare_available_windows_for_kernel_mock.return_value = available_intervals
        
        # Create unmocked Scheduler parameters
        scheduler_run_end = datetime.strptime("2013-05-22 00:00:00", '%Y-%m-%d %H:%M:%S')
        user_request_dict = {
                             'type' : Request.TARGET_OF_OPPORTUNITY,
                             Request.NORMAL_OBSERVATION_TYPE : [],
                             Request.TARGET_OF_OPPORTUNITY : [normal_single_ur],
                             }
        
        # Start scheduler run
        self.network_snapshot_mock.user_request_for_telescope = Mock(return_value=None)
        scheduler = Scheduler(FullScheduler_v6, self.sched_params, self.event_bus_mock)
        scheduler_result = scheduler.run_scheduler(user_request_dict, self.network_snapshot_mock, self.network_model, scheduler_run_end)
        
        # Start assertions
        assert_true(self.is_scheduled(1, scheduler_result.schedule))
        assert_equal(1, self.number_of_times_scheduled(request_number, scheduler_result.schedule))
        assert_true(self.is_schedule_on_resource(request_number, scheduler_result.schedule, target_telescope))
        assert_true(self.doesnt_start_before(request_number, scheduler_result.schedule, available_start, self.normalize_windows_to))
        assert_true(self.doesnt_start_after(request_number, scheduler_result.schedule, available_end - timedelta(seconds=request_duration_seconds), self.normalize_windows_to))
        assert_true(self.scheduled_duration_is(request_number, scheduler_result.schedule, self.sched_params.slicesize_seconds, request_duration_seconds))
        
        assert_equal(['1m0a.doma.elp'], scheduler_result.resource_schedules_to_cancel)
        assert_equal([], scheduler_result.unschedulable_user_request_numbers)
        assert_equal([], scheduler_result.unschedulable_request_numbers)
        
        
    @patch.object(Scheduler, 'prepare_available_windows_for_kernel')
    @patch.object(Scheduler, 'prepare_for_kernel')    
    def test_run_scheduler_too_mode_with_schedulable_too_single_ur_with_avoidable_confilct(self, prepare_for_kernel_mock, prepare_available_windows_for_kernel_mock):
        '''Should schedule a single too request on open telescope when available
        '''
        # Build mock user requests
        request_duration_seconds = 60
        priority = 10
        too_tracking_number = 1
        too_request_number = 1
        normal_tracking_number = 2
        normal_request_number = 2
        request_windows = create_user_request_windows((("2013-05-22 19:00:00", "2013-05-22 20:00:00"),))
        too_request = create_request(too_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc']) 
        too_single_ur = create_user_request(too_tracking_number, priority, [too_request], 'single')
        normal_request = create_request(normal_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc']) 
        normal_single_ur = create_user_request(normal_tracking_number, priority, [normal_request], 'single')
        
        # Build mock reservation list
        prepare_for_kernel_mock.side_effect = self.prepare_for_kernel_side_effect_factory(self.normalize_windows_to)
        
        # Create available intervals mock
        available_start = datetime.strptime("2013-05-22 19:30:00", '%Y-%m-%d %H:%M:%S')
        available_end = datetime.strptime("2013-05-22 19:40:00", '%Y-%m-%d %H:%M:%S')
        available_intervals = {
                               '1m0a.doma.elp' : self.build_intervals([(available_start, available_end),], self.normalize_windows_to),
                               '1m0a.doma.lsc' : self.build_intervals([(available_start, available_end),], self.normalize_windows_to),
                               } 
        prepare_available_windows_for_kernel_mock.return_value = available_intervals
        
        # Create unmocked Scheduler parameters
        scheduler_run_end = datetime.strptime("2013-05-22 00:00:00", '%Y-%m-%d %H:%M:%S')
        user_request_dict = {
                             'type' : Request.TARGET_OF_OPPORTUNITY,
                             Request.NORMAL_OBSERVATION_TYPE : [normal_single_ur],
                             Request.TARGET_OF_OPPORTUNITY : [too_single_ur],
                             }
        
        # Make the normal user request appear to be running
        running_request = RunningRequest('1m0a.doma.lsc', normal_request_number)
        running_user_request = RunningUserRequest(normal_tracking_number, running_request)
        self.network_snapshot_mock.user_request_for_telescope = Mock(side_effect=(lambda tel : running_user_request if tel == '1m0a.doma.lsc' else None))
        
        # Start scheduler run
        scheduler = Scheduler(FullScheduler_v6, self.sched_params, self.event_bus_mock)
        scheduler_result = scheduler.run_scheduler(user_request_dict, self.network_snapshot_mock, self.network_model, scheduler_run_end)
        
        # Start assertions
        # This checks to see that windows are only created for the correct telescope resources
        assert_equal(1, prepare_for_kernel_mock.call_count)
        assert_equal(['1m0a.doma.elp'], prepare_for_kernel_mock.call_args[0][1])
        assert_equal(1, prepare_available_windows_for_kernel_mock.call_count)
        assert_equal(['1m0a.doma.elp'], prepare_available_windows_for_kernel_mock.call_args[0][0])
        
        assert_true(self.is_scheduled(1, scheduler_result.schedule))
        assert_equal(1, self.number_of_times_scheduled(too_request_number, scheduler_result.schedule))
        assert_true(self.is_schedule_on_resource(too_request_number, scheduler_result.schedule, '1m0a.doma.elp'))
        assert_true(self.doesnt_start_before(too_request_number, scheduler_result.schedule, available_start, self.normalize_windows_to))
        assert_true(self.doesnt_start_after(too_request_number, scheduler_result.schedule, available_end - timedelta(seconds=request_duration_seconds), self.normalize_windows_to))
        assert_true(self.scheduled_duration_is(too_request_number, scheduler_result.schedule, self.sched_params.slicesize_seconds, request_duration_seconds))
        
        assert_equal(['1m0a.doma.elp'], scheduler_result.resource_schedules_to_cancel)
        assert_equal([], scheduler_result.unschedulable_user_request_numbers)
        assert_equal([], scheduler_result.unschedulable_request_numbers)
        
        
    @patch.object(Scheduler, 'prepare_available_windows_for_kernel')
    @patch.object(Scheduler, 'prepare_for_kernel')    
    def test_run_scheduler_too_mode_with_schedulable_too_single_ur_with_unavoidable_confilct(self, prepare_for_kernel_mock, prepare_available_windows_for_kernel_mock):
        '''Should cancel lowest priority normal running user request and schedule ToO
        '''
        # Build mock user requests
        request_duration_seconds = 60
        low_priority = 10
        high_priority = 20
        too_tracking_number = 1
        too_request_number = 1
        low_prioirty_normal_tracking_number = 2
        low_priority_normal_request_number = 2
        high_priority_normal_tracking_number = 3
        high_prioirty_normal_request_number = 3
        request_windows = create_user_request_windows((("2013-05-22 19:00:00", "2013-05-22 20:00:00"),))
        too_request = create_request(too_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc']) 
        too_single_ur = create_user_request(too_tracking_number, low_priority, [too_request], 'single')
        low_priority_normal_request = create_request(low_priority_normal_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc']) 
        low_priority_normal_single_ur = create_user_request(low_prioirty_normal_tracking_number, low_priority, [low_priority_normal_request], 'single')
        high_priority_normal_request = create_request(high_prioirty_normal_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc']) 
        high_priority_normal_single_ur = create_user_request(high_priority_normal_tracking_number, high_priority, [high_priority_normal_request], 'single')
          
        # Build mock reservation list
        prepare_for_kernel_mock.side_effect = self.prepare_for_kernel_side_effect_factory(self.normalize_windows_to)
          
        # Create available intervals mock
        available_start = datetime.strptime("2013-05-22 19:30:00", '%Y-%m-%d %H:%M:%S')
        available_end = datetime.strptime("2013-05-22 19:40:00", '%Y-%m-%d %H:%M:%S')
        available_intervals = {
                               '1m0a.doma.elp' : self.build_intervals([(available_start, available_end),], self.normalize_windows_to),
                               '1m0a.doma.lsc' : self.build_intervals([(available_start, available_end),], self.normalize_windows_to)
                              } 
        prepare_available_windows_for_kernel_mock.return_value = available_intervals
          
        # Create unmocked Scheduler parameters
        scheduler_run_end = datetime.strptime("2013-05-22 00:00:00", '%Y-%m-%d %H:%M:%S')
        user_request_dict = {
                             'type' : Request.TARGET_OF_OPPORTUNITY,
                             Request.NORMAL_OBSERVATION_TYPE : [low_priority_normal_single_ur, high_priority_normal_single_ur],
                             Request.TARGET_OF_OPPORTUNITY : [too_single_ur],
                             }
          
        # Make the normal user request appear to be running
        low_priority_running_request = RunningRequest('1m0a.doma.lsc', low_priority_normal_request_number)
        high_priority_running_request = RunningRequest('1m0a.doma.elp', high_prioirty_normal_request_number)
        low_priority_running_user_request = RunningUserRequest(low_prioirty_normal_tracking_number, low_priority_running_request)
        high_priority_running_user_request = RunningUserRequest(high_priority_normal_tracking_number, high_priority_running_request)
        running_user_requst_map = {
                                   '1m0a.doma.lsc' : low_priority_running_user_request,
                                   '1m0a.doma.elp' : high_priority_running_user_request,
                                   }
        self.network_snapshot_mock.user_request_for_telescope = Mock(side_effect=(lambda tel : running_user_requst_map.get(tel, None)))
          
        # Start scheduler run
        scheduler = Scheduler(FullScheduler_v6, self.sched_params, self.event_bus_mock)
        scheduler_result = scheduler.run_scheduler(user_request_dict, self.network_snapshot_mock, self.network_model, scheduler_run_end)
          
        # Start assertions
        # This checks to see that windows are only created for the correct telescope resources
        assert_equal(1, prepare_for_kernel_mock.call_count)
        assert_equal(['1m0a.doma.lsc'], prepare_for_kernel_mock.call_args[0][1])
        assert_equal(1, prepare_available_windows_for_kernel_mock.call_count)
        assert_equal(['1m0a.doma.lsc'], prepare_available_windows_for_kernel_mock.call_args[0][0])
          
        assert_true(self.is_scheduled(1, scheduler_result.schedule))
        assert_equal(1, self.number_of_times_scheduled(too_request_number, scheduler_result.schedule))
        assert_true(self.is_schedule_on_resource(too_request_number, scheduler_result.schedule, '1m0a.doma.lsc'))
        assert_true(self.doesnt_start_before(too_request_number, scheduler_result.schedule, available_start, self.normalize_windows_to))
        assert_true(self.doesnt_start_after(too_request_number, scheduler_result.schedule, available_end - timedelta(seconds=request_duration_seconds), self.normalize_windows_to))
        assert_true(self.scheduled_duration_is(too_request_number, scheduler_result.schedule, self.sched_params.slicesize_seconds, request_duration_seconds))
          
        assert_equal(['1m0a.doma.lsc'], scheduler_result.resource_schedules_to_cancel)
        assert_equal([], scheduler_result.unschedulable_user_request_numbers)
        assert_equal([], scheduler_result.unschedulable_request_numbers)


    @patch.object(Scheduler, 'prepare_available_windows_for_kernel')
    @patch.object(Scheduler, 'prepare_for_kernel')    
    def test_run_scheduler_too_mode_with_schedulable_too_single_ur_with_unavoidable_too_confilct(self, prepare_for_kernel_mock, prepare_available_windows_for_kernel_mock):
        '''Should not be scheduled and should not cancel running ToOs
        '''
        # Build mock user requests
        request_duration_seconds = 60
        low_priority = 10
        high_priority = 20
        new_too_tracking_number = 1
        new_too_request_number = 1
        old_low_prioirty_too_tracking_number = 2
        old_low_priority_too_request_number = 2
        old_high_priority_too_tracking_number = 3
        old_high_priority_too_request_number = 3
        request_windows = create_user_request_windows((("2013-05-22 19:00:00", "2013-05-22 20:00:00"),))
        new_too_request = create_request(new_too_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc']) 
        new_too_single_ur = create_user_request(new_too_tracking_number, low_priority, [new_too_request], 'single')
        old_low_priority_too_request = create_request(old_low_priority_too_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc']) 
        old_low_priority_too_single_ur = create_user_request(old_low_prioirty_too_tracking_number, low_priority, [old_low_priority_too_request], 'single')
        old_high_priority_too_request = create_request(old_high_priority_too_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc']) 
        old_high_priority_too_single_ur = create_user_request(old_high_priority_too_tracking_number, high_priority, [old_high_priority_too_request], 'single')
          
        # Build mock reservation list
        prepare_for_kernel_mock.side_effect = self.prepare_for_kernel_side_effect_factory(self.normalize_windows_to)
          
        # Create available intervals mock
        prepare_available_windows_for_kernel_mock.return_value = {}
          
        # Create unmocked Scheduler parameters
        scheduler_run_end = datetime.strptime("2013-05-22 00:00:00", '%Y-%m-%d %H:%M:%S')
        user_request_dict = {
                             'type' : Request.TARGET_OF_OPPORTUNITY,
                             Request.NORMAL_OBSERVATION_TYPE : [],
                             Request.TARGET_OF_OPPORTUNITY : [new_too_single_ur, old_low_priority_too_single_ur, old_high_priority_too_single_ur],
                             }
          
        # Make the normal user request appear to be running
        low_priority_running_request = RunningRequest('1m0a.doma.lsc', old_low_priority_too_request_number)
        high_priority_running_request = RunningRequest('1m0a.doma.elp', old_high_priority_too_request_number)
        low_priority_running_user_request = RunningUserRequest(old_low_prioirty_too_tracking_number, low_priority_running_request)
        high_priority_running_user_request = RunningUserRequest(old_high_priority_too_tracking_number, high_priority_running_request)
        running_user_requst_map = {
                                   '1m0a.doma.lsc' : low_priority_running_user_request,
                                   '1m0a.doma.elp' : high_priority_running_user_request,
                                   }
        self.network_snapshot_mock.user_request_for_telescope = Mock(side_effect=(lambda tel : running_user_requst_map.get(tel, None)))
          
        # Start scheduler run
        scheduler = Scheduler(FullScheduler_v6, self.sched_params, self.event_bus_mock)
        scheduler_result = scheduler.run_scheduler(user_request_dict, self.network_snapshot_mock, self.network_model, scheduler_run_end)
          
        # Start assertions
        # This checks to see that windows are only created for the correct telescope resources
        assert_equal(1, prepare_for_kernel_mock.call_count)
        assert_equal([], prepare_for_kernel_mock.call_args[0][1])
        assert_equal(1, prepare_available_windows_for_kernel_mock.call_count)
        assert_equal([], prepare_available_windows_for_kernel_mock.call_args[0][0])
          
        assert_false(self.is_scheduled(1, scheduler_result.schedule))
        assert_false(self.is_scheduled(2, scheduler_result.schedule))
        assert_false(self.is_scheduled(3, scheduler_result.schedule))
           
        assert_equal([], scheduler_result.resource_schedules_to_cancel)
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
    
    
def create_user_request_windows(start_end_tuples):
    windows = []
    for start, end in start_end_tuples:
        window_dict = {
                         'start' : start,
                         'end'   : end,
                       }
    windows.append(window_dict)
    
    return windows
    
    
def create_user_request(tracking_number, priority, requests, operator):#window_dicts, operator='and', resource_name='Martin', target=None, molecules=None, proposal=create_mock_proposal(), expires=None, duration=60):
    
    mock_user_request = Mock(tracking_number=tracking_number, priority=priority, requests=requests, operator=operator)
    mock_user_request.n_requests = Mock(return_value=len(requests))
    mock_user_request.get_priority = Mock(return_value=priority)
    mock_user_request.drop_empty_children = Mock(side_effect=lambda *args : [])#[request.request_number for request in requests if len(request.windows) > 0])
    
    return mock_user_request

    
def create_request(request_number, duration, windows, possible_telescopes):
    model_windows = []
    for window in windows:
        for telescope in possible_telescopes:
            model_windows.append(Window(window, telescope))
    mock_request = Mock(request_number=request_number, duration=duration, windows=model_windows)
    mock_request.get_duration = Mock(return_value=duration)
    mock_request.n_windows = Mock(return_value=len(windows))
    
    return mock_request
    
    
class TestSchedulerRunner(object):
     
    def test_scheduler_runner_all_interfaces_mocked(self):
        request_duration_seconds = 60
        priority = 10
        too_tracking_number = 1
        too_request_number = 1
        normal_tracking_number = 2
        normal_request_number = 2
        request_windows = create_user_request_windows((("2013-05-22 19:00:00", "2013-05-22 20:00:00"),))
        too_request = create_request(too_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc']) 
        too_single_ur = create_user_request(too_tracking_number, priority, [too_request], 'single')
        too_single_ur.has_target_of_opportunity = Mock(return_value=True)
        normal_request = create_request(normal_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc']) 
        normal_single_ur = create_user_request(normal_tracking_number, priority, [normal_request], 'single')
        
        
        sched_params = SchedulerParameters(run_once=True)
        scheduler_mock = Mock()
        network_interface_mock = Mock()
        network_interface_mock.get_all_user_requests = Mock(return_value=[too_single_ur, normal_single_ur])
        network_model_mock = {}
        scheduler_runner = SchedulerRunner(sched_params, scheduler_mock, network_interface_mock, network_model_mock)
        scheduler_runner.run()

        assert_equal(1, network_interface_mock.get_all_user_requests.call_count)
        assert_equal(2, scheduler_mock.run_scheduler.call_count)
        assert_equal(2, network_interface_mock.cancel.call_count)
        assert_equal(2, network_interface_mock.save)
        
        

