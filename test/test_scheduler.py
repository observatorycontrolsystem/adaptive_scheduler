from adaptive_scheduler.scheduler import Scheduler, SchedulerRunner, SchedulerResult
from adaptive_scheduler.scheduler_input import  SchedulerParameters, SchedulingInputProvider, SchedulingInput, SchedulingInputUtils
from adaptive_scheduler.model2 import UserRequest, Window, Windows
from adaptive_scheduler.interfaces import RunningRequest, RunningUserRequest, ResourceUsageSnapshot
from adaptive_scheduler.kernel.reservation_v3 import Reservation_v3 as Reservation
from adaptive_scheduler.kernel.reservation_v3 import CompoundReservation_v2 as CompoundReservation
from adaptive_scheduler.kernel_mappings import normalise_dt_intervals
from adaptive_scheduler.kernel.fullscheduler_v6 import FullScheduler_v6
from time_intervals.intervals import Intervals
from adaptive_scheduler.utils import datetime_to_normalised_epoch
from nose.tools import nottest

from mock import Mock, patch
from nose.tools import assert_equal, assert_not_equal, assert_true, assert_false

from datetime import datetime, timedelta


class TestScheduler(object):

    def __init__(self):
        self.scheduler_run_date = "2013-05-22T00:00:00Z"
        self.normalize_windows_to = datetime.strptime(self.scheduler_run_date, '%Y-%m-%dT%H:%M:%SZ')

        self.sched_params = SchedulerParameters()
        self.sched_params.simulate_now = self.scheduler_run_date
        self.sched_params.timelimit_seconds = 5
        self.sched_params.slicesize_seconds = 300
        self.network_model = {'1m0a.doma.elp':{'status': 'online',
                                               'name': '1m0a.doma.elp',
                                               'events': []},
                              '1m0a.doma.lsc':{'status': 'online',
                                               'name': '1m0a.doma.lsc',
                                               'events': []}
                              }

        self.event_bus_mock = Mock()
        self.network_snapshot_mock = Mock()
        self.network_snapshot_mock.running_tracking_numbers = Mock(return_value=[])
        self.network_snapshot_mock.blocked_intervals = Mock(return_value=Intervals())
        self.fake_semester = {'id': '2015A',
                              'start': (datetime.utcnow() - timedelta(days=30)),
                              'end': (datetime.utcnow() + timedelta(days=30))}


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
                               ipp_value=1.0
                             )

            ur_list.append(ur)

        return ur_list



    def test_run_scheduler_with_mocked_interfaces(self):
        event_bus_mock = Mock()
        sched_params = SchedulerParameters()
        normal_user_requests = []

        network_snapshot_mock = Mock()
        network_snapshot_mock.running_tracking_numbers = Mock(return_value=[])

        network_snapshot_mock.blocked_intervals = Mock(return_value=Intervals())
        network_snapshot_mock.running_requests_for_resources = Mock(return_value=[])
        network_snapshot_mock.user_requests_for_resource = Mock(return_value=[])
        network_model = ['',]
        estimated_scheduler_runtime = timedelta(seconds=300)

        kernel_class_mock = Mock()
        scheduler = Scheduler(kernel_class_mock, sched_params, event_bus_mock)
        model_builder = Mock()
        model_builder.semester_details = {'start': datetime.utcnow() - timedelta(days=7)}
        scheduler_input = SchedulingInput(sched_params, datetime.utcnow(), estimated_scheduler_runtime,
                                          normal_user_requests, network_snapshot_mock,
                                          model_builder, self.network_model, False)
        estimated_scheduler_end = datetime.utcnow() + estimated_scheduler_runtime
        scheduler_result = scheduler.run_scheduler(scheduler_input, estimated_scheduler_end,
                                                   self.fake_semester, preemption_enabled=False)

        assert_equal({}, scheduler_result.schedule)
        assert_equal({}, scheduler_result.resource_schedules_to_cancel)

    def test_constructing_value_matrix(self):
        # tel2 is not used
        tels = ['1m0a.doma.tel1', '1m0a.doma.tel2']

        windows = create_user_request_windows((("2013-05-22T19:00:00Z", "2013-05-22T20:00:00Z"),))
        too_r1 = create_request(1, 60, windows, tels, is_too=True)
        too_ur1 = create_user_request(1, 20, [too_r1], 'single')
        too_r2 = create_request(2, 60, windows, tels, is_too=True)
        too_ur2 = create_user_request(2, 100, [too_r2], 'single')

        too_urs = [too_ur1, too_ur2]

        normal_r1 = create_request(30, 60, windows, tels, is_too=False)
        normal_ur1 = create_user_request(30, 10, [normal_r1], 'single')

        running_r1 = RunningRequest('1m0a.doma.tel1', normal_ur1.tracking_number, Mock(), Mock())
        running_ur1 = RunningUserRequest(normal_ur1.tracking_number, running_r1)
        running_user_requests = [running_ur1]
        extra_block_intervals = {}
        timestamp = datetime.utcnow()
        user_request_priorities = {}
        user_request_priorities[normal_ur1.tracking_number] = normal_ur1.get_priority()
        resource_usage_snapshot = ResourceUsageSnapshot(timestamp, running_user_requests, extra_block_intervals)

        mock_kernel_class = Mock()
        scheduler = Scheduler(mock_kernel_class, self.sched_params, self.event_bus_mock)
        matrix = scheduler.construct_value_function_dict(too_urs, tels, resource_usage_snapshot, user_request_priorities)

        expected = {
                    ('1m0a.doma.tel1', 1) : 2.0,
                    ('1m0a.doma.tel1', 2) : 10.0,
                    ('1m0a.doma.tel2', 1) : 20.0,
                    ('1m0a.doma.tel2', 2) : 100.0,
                    }

        assert_equal(matrix, expected)


    def test_combine_running_and_too_requests(self):
        start = datetime(2012, 1, 1, 0, 0, 0)
        end = datetime(2012, 1, 2, 0, 0, 0)
        running = {
                   '0m4a.aqwb.coj' : Intervals([{'time': start, 'type': 'start'}, {'time': end, 'type': 'end'}])
                  }
        too = {
               '0m4a.aqwb.coj' : Intervals([{'time': start + timedelta(seconds=10), 'type': 'start'}, {'time': end + timedelta(seconds=10), 'type': 'end'}])
               }

        mock_kernel_class = Mock()
        scheduler = Scheduler(mock_kernel_class, self.sched_params, self.event_bus_mock)
        combined = scheduler.combine_excluded_intervals(running, too)

        expected = {
                    '0m4a.aqwb.coj' : Intervals([{'time': start, 'type': 'start'}, {'time': end + timedelta(seconds=10), 'type': 'end'}])
                    }

        assert_equal(expected, combined)

    def test_optimal_schedule(self):
        tel_ur_value_dict = {
                                  ('tel1', 1) : 6,
                                  ('tel1', 2) : 7,
                                  ('tel2', 1) : 8,
                                  ('tel2', 2) : 10
                                  }

        tracking_numbers = [1, 2];
        telescopes = ['tel1', 'tel2']

        mock_kernel_class = Mock()
        scheduler = Scheduler(mock_kernel_class, self.sched_params, self.event_bus_mock)
        combinations = scheduler.compute_optimal_combination(tel_ur_value_dict, tracking_numbers, telescopes)

        expected_combinations = [('tel1', 1), ('tel2', 2)]

        assert_equal(combinations, expected_combinations)


    def test_optimal_schedule_one_of_two_urs_possible(self):
        tel_ur_value_dict = {
                                  ('tel1', 1) : 6,
                                  ('tel2', 1) : 8,
                                  }

        tracking_numbers = [1, 2];
        telescopes = ['tel1', 'tel2']

        mock_kernel_class = Mock()
        scheduler = Scheduler(mock_kernel_class, self.sched_params, self.event_bus_mock)
        combinations = scheduler.compute_optimal_combination(tel_ur_value_dict, tracking_numbers, telescopes)

        expected_combinations = [('tel1', 1)]

        assert_equal(combinations, expected_combinations)


    def test_optimal_schedule_zero_of_two_urs_possible(self):
        tel_ur_value_dict = {
                            }

        tracking_numbers = [1, 2];
        telescopes = ['tel1', 'tel2']

        mock_kernel_class = Mock()
        scheduler = Scheduler(mock_kernel_class, self.sched_params, self.event_bus_mock)
        combinations = scheduler.compute_optimal_combination(tel_ur_value_dict, tracking_numbers, telescopes)

        expected_combinations = []

        assert_equal(combinations, expected_combinations)


    def test_optimal_schedule_more_telescopes_than_urs(self):
        tel_ur_value_dict = {
                                  ('tel1', 1) : 6,
                                  ('tel1', 2) : 7,
                                  ('tel2', 1) : 8,
                                  ('tel2', 2) : 10,
                                  ('tel3', 1) : 12,
                                  ('tel3', 2) : 14
                                  }

        tracking_numbers = [1, 2];
        telescopes = ['tel1', 'tel2', 'tel3']

        mock_kernel_class = Mock()
        scheduler = Scheduler(mock_kernel_class, self.sched_params, self.event_bus_mock)
        combinations = scheduler.compute_optimal_combination(tel_ur_value_dict, tracking_numbers, telescopes)

        expected_combinations = [('tel2', 1), ('tel3', 2)]

        assert_equal(combinations, expected_combinations)


    def test_optimal_schedule_more_telescopes_than_urs_not_all_telescopes_possible(self):
        tel_ur_value_dict = {
                                  ('tel1', 1) : 6,
                                  ('tel1', 2) : 7,
                                  ('tel2', 1) : 8,
                                  ('tel2', 2) : 10,
                                  }

        tracking_numbers = [1, 2];
        telescopes = ['tel1', 'tel2', 'tel3']

        mock_kernel_class = Mock()
        scheduler = Scheduler(mock_kernel_class, self.sched_params, self.event_bus_mock)
        combinations = scheduler.compute_optimal_combination(tel_ur_value_dict, tracking_numbers, telescopes)

        expected_combinations = [('tel1', 1), ('tel2', 2)]

        assert_equal(combinations, expected_combinations)


    # TODO: Not sure if this case really needs to work.  If scheduler can only put in a single
    # ToO for a resource at a time, that seems OK, but make sure it handles the case where the
    # earlier ToO has a lower value than the non competing later ToO.  In that case, both should
    # get scheduled, but I suspect that the current implementation will only schedule the later
    # more valuable one.
    @nottest
    def test_optimal_schedule_two_urs_possible_only_on_same_telescope(self):
        tel_ur_value_dict = {
                                  ('tel1', 1) : 6,
                                  ('tel1', 2) : 8,
                                  }

        tracking_numbers = [1, 2];
        telescopes = ['tel1', 'tel2']

        mock_kernel_class = Mock()
        scheduler = Scheduler(mock_kernel_class, self.sched_params, self.event_bus_mock)
        combinations = scheduler.compute_optimal_combination(tel_ur_value_dict, tracking_numbers, telescopes)

        expected_combinations = [('tel1', 1), ('tel1', 2)]

        assert_equal(combinations, expected_combinations)


    def test_create_resource_mask_no_running_requests(self):
        mock_kernel_class = Mock()
        scheduler = Scheduler(mock_kernel_class, self.sched_params, self.event_bus_mock)
        
        available_resources = ['1m0a.doma.bpl']
        running_user_requests = []
        extra_block_intervals = {}
        timestamp = datetime.utcnow()
        resource_usage_snapshot = ResourceUsageSnapshot(timestamp, running_user_requests, extra_block_intervals)
        too_tracking_numbers = []
        
        resource_mask = scheduler.create_resource_mask(available_resources, resource_usage_snapshot, too_tracking_numbers, preemption_enabled=False)
        
        assert_equal(resource_mask['1m0a.doma.bpl'], Intervals([]))


    def test_create_resource_mask_running_too(self):
        mock_kernel_class = Mock()
        scheduler = Scheduler(mock_kernel_class, self.sched_params, self.event_bus_mock)
        
        available_resources = ['1m0a.doma.bpl']
        
        start = datetime(2014, 10, 4, 2, 43, 0)
        end = datetime(2014, 10, 4, 2, 53, 0)
        tracking_number = 1
        running_r1 = RunningRequest('1m0a.doma.bpl', tracking_number, start, end)
        running_ur1 = RunningUserRequest(tracking_number, running_r1)
        running_user_requests = [running_ur1]
        extra_block_intervals = {}
        timestamp = datetime.utcnow()
        resource_usage_snapshot = ResourceUsageSnapshot(timestamp, running_user_requests, extra_block_intervals)
        
        too_tracking_numbers = [tracking_number]
        
        resource_mask = scheduler.create_resource_mask(available_resources, resource_usage_snapshot, too_tracking_numbers, preemption_enabled=False)
        assert_equal(resource_mask['1m0a.doma.bpl'], self.build_intervals([(start, end)]))


    def test_create_resource_mask_running_too_failed(self):
        mock_kernel_class = Mock()
        scheduler = Scheduler(mock_kernel_class, self.sched_params, self.event_bus_mock)
        
        available_resources = ['1m0a.doma.bpl']
        
        start = datetime(2014, 10, 4, 2, 43, 0)
        end = datetime(2014, 10, 4, 2, 53, 0)
        tracking_number = 1
        running_r1 = RunningRequest('1m0a.doma.bpl', tracking_number, start, end)
        running_r1.add_error("An error")
        running_ur1 = RunningUserRequest(tracking_number, running_r1)
        running_user_requests = [running_ur1]
        extra_block_intervals = {}
        timestamp = datetime.utcnow()
        resource_usage_snapshot = ResourceUsageSnapshot(timestamp, running_user_requests, extra_block_intervals)
        
        too_tracking_numbers = [tracking_number]
        
        resource_mask = scheduler.create_resource_mask(available_resources, resource_usage_snapshot, too_tracking_numbers, preemption_enabled=False)
        assert_equal(resource_mask['1m0a.doma.bpl'], Intervals([]))


    def test_create_resource_mask_running_request_preemption_disabled(self):
        mock_kernel_class = Mock()
        scheduler = Scheduler(mock_kernel_class, self.sched_params, self.event_bus_mock)
        
        available_resources = ['1m0a.doma.bpl']
        
        start = datetime(2014, 10, 4, 2, 43, 0)
        end = datetime(2014, 10, 4, 2, 53, 0)
        tracking_number = 1
        running_r1 = RunningRequest('1m0a.doma.bpl', tracking_number, start, end)
        running_ur1 = RunningUserRequest(tracking_number, running_r1)
        running_user_requests = [running_ur1]
        extra_block_intervals = {}
        timestamp = datetime.utcnow()
        resource_usage_snapshot = ResourceUsageSnapshot(timestamp, running_user_requests, extra_block_intervals)
        
        too_tracking_numbers = []
        
        resource_mask = scheduler.create_resource_mask(available_resources, resource_usage_snapshot, too_tracking_numbers, preemption_enabled=False)
        assert_equal(resource_mask['1m0a.doma.bpl'], self.build_intervals([(start, end)]))
        
        
    def test_create_resource_mask_running_request_failed_preemption_disabled(self):
        mock_kernel_class = Mock()
        scheduler = Scheduler(mock_kernel_class, self.sched_params, self.event_bus_mock)
        
        available_resources = ['1m0a.doma.bpl']
        
        start = datetime(2014, 10, 4, 2, 43, 0)
        end = datetime(2014, 10, 4, 2, 53, 0)
        tracking_number = 1
        running_r1 = RunningRequest('1m0a.doma.bpl', tracking_number, start, end)
        running_r1.add_error("An error")
        running_ur1 = RunningUserRequest(tracking_number, running_r1)
        running_user_requests = [running_ur1]
        extra_block_intervals = {}
        timestamp = datetime.utcnow()
        resource_usage_snapshot = ResourceUsageSnapshot(timestamp, running_user_requests, extra_block_intervals)
        
        too_tracking_numbers = []
        
        resource_mask = scheduler.create_resource_mask(available_resources, resource_usage_snapshot, too_tracking_numbers, preemption_enabled=False)
        assert_equal(resource_mask['1m0a.doma.bpl'], Intervals([]))


    def test_create_resource_mask_running_request_preemption_enabled(self):
        mock_kernel_class = Mock()
        scheduler = Scheduler(mock_kernel_class, self.sched_params, self.event_bus_mock)
        
        available_resources = ['1m0a.doma.bpl']
        
        start = datetime(2014, 10, 4, 2, 43, 0)
        end = datetime(2014, 10, 4, 2, 53, 0)
        tracking_number = 1
        running_r1 = RunningRequest('1m0a.doma.bpl', tracking_number, start, end)
        running_ur1 = RunningUserRequest(tracking_number, running_r1)
        running_user_requests = [running_ur1]
        extra_block_intervals = {}
        timestamp = datetime.utcnow()
        resource_usage_snapshot = ResourceUsageSnapshot(timestamp, running_user_requests, extra_block_intervals)
        
        too_tracking_numbers = []
        
        resource_mask = scheduler.create_resource_mask(available_resources, resource_usage_snapshot, too_tracking_numbers, preemption_enabled=True)
        assert_equal(resource_mask['1m0a.doma.bpl'], Intervals([]))


    def prepare_for_kernel_side_effect_factory(self, normailze_to_date):

        def side_effect(user_requests, estimated_scheduler_end):
            return [self.build_compound_reservation(ur, normalize_windows_to=normailze_to_date) for ur in user_requests]

        return side_effect

    def prepare_available_windows_for_kernel_side_effect_factory(self, available_intervals):

        def side_effect(resources_to_schedule, resource_interval_mask, estimated_scheduler_end):
            return {r:i for r, i in available_intervals.items() if r in resources_to_schedule}

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
        request_windows = create_user_request_windows((("2013-05-22T19:00:00Z", "2013-05-22T20:00:00Z"),))
        request = create_request(request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=[target_telescope])
        normal_single_ur = create_user_request(tracking_number, priority, [request], 'single')

        # Build mock reservation list
        prepare_for_kernel_mock.side_effect = self.prepare_for_kernel_side_effect_factory(self.normalize_windows_to)

        # Create available intervals mock
        available_start = datetime.strptime("2013-05-22 19:30:00", '%Y-%m-%d %H:%M:%S')
        available_end = datetime.strptime("2013-05-22 19:40:00", '%Y-%m-%d %H:%M:%S')
        available_intervals = {
                               target_telescope : self.build_intervals([(available_start, available_end), ], self.normalize_windows_to)
                               }
        prepare_available_windows_for_kernel_mock.side_effect = self.prepare_available_windows_for_kernel_side_effect_factory(available_intervals)

        # Create unmocked Scheduler parameters
        scheduler_run_end = datetime.strptime("2013-05-22 00:00:00", '%Y-%m-%d %H:%M:%S')
        normal_user_requests = [normal_single_ur]
        resource_usage_snapshot = ResourceUsageSnapshot(datetime.utcnow(), [], {})

        # Start scheduler run
        mock_scheduler_input = Mock(user_requests=normal_user_requests,
                            too_user_requests=[],
                            normal_user_requests=normal_user_requests,
                            user_request_priorities={1 : 10},
                            available_resources=self.network_model,
                            estimated_scheduler_end=scheduler_run_end,
                            resource_usage_snapshot=resource_usage_snapshot,
                            is_too_input=True,
                            estimated_scheduler_runtime=timedelta(seconds=300)
                            )
        mock_scheduler_input.invalid_user_requests = []
        mock_scheduler_input.invalid_requests = []
        scheduler = Scheduler(FullScheduler_v6, self.sched_params, self.event_bus_mock)
        estimated_scheduler_end = datetime.utcnow() + timedelta(seconds=300)
        scheduler_result = scheduler.run_scheduler(mock_scheduler_input, estimated_scheduler_end,
                                                   self.fake_semester, preemption_enabled=False)

        # Start assertions
        assert_true(self.is_scheduled(1, scheduler_result.schedule))
        assert_equal(1, self.number_of_times_scheduled(request_number, scheduler_result.schedule))
        assert_true(self.is_schedule_on_resource(request_number, scheduler_result.schedule, target_telescope))
        assert_true(self.doesnt_start_before(request_number, scheduler_result.schedule, available_start, self.normalize_windows_to))
        assert_true(self.doesnt_start_after(request_number, scheduler_result.schedule, available_end - timedelta(seconds=request_duration_seconds), self.normalize_windows_to))
        assert_true(self.scheduled_duration_is(request_number, scheduler_result.schedule, self.sched_params.slicesize_seconds, request_duration_seconds))

        assert_equal(sorted(self.network_model), sorted(scheduler_result.resource_schedules_to_cancel))


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
        telescope1 = '1m0a.doma.elp'
        telescope2 = '1m0a.doma.lsc'
        request_windows = create_user_request_windows((("2013-05-22T19:00:00Z", "2013-05-22T20:00:00Z"),))
        request = create_request(request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=[telescope1, telescope2], is_too=True)
        too_single_ur = create_user_request(tracking_number, priority, [request], 'single')

        # Build mock reservation list
        prepare_for_kernel_mock.side_effect = self.prepare_for_kernel_side_effect_factory(self.normalize_windows_to)

        # Create available intervals mock
        available_start = datetime.strptime("2013-05-22 19:30:00", '%Y-%m-%d %H:%M:%S')
        available_end = datetime.strptime("2013-05-22 19:40:00", '%Y-%m-%d %H:%M:%S')
        available_intervals = {
                               telescope1 : self.build_intervals([(available_start, available_end), ], self.normalize_windows_to),
                               telescope2 : self.build_intervals([(available_start, available_end), ], self.normalize_windows_to),
                               }
        prepare_available_windows_for_kernel_mock.side_effect = self.prepare_available_windows_for_kernel_side_effect_factory(available_intervals)

        # Create unmocked Scheduler parameters
        scheduler_run_end = datetime.strptime("2013-05-22 00:00:00", '%Y-%m-%d %H:%M:%S')
        too_user_requests = [too_single_ur]

        # Start scheduler run
        resource_usage_snapshot = ResourceUsageSnapshot(datetime.utcnow(), [], {})

        mock_scheduler_input = Mock(user_requests=too_user_requests,
                            too_user_requests=too_user_requests,
                            normal_user_requests=[],
                            user_request_priorities={1 : 10},
                            available_resources=['1m0a.doma.elp', '1m0a.doma.lsc'],
                            estimated_scheduler_end=scheduler_run_end,
                            resource_usage_snapshot=resource_usage_snapshot,
                            is_too_input=True,
                            estimated_scheduler_runtime=timedelta(seconds=300))
        mock_scheduler_input.invalid_user_requests = []
        mock_scheduler_input.invalid_requests = []
        scheduler = Scheduler(FullScheduler_v6, self.sched_params, self.event_bus_mock)
        estimated_scheduler_end = datetime.utcnow() + timedelta(seconds=300)
        scheduler_result = scheduler.run_scheduler(mock_scheduler_input, estimated_scheduler_end,
                                                   self.fake_semester, preemption_enabled=True)

        # Start assertions
        assert_true(self.is_scheduled(1, scheduler_result.schedule))
        assert_equal(1, self.number_of_times_scheduled(request_number, scheduler_result.schedule))
        assert_true(self.doesnt_start_before(request_number, scheduler_result.schedule, available_start, self.normalize_windows_to))
        assert_true(self.doesnt_start_after(request_number, scheduler_result.schedule, available_end - timedelta(seconds=request_duration_seconds), self.normalize_windows_to))
        assert_true(self.scheduled_duration_is(request_number, scheduler_result.schedule, self.sched_params.slicesize_seconds, request_duration_seconds))

        assert_equal(1, len(scheduler_result.resource_schedules_to_cancel))
        assert_true(scheduler_result.resource_schedules_to_cancel[0] in ['1m0a.doma.lsc', '1m0a.doma.elp'])


    @patch.object(Scheduler, 'prepare_available_windows_for_kernel')
    @patch.object(Scheduler, 'prepare_for_kernel')
    def test_run_scheduler_too_mode_with_schedulable_too_single_ur_with_avoidable_conflict(self, prepare_for_kernel_mock, prepare_available_windows_for_kernel_mock):
        '''Should schedule a single too request on open telescope when available
        '''
        # Build mock user requests
        request_duration_seconds = 60
        priority = 10
        too_tracking_number = 1
        too_request_number = 1
        normal_tracking_number = 2
        normal_request_number = 2
        request_windows = create_user_request_windows((("2013-05-22T19:00:00Z", "2013-05-22T20:00:00Z"),))
        too_request = create_request(too_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc'], is_too=True)
        too_single_ur = create_user_request(too_tracking_number, priority, [too_request], 'single')
        normal_request = create_request(normal_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc'])
        normal_single_ur = create_user_request(normal_tracking_number, priority, [normal_request], 'single')

        # Build mock reservation list
        prepare_for_kernel_mock.side_effect = self.prepare_for_kernel_side_effect_factory(self.normalize_windows_to)

        # Create available intervals mock
        available_start = datetime.strptime("2013-05-22 19:30:00", '%Y-%m-%d %H:%M:%S')
        available_end = datetime.strptime("2013-05-22 19:40:00", '%Y-%m-%d %H:%M:%S')
        available_intervals = {
                               '1m0a.doma.elp' : self.build_intervals([(available_start, available_end), ], self.normalize_windows_to),
                               }
        prepare_available_windows_for_kernel_mock.side_effect = self.prepare_available_windows_for_kernel_side_effect_factory(available_intervals)

        # Create unmocked Scheduler parameters
        scheduler_run_end = datetime.strptime("2013-05-22 00:00:00", '%Y-%m-%d %H:%M:%S')
        too_user_requests = [too_single_ur]
        normal_user_requests = [normal_single_ur]
        normal_user_requests_priority_by_tracking_number = {ur.tracking_number : ur.priority for ur in normal_user_requests}

        # Make the normal user request appear to be running
        running_request = RunningRequest('1m0a.doma.lsc', normal_request_number, Mock(), Mock())
        running_user_request = RunningUserRequest(normal_tracking_number, running_request)
        resource_usage_snapshot = ResourceUsageSnapshot(datetime.utcnow(),
                                                        [running_user_request],
                                                        {})
        model_builder = Mock()
        model_builder.semester_details = {'start': datetime.utcnow() - timedelta(days=7)}
        # Setup input
        scheduler_input = SchedulingInput(self.sched_params, datetime.utcnow(),
                            timedelta(seconds=300),
                            [], 
                            resource_usage_snapshot,
                            model_builder,
                            self.network_model,
                            is_too_input=True,
                            )
        scheduler_input._scheduler_model_too_user_requests = too_user_requests
        scheduler_input._scheduler_model_normal_user_requests = normal_user_requests
        
        # Start scheduler run
        scheduler = Scheduler(FullScheduler_v6, self.sched_params, self.event_bus_mock)
        estimated_scheduler_end = datetime.utcnow() + timedelta(seconds=300)
        scheduler_result = scheduler.run_scheduler(scheduler_input, estimated_scheduler_end,
                                                   self.fake_semester, preemption_enabled=True)

        # Start assertions
        # This checks to see that windows are only created for the correct telescope resources
        assert_equal(1, prepare_for_kernel_mock.call_count)
        assert_equal(1, prepare_available_windows_for_kernel_mock.call_count)

        assert_true(self.is_scheduled(1, scheduler_result.schedule))
        assert_equal(1, self.number_of_times_scheduled(too_request_number, scheduler_result.schedule))
        assert_true(self.is_schedule_on_resource(too_request_number, scheduler_result.schedule, '1m0a.doma.elp'))
        assert_true(self.doesnt_start_before(too_request_number, scheduler_result.schedule, available_start, self.normalize_windows_to))
        assert_true(self.doesnt_start_after(too_request_number, scheduler_result.schedule, available_end - timedelta(seconds=request_duration_seconds), self.normalize_windows_to))
        assert_true(self.scheduled_duration_is(too_request_number, scheduler_result.schedule, self.sched_params.slicesize_seconds, request_duration_seconds))

        assert_equal(['1m0a.doma.elp'], scheduler_result.resource_schedules_to_cancel)


    @patch.object(Scheduler, 'prepare_available_windows_for_kernel')
    @patch.object(Scheduler, 'prepare_for_kernel')
    def test_run_scheduler_too_mode_with_schedulable_too_single_ur_with_unavoidable_conflict(self, prepare_for_kernel_mock, prepare_available_windows_for_kernel_mock):
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
        request_windows = create_user_request_windows((("2013-05-22T19:00:00Z", "2013-05-22T20:00:00Z"),))
        too_request = create_request(too_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc'], is_too=True)
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
                               '1m0a.doma.lsc' : self.build_intervals([(available_start, available_end), ], self.normalize_windows_to)
                              }
        prepare_available_windows_for_kernel_mock.side_effect = self.prepare_available_windows_for_kernel_side_effect_factory(available_intervals)

        # Create unmocked Scheduler parameters
        scheduler_run_end = datetime.strptime("2013-05-22 00:00:00", '%Y-%m-%d %H:%M:%S')
        too_user_requests = [too_single_ur]
        normal_user_requests = [low_priority_normal_single_ur, high_priority_normal_single_ur]
        normal_user_requests_priority_by_tracking_number = {ur.tracking_number : ur.priority for ur in normal_user_requests}

        # Make the normal user request appear to be running
        low_priority_running_request = RunningRequest('1m0a.doma.lsc', low_priority_normal_request_number, Mock(), Mock())
        high_priority_running_request = RunningRequest('1m0a.doma.elp', high_prioirty_normal_request_number, Mock(), Mock())
        low_priority_running_user_request = RunningUserRequest(low_prioirty_normal_tracking_number, low_priority_running_request)
        high_priority_running_user_request = RunningUserRequest(high_priority_normal_tracking_number, high_priority_running_request)
        resource_usage_snapshot = ResourceUsageSnapshot(datetime.utcnow(),
                                                        [low_priority_running_user_request, high_priority_running_user_request],
                                                        {})

        model_builder = Mock()
        model_builder.semester_details = {'start': datetime.utcnow() - timedelta(days=7)}
        # Setup input
        scheduler_input = SchedulingInput(self.sched_params, datetime.utcnow(),
                            timedelta(seconds=300),
                            [], 
                            resource_usage_snapshot,
                            model_builder,
                            self.network_model,
                            is_too_input=True,
                            )
        scheduler_input._scheduler_model_too_user_requests = too_user_requests
        scheduler_input._scheduler_model_normal_user_requests = normal_user_requests
        
        # Start scheduler run
        scheduler = Scheduler(FullScheduler_v6, self.sched_params, self.event_bus_mock)
        estimated_scheduler_end = datetime.utcnow() + timedelta(seconds=300)
        scheduler_result = scheduler.run_scheduler(scheduler_input, estimated_scheduler_end,
                                                   self.fake_semester, preemption_enabled=True)

        # Start assertions
        # This checks to see that windows are only created for the correct telescope resources
        assert_equal(1, prepare_for_kernel_mock.call_count)
        assert_equal(1, prepare_available_windows_for_kernel_mock.call_count)

        assert_true(self.is_scheduled(1, scheduler_result.schedule))
        assert_equal(1, self.number_of_times_scheduled(too_request_number, scheduler_result.schedule))
        assert_true(self.is_schedule_on_resource(too_request_number, scheduler_result.schedule, '1m0a.doma.lsc'))
        assert_true(self.doesnt_start_before(too_request_number, scheduler_result.schedule, available_start, self.normalize_windows_to))
        assert_true(self.doesnt_start_after(too_request_number, scheduler_result.schedule, available_end - timedelta(seconds=request_duration_seconds), self.normalize_windows_to))
        assert_true(self.scheduled_duration_is(too_request_number, scheduler_result.schedule, self.sched_params.slicesize_seconds, request_duration_seconds))

        assert_equal(['1m0a.doma.lsc'], scheduler_result.resource_schedules_to_cancel)


    @patch.object(Scheduler, 'prepare_available_windows_for_kernel')
    @patch.object(Scheduler, 'prepare_for_kernel')
    def test_run_scheduler_too_mode_with_schedulable_too_single_ur_with_unavoidable_too_conflict(self, prepare_for_kernel_mock, prepare_available_windows_for_kernel_mock):
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
        request_windows = create_user_request_windows((("2013-05-22T19:00:00Z", "2013-05-22T20:00:00Z"),))
        new_too_request = create_request(new_too_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc'], is_too=True)
        new_too_single_ur = create_user_request(new_too_tracking_number, low_priority, [new_too_request], 'single')
        old_low_priority_too_request = create_request(old_low_priority_too_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc'], is_too=True)
        old_low_priority_too_single_ur = create_user_request(old_low_prioirty_too_tracking_number, low_priority, [old_low_priority_too_request], 'single')
        old_high_priority_too_request = create_request(old_high_priority_too_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc'], is_too=True)
        old_high_priority_too_single_ur = create_user_request(old_high_priority_too_tracking_number, high_priority, [old_high_priority_too_request], 'single')

        # Build mock reservation list
        prepare_for_kernel_mock.side_effect = self.prepare_for_kernel_side_effect_factory(self.normalize_windows_to)

        # Create available intervals mock
        available_intervals = {}
        prepare_available_windows_for_kernel_mock.side_effect = self.prepare_available_windows_for_kernel_side_effect_factory(available_intervals)

        # Create unmocked Scheduler parameters
        scheduler_run_end = datetime.strptime("2013-05-22 00:00:00", '%Y-%m-%d %H:%M:%S')
        too_user_requests = [new_too_single_ur, old_low_priority_too_single_ur, old_high_priority_too_single_ur]

        # Make the normal user request appear to be running
        low_priority_running_request = RunningRequest('1m0a.doma.lsc', old_low_priority_too_request_number, Mock(), Mock())
        high_priority_running_request = RunningRequest('1m0a.doma.elp', old_high_priority_too_request_number, Mock(), Mock())
        low_priority_running_user_request = RunningUserRequest(old_low_prioirty_too_tracking_number, low_priority_running_request)
        high_priority_running_user_request = RunningUserRequest(old_high_priority_too_tracking_number, high_priority_running_request)
        resource_usage_snapshot = ResourceUsageSnapshot(datetime.utcnow(),
                                                        [low_priority_running_user_request, high_priority_running_user_request], {})

        model_builder = Mock()
        model_builder.semester_details = {'start': datetime.utcnow() - timedelta(days=7)}
        # Setup input
        scheduler_input = SchedulingInput(self.sched_params, datetime.utcnow(),
                            timedelta(seconds=300),
                            [], 
                            resource_usage_snapshot,
                            model_builder,
                            self.network_model,
                            is_too_input=True,
                            )
        scheduler_input._scheduler_model_too_user_requests = too_user_requests
        scheduler_input._scheduler_model_normal_user_requests = []
        
        # Start scheduler run
        scheduler = Scheduler(FullScheduler_v6, self.sched_params, self.event_bus_mock)
        estimated_scheduler_end = datetime.utcnow() + timedelta(seconds=300)
        scheduler_result = scheduler.run_scheduler(scheduler_input, estimated_scheduler_end,
                                                   self.fake_semester, preemption_enabled=True)

        # Start assertions
        # This checks to see that windows are only created for the correct telescope resources
        assert_equal(1, prepare_for_kernel_mock.call_count)
        assert_equal(1, prepare_available_windows_for_kernel_mock.call_count)

        assert_false(self.is_scheduled(1, scheduler_result.schedule))
        assert_false(self.is_scheduled(2, scheduler_result.schedule))
        assert_false(self.is_scheduled(3, scheduler_result.schedule))

        assert_equal([], scheduler_result.resource_schedules_to_cancel)

    @patch.object(Scheduler, 'apply_window_filters')
    @patch.object(Scheduler, 'prepare_available_windows_for_kernel')
    @patch.object(Scheduler, 'prepare_for_kernel')
    def test_run_scheduler_too_mode_with_schedulable_not_visible_too_single_ur(self, prepare_for_kernel_mock, prepare_available_windows_for_kernel_mock, apply_window_filters_mock):
        '''Should result in empty too schedule result when ToO not visible
        '''
        # Build mock user requests
        request_duration_seconds = 60
        priority = 10
        tracking_number = 1
        request_number = 1
        target_telescope = '1m0a.doma.elp'
        request_windows = create_user_request_windows([])
        request = create_request(request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=[target_telescope], is_too=True)
        too_single_ur = create_user_request(tracking_number, priority, [request], 'single')

        # Build mock reservation list
        prepare_for_kernel_mock.side_effect = self.prepare_for_kernel_side_effect_factory(self.normalize_windows_to)

        # UR is not visible so don't let it come out of window filters
        apply_window_filters_mock.return_value = []

        # Create unmocked Scheduler parameters
        scheduler_run_end = datetime.strptime("2013-05-22 00:00:00", '%Y-%m-%d %H:%M:%S')
        too_user_requests = [too_single_ur]
        resource_usage_snapshot = ResourceUsageSnapshot(datetime.utcnow(), [], {})

        # Start scheduler run
        mock_scheduler_input = Mock(user_requests=too_user_requests,
                            too_user_requests=too_user_requests,
                            normal_user_requests=[],
                            user_request_priorities={},
                            available_resources=self.network_model,
                            estimated_scheduler_end=scheduler_run_end,
                            resource_usage_snapshot=resource_usage_snapshot,
                            is_too_input=True,
                            estimated_scheduler_runtime=timedelta(seconds=300))
        mock_scheduler_input.invalid_user_requests = []
        mock_scheduler_input.invalid_requests = []
        scheduler = Scheduler(FullScheduler_v6, self.sched_params, self.event_bus_mock)
        estimated_scheduler_end = datetime.utcnow() + timedelta(seconds=300)
        scheduler_result = scheduler.run_scheduler(mock_scheduler_input, estimated_scheduler_end,
                                                   self.fake_semester, preemption_enabled=True)

        # Start assertions
        assert_equal({}, scheduler_result.schedule)
        assert_false(self.is_scheduled(request_number, scheduler_result.schedule))
        assert_equal([], sorted(scheduler_result.resource_schedules_to_cancel))

        # Make sure the prepare_for_kernel_mock is called once with empty UR list
        assert_equal(1, prepare_for_kernel_mock.call_count)
        assert_equal([], prepare_for_kernel_mock.call_args[0][0])

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


    def build_compound_reservation(self, ur, normalize_windows_to):
        reservation_list = []
        intervals_by_resource = {}
        for request in ur.requests:
            for resource, windows in request.windows:
                start_end_tuples = []
                for window in windows:
#                     start = datetime.strptime(window.start, '%Y-%m-%d %H:%M:%S')
#                     end = datetime.strptime(window.end, '%Y-%m-%d %H:%M:%S')
                    start_end_tuples.append((window.start, window.end))
                    intervals = self.build_intervals(start_end_tuples, normalize_windows_to)
                    intervals_by_resource[resource] = intervals

#             window_dict = {}
#             for resource in resources:
#                 window_dict[resource] = intervals

            res = Reservation(ur.priority, request.duration, intervals_by_resource)
            res.request = request
            reservation_list.append(res)

        compound_reservation_mock = CompoundReservation(reservation_list, 'single')

        return compound_reservation_mock


    def build_intervals(self, start_end_tuples, normalize_to=None):
        timepoints = []
        for start, end in start_end_tuples:
            start_timepoint = {'time': start, 'type': 'start'}
            end_timepoint = {'time': end, 'type': 'end'}
            timepoints.append(start_timepoint)
            timepoints.append(end_timepoint)

        intervals = Intervals(timepoints)
        if normalize_to:
            intervals = normalise_dt_intervals(intervals, normalize_to)
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


def create_user_request(tracking_number, priority, requests, operator):  # window_dicts, operator='and', resource_name='Martin', target=None, molecules=None, proposal=create_mock_proposal(), expires=None, duration=60):

    mock_user_request = Mock(tracking_number=tracking_number, priority=priority, requests=requests, operator=operator)
    mock_user_request.n_requests = Mock(return_value=len(requests))
    mock_user_request.get_priority = Mock(return_value=priority)
    mock_user_request.drop_empty_children = Mock(side_effect=lambda *args : [])  # [request.request_number for request in requests if len(request.windows) > 0])
    mock_user_request.is_target_of_opportunity = Mock(return_value=reduce(lambda x, y: x and y, map(lambda r : r.observation_type == 'TARGET_OF_OPPORTUNITY', requests)))

    return mock_user_request


def create_request(request_number, duration, windows, possible_telescopes, is_too=False):
    model_windows = Windows()
    for window in windows:
        for telescope in possible_telescopes:
            mock_resource = telescope
            model_windows.append(Window(window, mock_resource))
    observation_type = "NORMAL"
    if is_too:
        observation_type = "TARGET_OF_OPPORTUNITY"
    mock_request = Mock(request_number=request_number, duration=duration, windows=model_windows, observation_type=observation_type)
    mock_request.get_duration = Mock(return_value=duration)
    mock_request.n_windows = Mock(return_value=len(windows))


    return mock_request


def create_scheduler_input(user_requests, block_schedule_by_resource, running_user_requests, too_tracking_numbers):
    input_mock = Mock()
    input_mock.scheduler_now = datetime.utcnow()
    input_mock.estimated_scheduler_end = datetime.utcnow()
    input_mock.user_requests = user_requests
    input_mock.resource_usage_snapshot = ResourceUsageSnapshot(datetime.utcnow(), running_user_requests, {})
    input_mock.available_resources = ['1m0a.doma.ogg',]
    input_mock.invalid_requests = []
    input_mock.invalid_user_requests = []
    input_mock.get_scheduling_start = Mock(return_value=datetime.utcnow())
    input_mock.get_block_schedule_by_resource = Mock(return_value=block_schedule_by_resource)
    input_mock.too_tracking_numbers = too_tracking_numbers

    return input_mock


def create_scheduler_input_factory(too_user_requests, normal_user_requests, block_schedule_by_resource={},
                                   running_user_requests=[], too_tracking_numbers=[]):
    too_input_mock = create_scheduler_input(too_user_requests, {}, running_user_requests, too_tracking_numbers)
    normal_input_mock = create_scheduler_input(normal_user_requests, block_schedule_by_resource, running_user_requests,
                                               too_tracking_numbers)
    mock_input_factory = Mock()
    mock_input_factory.create_too_scheduling_input = Mock(return_value=too_input_mock)
    mock_input_factory.create_normal_scheduling_input = Mock(return_value=normal_input_mock)

    return mock_input_factory


def create_running_user_request(tracking_number, request_number, resource, start, end):
    running_request = (RunningRequest(resource, request_number, start, end))
    running_ur = RunningUserRequest(tracking_number)
    running_ur.add_running_request(running_request)
    return running_ur


class TestSchedulerRunner(object):

    def setup(self):
        self.mock_kernel_class = Mock()
        self.sched_params = SchedulerParameters(run_once=True)
        self.mock_event_bus = Mock()
        self.scheduler_mock = Scheduler(self.mock_kernel_class, self.sched_params, self.mock_event_bus)

        self.network_interface_mock = Mock()
        self.network_interface_mock.cancel = Mock(return_value=0)
        self.network_interface_mock.save = Mock(return_value=0)
        self.network_interface_mock.abort = Mock(return_value=0)

        self.configdb_interface_mock = Mock()
        self.configdb_interface_mock.get_telescope_info = {'1m0a.doma.lsc': {'name': '1m0a.doma.lsc',
                                                                             'events': [],
                                                                             'status': 'online'},
                                                           '1m0a.doma.coj': {'name': '1m0a.doma.coj',
                                                                             'events': [],
                                                                             'status': 'online'},
                                                           '1m0a.doma.elp': {'name': '1m0a.doma.elp',
                                                                             'events': [],
                                                                             'status': 'online'},
                                                           }
        self.network_interface_mock.configdb_interface = self.configdb_interface_mock

        self.valhalla_interface_mock = Mock()
        self.valhalla_interface_mock.get_semester_details = Mock(return_value={'id': '2015A',
                                                                               'start': datetime.utcnow() - timedelta(days=30),
                                                                               'end': datetime.utcnow() + timedelta(days=30)})
        self.network_interface_mock.valhalla_interface = self.valhalla_interface_mock

        self.network_model = {}
        self.mock_input_factory = Mock()
        self.scheduler_runner = SchedulerRunner(self.sched_params, self.scheduler_mock, self.network_interface_mock, self.network_model, self.mock_input_factory)
        self.scheduler_runner.semester_details = self.valhalla_interface_mock.get_semester_details(datetime.utcnow())

    def test_update_network_model_no_events(self):
        self.network_model['1m0a.doma.lsc'] = {'name': '1m0a.doma.lsc',
                                               'events': [],
                                               'status': 'online'}
        self.network_model['1m0a.doma.coj'] = {'name': '1m0a.doma.coj',
                                               'events': [],
                                               'status': 'online'}

        current_events = {}
        current_events['1m0a.doma.lsc'] = []
        current_events['1m0a.doma.coj'] = []
        self.network_interface_mock.get_current_events = Mock(return_value=current_events)
        self.scheduler_runner.update_network_model()

        assert_equal(self.scheduler_runner.network_model['1m0a.doma.lsc']['events'], [])
        assert_equal(self.scheduler_runner.network_model['1m0a.doma.coj']['events'], [])


    def test_update_network_model_one_event(self):
        self.network_model['1m0a.doma.lsc'] = {'name': '1m0a.doma.lsc',
                                               'events': [],
                                               'status': 'online'}
        self.network_model['1m0a.doma.coj'] = {'name': '1m0a.doma.coj',
                                               'events': [],
                                               'status': 'online'}

        current_events = {}
        current_events['1m0a.doma.lsc'] = ['event1', 'event2']
        current_events['1m0a.doma.coj'] = []
        self.network_interface_mock.get_current_events = Mock(return_value=current_events)
        self.scheduler_runner.update_network_model()

        assert_equal(self.scheduler_runner.network_model['1m0a.doma.lsc']['events'], ['event1', 'event2'])
        assert_equal(self.scheduler_runner.network_model['1m0a.doma.coj']['events'], [])


    def test_scheduler_runner_update_network_model_with_new_event(self):
        network_model = {
                         '1m0a.doma.elp' : {'name': '1m0a.doma.elp',
                                               'events': [],
                                               'status': 'online'},
                         '1m0a.doma.lsc' : {'name': '1m0a.doma.lsc',
                                               'events': [],
                                               'status': 'online'}
                         }
        scheduler_runner = SchedulerRunner(self.sched_params, self.scheduler_mock, self.network_interface_mock, network_model, self.mock_input_factory)
        scheduler_runner.semester_details = self.valhalla_interface_mock.get_semester_details(None)
        self.network_interface_mock.get_current_events = Mock(return_value={'1m0a.doma.elp' : ['event']})
        scheduler_runner.update_network_model()
        assert_equal(network_model['1m0a.doma.elp']['events'], ['event'], "1m0a.doma.elp should have a single event")
        assert_equal(network_model['1m0a.doma.lsc']['events'], [], "1m0a.doma.lsc should have no events")


    def test_scheduler_runner_update_network_model_clear_event(self):
        network_model = {
                         '1m0a.doma.elp' : {'name': '1m0a.doma.elp',
                                               'events': [],
                                               'status': 'online'},
                         '1m0a.doma.lsc' : {'name': '1m0a.doma.lsc',
                                               'events': [],
                                               'status': 'online'}
                         }
        network_model['1m0a.doma.elp']['events'].append('event')
        scheduler_runner = SchedulerRunner(self.sched_params, self.scheduler_mock, self.network_interface_mock, network_model, self.mock_input_factory)
        scheduler_runner.semester_details = self.valhalla_interface_mock.get_semester_details(None)

        assert_equal(network_model['1m0a.doma.elp']['events'], ['event'], "1m0a.doma.elp should have a single event")
        assert_equal(network_model['1m0a.doma.lsc']['events'], [], "1m0a.doma.lsc should have no events")

        self.network_interface_mock.get_current_events = Mock(return_value={})
        scheduler_runner.update_network_model()
        assert_equal(network_model['1m0a.doma.elp']['events'], [], "1m0a.doma.elp should have no events")
        assert_equal(network_model['1m0a.doma.lsc']['events'], [], "1m0a.doma.elp should have no events")


    def test_determine_resource_cancelation_start_date_for_reservation_conflicting_running_request(self):
        ''' Should use default cancelation date when reservation overlaps with running request
        '''
        mock_reservation = Mock(scheduled_start=0)
        scheduled_reservations = [mock_reservation]

        start = datetime.strptime("2013-05-22 19:00:00", '%Y-%m-%d %H:%M:%S')
        end = start + timedelta(minutes=60)
        running_request = RunningRequest('1m0a.doma.elp', 1, start, end)
        running_user_request = RunningUserRequest(1, running_request)
        running_user_requests = [running_user_request]

        default_cancelation_start_date = start + timedelta(minutes=30)

        cancelation_start_date = self.scheduler_runner._determine_resource_cancelation_start_date(scheduled_reservations, running_user_requests, default_cancelation_start_date)

        expected_cancelation_start_date = end
        assert_equal(expected_cancelation_start_date, cancelation_start_date)


    def test_determine_resource_cancelation_start_date_for_reservation_not_conflicting_running_request(self):
        ''' Should cancel after running block has finished when reservation does not overlap with running request
        '''
        mock_reservation = Mock(scheduled_start=0)
        scheduled_reservations = [mock_reservation]

        start = datetime.strptime("2013-05-22 19:00:00", '%Y-%m-%d %H:%M:%S')
        end = start + timedelta(minutes=60)
        running_request = RunningRequest('1m0a.doma.elp', 1, start, end)
        running_user_request = RunningUserRequest(1, running_request)
        running_user_requests = [running_user_request]

        default_cancelation_start_date = start + timedelta(minutes=30)

        cancelation_start_date = self.scheduler_runner._determine_resource_cancelation_start_date(scheduled_reservations, running_user_requests, default_cancelation_start_date)

        expected_cancelation_start_date = end
        assert_equal(expected_cancelation_start_date, cancelation_start_date)


    def test_determine_resource_cancelation_start_date_for_reservation_no_running_request(self):
        ''' Should use default cancelation date when reservation overlaps with running request
        '''
        mock_reservation = Mock(scheduled_start=0)
        scheduled_reservations = [mock_reservation]

        running_user_requests = []

        default_cancelation_start_date = datetime.strptime("2013-05-22 19:00:00", '%Y-%m-%d %H:%M:%S')

        cancelation_start_date = self.scheduler_runner._determine_resource_cancelation_start_date(scheduled_reservations, running_user_requests, default_cancelation_start_date)

        expected_cancelation_start_date = default_cancelation_start_date
        assert_equal(expected_cancelation_start_date, cancelation_start_date)


    def test_determine_schedule_cancelation_start_dates(self):
        mock_reservation_elp = Mock(scheduled_start=0)
        mock_reservation_lsc = Mock(scheduled_start=0)
        scheduled_reservations = {}
        scheduled_reservations['1m0a.doma.elp'] = [mock_reservation_elp]
        scheduled_reservations['1m0a.doma.lsc'] = [mock_reservation_lsc]

        start = datetime.strptime("2013-05-22 19:00:00", '%Y-%m-%d %H:%M:%S')
        end = start + timedelta(minutes=60)
        running_request = RunningRequest('1m0a.doma.elp', 1, start, end)
        running_user_request = RunningUserRequest(1, running_request)
        resource_usage_snapshot = ResourceUsageSnapshot(Mock(), [running_user_request], Mock())

        default_cancelation_start_date = start + timedelta(minutes=30)
        default_cancelation_end_date = default_cancelation_start_date + timedelta(days=300)

        cancelation_start_dates = self.scheduler_runner._determine_schedule_cancelation_start_dates(['1m0a.doma.elp', '1m0a.doma.lsc'], scheduled_reservations, resource_usage_snapshot, default_cancelation_start_date, default_cancelation_end_date)

        expected_cancelation_start_dates = {
                                            '1m0a.doma.elp' : [(end, default_cancelation_end_date),],
                                            '1m0a.doma.lsc' : [(default_cancelation_start_date, default_cancelation_end_date),]
                                            }
        assert_equal(expected_cancelation_start_dates, cancelation_start_dates)


    def test_determine_abort_requests_empty_input(self):
        semester_start = datetime(2013, 5, 1)
        to_abort = self.scheduler_runner._determine_abort_requests([],
                                                      semester_start,
                                                      None)
        assert_equal([], to_abort)


    def test_conflicting_running_requests_aborted(self):
        ''' Should abort conflicting requests
        '''
        start = datetime.strptime("2013-05-22 19:00:00", '%Y-%m-%d %H:%M:%S')
        end = start + timedelta(minutes=60)
        running_request = RunningRequest('1m0a.doma.elp', 1, start, end)
        running_user_request = RunningUserRequest(1, running_request)
        running_user_requests = [running_user_request]

        semester_start = datetime(2013, 5, 1)
        running_req_start = (start - semester_start).total_seconds()
        running_req_length = (end - start).total_seconds()

        conflicting_reservation_start = running_req_start + running_req_length / 2
        conflicting_reservation = Mock(scheduled_start=conflicting_reservation_start,
                                       request=(Mock(request_number="123456789")))

        to_abort = self.scheduler_runner._determine_abort_requests(running_user_requests,
                                                      semester_start,
                                                      conflicting_reservation)

        assert_equal([(running_request, ["Request interrupted to observe request: 123456789"])], to_abort)


    def test_running_request_cannot_complete_succesfully_aborted(self):
        ''' Should abort requests with errors
        '''
        start = datetime.strptime("2013-05-22 19:00:00", '%Y-%m-%d %H:%M:%S')
        end = start + timedelta(minutes=60)
        running_request = RunningRequest('1m0a.doma.elp', 1, start, end)
        running_request.add_error("me love you long time")
        running_user_request = RunningUserRequest(1, running_request)
        running_user_requests = [running_user_request]

        semester_start = datetime(2013, 5, 1)

        to_abort = self.scheduler_runner._determine_abort_requests(running_user_requests,
                                                      semester_start,
                                                      None)

        assert_equal([(running_request, ["Can not complete successfully: me love you long time"])], to_abort)


    def test_scheduler_runner_abort_running_requests(self):
        start = datetime.strptime("2013-05-22 19:00:00", '%Y-%m-%d %H:%M:%S')
        end = start + timedelta(minutes=60)
        running_request = RunningRequest('1m0a.doma.elp', 1, start, end)
        running_request.add_error("me love you long time")

        self.scheduler_runner.abort_running_requests([(running_request, ("A good reason", "Another good reason"))])
        self.network_interface_mock.abort.assert_called_once_with(running_request, "A good reason, Another good reason")


    def test_scheduler_runner_abort_running_requests_empty_list(self):
        start = datetime.strptime("2013-05-22 19:00:00", '%Y-%m-%d %H:%M:%S')
        end = start + timedelta(minutes=60)
        running_request = RunningRequest('1m0a.doma.elp', 1, start, end)
        running_request.add_error("me love you long time")

        self.scheduler_runner.abort_running_requests([])
        assert_equal(self.network_interface_mock.abort.call_count, 0)


    def test_scheduler_runner_all_interfaces_mocked(self):
        ''' schedule should be changed through the network interface
        '''
        request_duration_seconds = 60
        priority = 10
        too_tracking_number = 1
        too_request_number = 1
        normal_tracking_number = 2
        normal_request_number = 2
        request_windows = create_user_request_windows((("2013-05-22T19:00:00Z", "2013-05-22T20:00:00Z"),))
        too_request = create_request(too_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc'], is_too=True)
        too_single_ur = create_user_request(too_tracking_number, priority, [too_request], 'single')
        normal_request = create_request(normal_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc'])
        normal_single_ur = create_user_request(normal_tracking_number, priority, [normal_request], 'single')

        self.scheduler_mock.run_scheduler = Mock(return_value=SchedulerResult())

        mock_input_factory = create_scheduler_input_factory([too_single_ur], [normal_single_ur])
        scheduler_runner = SchedulerRunner(self.sched_params, self.scheduler_mock, self.network_interface_mock, self.network_model, mock_input_factory)
        scheduler_runner.semester_details = self.valhalla_interface_mock.get_semester_details(None)
        scheduler_runner.run()

        assert_equal(2, self.scheduler_mock.run_scheduler.call_count)
        assert_equal(2, self.network_interface_mock.cancel.call_count)
        assert_equal(1, self.network_interface_mock.save.call_count)


    def test_scheduler_runner_no_too_requests(self):
        ''' Shouldn't fail when no ToOs are passed to scheduler
        '''
        request_duration_seconds = 60
        priority = 10
        normal_tracking_number = 2
        normal_request_number = 2
        request_windows = create_user_request_windows((("2013-05-22T19:00:00Z", "2013-05-22T20:00:00Z"),))
        normal_request = create_request(normal_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc'])
        normal_single_ur = create_user_request(normal_tracking_number, priority, [normal_request], 'single')

        self.scheduler_mock.run_scheduler = Mock(return_value=SchedulerResult())

        mock_input_factory = create_scheduler_input_factory([], [normal_single_ur])
        scheduler_runner = SchedulerRunner(self.sched_params, self.scheduler_mock, self.network_interface_mock, self.network_model, mock_input_factory)
        scheduler_runner.semester_details = self.valhalla_interface_mock.get_semester_details(None)
        scheduler_runner.json_urs_to_scheduler_model_urs = Mock(return_value=[normal_single_ur])
        scheduler_runner.run()

        assert_equal(1, self.scheduler_mock.run_scheduler.call_count)
        assert_equal(0, self.network_interface_mock.cancel.call_count)
        assert_equal(0, self.network_interface_mock.save.call_count)


    def test_scheduler_runner_no_normal_requests(self):
        ''' Shouldn't fail when no normal requests are passed to scheduler
        '''
        request_duration_seconds = 60
        priority = 10
        too_tracking_number = 1
        too_request_number = 1
        request_windows = create_user_request_windows((("2013-05-22T19:00:00Z", "2013-05-22T20:00:00Z"),))
        too_request = create_request(too_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc'], is_too=True)
        too_single_ur = create_user_request(too_tracking_number, priority, [too_request], 'single')

        self.scheduler_mock.run_scheduler = Mock(return_value=SchedulerResult())

        mock_input_factory = create_scheduler_input_factory([too_single_ur], [])
        scheduler_runner = SchedulerRunner(self.sched_params, self.scheduler_mock, self.network_interface_mock, self.network_model, mock_input_factory)
        scheduler_runner.semester_details = self.valhalla_interface_mock.get_semester_details(None)
        scheduler_runner.json_urs_to_scheduler_model_urs = Mock(return_value=[too_single_ur])
        scheduler_runner.run()

        assert_equal(1, self.scheduler_mock.run_scheduler.call_count)
        assert_equal(2, self.network_interface_mock.cancel.call_count)
        assert_equal(1, self.network_interface_mock.save.call_count)


    @patch('adaptive_scheduler.scheduler_input.SchedulingInput.user_requests')
    def test_call_scheduler_cancels_proper_resources(self, mock_prop):
        ''' Only part of resources scheduled for ToO should be canceled after ToO scheduling run
        and normal run should not cancel toos
        '''
        network_model = {'1m0a.doma.lsc': {}, '1m0a.doma.elp': {}}
        too_input = SchedulingInput(sched_params=SchedulerParameters(), scheduler_now=datetime.utcnow(),
                                    estimated_scheduler_runtime=timedelta(seconds=300),
                                    json_user_request_list=[],
                                    resource_usage_snapshot=ResourceUsageSnapshot(datetime.utcnow, [], []),
                                    model_builder=None,
                                    normal_model_user_requests=[],
                                    too_model_user_requests=[],
                                    available_resources=['1m0a.doma.lsc', '1m0a.doma.elp'],
                                    is_too_input=True)
#         too_input.user_requests = Mock()
        self.mock_input_factory.create_too_scheduling_input = Mock(return_value=too_input)
        normal_input = SchedulingInput(sched_params=SchedulerParameters(), scheduler_now=datetime.utcnow(),
                                    estimated_scheduler_runtime=timedelta(seconds=300),
                                    json_user_request_list=[],
                                    resource_usage_snapshot=ResourceUsageSnapshot(datetime.utcnow, [], []),
                                    model_builder=None,
                                    normal_model_user_requests=[],
                                    too_model_user_requests=[],
                                    available_resources=['1m0a.doma.lsc', '1m0a.doma.elp'],
                                    is_too_input=False)
#         normal_input.user_requests = Mock()
        self.mock_input_factory.create_normal_scheduling_input = Mock(return_value=normal_input)

        scheduler_runner = SchedulerRunner(self.sched_params, self.scheduler_mock, self.network_interface_mock, network_model, self.mock_input_factory)
        scheduler_runner.semester_details = self.valhalla_interface_mock.get_semester_details(None)
        too_reservation = Mock()
        too_reservation_start = datetime(2017, 1, 10, 5)
        too_reservation.scheduled_start = datetime_to_normalised_epoch(too_reservation_start,
                                                                       self.valhalla_interface_mock.get_semester_details(None)['start'])
        too_reservation.duration = 3600
        too_scheduler_result = SchedulerResult(
                                resource_schedules_to_cancel=['1m0a.doma.lsc'],
                                schedule={'1m0a.doma.lsc':[too_reservation]}
                                )
        normal_scheduler_result = SchedulerResult(
                                    resource_schedules_to_cancel=['1m0a.doma.lsc', '1m0a.doma.elp'],
                                    schedule={'1m0a.doma.lsc':[Mock()], '1m0a.doma.elp':[Mock()]}
                                )
        scheduler_runner.call_scheduler = Mock(side_effect=lambda scheduler_input, estimated_scheduler_end: too_scheduler_result if scheduler_input.is_too_input else normal_scheduler_result)

        scheduler_runner._determine_resource_cancelation_start_date = Mock(return_value=Mock())
        network_state_timestamp = datetime(2014, 10, 29, 12, 0, 0)
        scheduler_runner.create_new_schedule(network_state_timestamp)


        assert_equal(2, scheduler_runner.call_scheduler.call_count)
        assert_equal(4, self.network_interface_mock.cancel.call_count)

        assert_true('1m0a.doma.lsc' in self.network_interface_mock.cancel.call_args_list[0][0][0])
        assert_false('1m0a.doma.elp' in self.network_interface_mock.cancel.call_args_list[0][0][0])
        # too loop cancels just the time it has reserved on a resource
        assert_equal(self.network_interface_mock.cancel.call_args_list[0][0][0]['1m0a.doma.lsc'], [(too_reservation_start, too_reservation_start + timedelta(seconds=too_reservation.duration))])
        # cancel too flag set for too loop
        assert_true(self.network_interface_mock.cancel.call_args_list[0][0][2])
        # cancel normal flag set for first cancel of ToO loop (time based cancel)
        assert_true(self.network_interface_mock.cancel.call_args_list[0][0][3])
        # cancel normal flag not set for second cancel of ToO loop (all resource ToO cancel)
        assert_false(self.network_interface_mock.cancel.call_args_list[1][0][3])
        # cancel_too flag not set for normal loop
        assert_false(self.network_interface_mock.cancel.call_args_list[2][0][2])
        # normal loop cancels on all resources
        assert_true('1m0a.doma.elp' in self.network_interface_mock.cancel.call_args_list[2][0][0])
        assert_true('1m0a.doma.lsc' in self.network_interface_mock.cancel.call_args_list[3][0][0])


    def test_scheduler_runner_dry_run(self):
        ''' No write calls to network interface should be made
        '''
        request_duration_seconds = 60
        priority = 10
        too_tracking_number = 1
        too_request_number = 1
        normal_tracking_number = 2
        normal_request_number = 2
        request_windows = create_user_request_windows((("2013-05-22T19:00:00Z", "2013-05-22T20:00:00Z"),))
        too_request = create_request(too_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc'], is_too=True)
        too_single_ur = create_user_request(too_tracking_number, priority, [too_request], 'single')
#         too_single_ur.has_target_of_opportunity = Mock(return_value=True)
        normal_request = create_request(normal_request_number, duration=request_duration_seconds, windows=request_windows, possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc'])
        normal_single_ur = create_user_request(normal_tracking_number, priority, [normal_request], 'single')


        sched_params = SchedulerParameters(run_once=True, dry_run=True)
        scheduler_mock = Mock()
        scheduler_mock.run_scheduler = Mock(return_value=SchedulerResult())
        network_model_mock = {}

        mock_input_factory = create_scheduler_input_factory([too_single_ur], [normal_single_ur])
        scheduler_runner = SchedulerRunner(sched_params, scheduler_mock, self.network_interface_mock, network_model_mock, mock_input_factory)
        scheduler_runner.semester_details = self.valhalla_interface_mock.get_semester_details(None)
        scheduler_runner.run()

        assert_equal(2, scheduler_mock.run_scheduler.call_count)
        # assert_equal(0, self.network_interface_mock.set_requests_to_unschedulable.call_count)
        # assert_equal(0, self.network_interface_mock.set_user_requests_to_unschedulable.call_count)
        assert_equal(0, self.network_interface_mock.cancel.call_count)
        assert_equal(0, self.network_interface_mock.save.call_count)


class TestSchedulerRunnerUseOfRunTimes(object):

    def setup(self):
        self.sched_params = SchedulerParameters()
        self.scheduler = Mock()
        self.scheduler.run_scheduler = Mock(return_value=SchedulerResult())
        self.network_interface = Mock()
        self.network_interface.cancel = Mock(return_value=0)
        self.network_interface.save = Mock(return_value=0)
        self.valhalla_interface_mock = Mock()
        self.valhalla_interface_mock.get_semester_details = Mock(return_value={'id': '2015A',
                                                                               'start': datetime.utcnow() - timedelta(days=30),
                                                                               'end': datetime.utcnow() + timedelta(days=30)})
        self.network_interface.valhalla_interface = self.valhalla_interface_mock
        self.network_model = {}
        self.input_factory = Mock()


    def create_user_request_list(self):
        request_duration_seconds = 60
        priority = 10
        too_tracking_number = 1
        too_request_number = 1
        normal_tracking_number = 2
        normal_request_number = 2
        request_windows = create_user_request_windows(
                            (
                             ("2013-05-22T19:00:00Z", "2013-05-22T20:00:00Z"),
                            ))
        too_request = create_request(too_request_number,
                                     duration=request_duration_seconds,
                                     windows=request_windows,
                                     possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc'],
                                     is_too=True)
        too_single_ur = create_user_request(too_tracking_number, priority,
                                            [too_request], 'single')
        normal_request = create_request(normal_request_number,
                                        duration=request_duration_seconds,
                                        windows=request_windows,
                                        possible_telescopes=['1m0a.doma.elp', '1m0a.doma.lsc'])
        normal_single_ur = create_user_request(normal_tracking_number,
                                               priority, [normal_request],
                                               'single')

        return [normal_single_ur, too_single_ur]


    def setup_mock_create_input_factory(self, user_requests):
        snapshot = ResourceUsageSnapshot(datetime.utcnow, [], [])
        too_scheduling_input = Mock(user_requests=user_requests,
                                    scheduler_now=datetime.utcnow(),
                                    estimated_scheduler_end=datetime.utcnow(),
                                    resource_usage_snapshot=snapshot)
        too_scheduling_input.get_scheduling_start = Mock(return_value=datetime.utcnow())
        normal_scheduling_input = Mock(user_requests=user_requests,
                                       scheduler_now=datetime.utcnow(),
                                       estimated_scheduler_end=datetime.utcnow(),
                                       resource_usage_snapshot=snapshot)
        normal_scheduling_input.get_scheduling_start = Mock(return_value=datetime.utcnow())
        too_input_mock = Mock(return_value=too_scheduling_input)
        self.input_factory.create_too_scheduling_input = too_input_mock
        normal_input_mock = Mock(return_value=normal_scheduling_input)
        self.input_factory.create_normal_scheduling_input = normal_input_mock


    def test_create_new_schedule_uses_estimated_run_time(self):
        '''Should use estimated run times but should not set estimates because
        of empty input
        '''
        self.setup_mock_create_input_factory([])
        scheduler_runner = SchedulerRunner(self.sched_params, self.scheduler,
                                           self.network_interface,
                                           self.network_model,
                                           self.input_factory)
        expected_too_run_time_arg = scheduler_runner.estimated_too_run_timedelta
        expected_normal_run_time_arg = scheduler_runner.estimated_normal_run_timedelta
        network_state_timestamp = datetime(2014, 10, 29, 12, 0, 0)
        scheduler_runner.create_new_schedule(network_state_timestamp)

        assert_equal(1, self.input_factory.create_too_scheduling_input.call_count)
        assert_equal(expected_too_run_time_arg.total_seconds(),
                     self.input_factory.create_too_scheduling_input.call_args[0][0])
        assert_equal(expected_too_run_time_arg,
                     scheduler_runner.estimated_too_run_timedelta,
                     msg="Estimated run time should not change")

        assert_equal(1, self.input_factory.create_normal_scheduling_input.call_count)
        assert_equal(expected_normal_run_time_arg.total_seconds(),
                     self.input_factory.create_normal_scheduling_input.call_args[0][0])
        assert_equal(expected_normal_run_time_arg,
                     scheduler_runner.estimated_normal_run_timedelta,
                     msg="Estimated run time should not change")


    def test_create_new_schedule_uses_and_sets_estimated_run_time(self):
        '''Should use estimated run times and should set estimates
        '''
        self.setup_mock_create_input_factory(self.create_user_request_list())
        scheduler_runner = SchedulerRunner(self.sched_params, self.scheduler,
                                           self.network_interface,
                                           self.network_model,
                                           self.input_factory)
        scheduler_runner.semester_details = self.valhalla_interface_mock.get_semester_details(datetime.utcnow())
        expected_too_run_time_arg = scheduler_runner.estimated_too_run_timedelta
        expected_normal_run_time_arg = scheduler_runner.estimated_normal_run_timedelta
        network_state_timestamp = datetime(2014, 10, 29, 12, 0, 0)
        scheduler_runner.create_new_schedule(network_state_timestamp)

        assert_equal(1, self.input_factory.create_too_scheduling_input.call_count)
        assert_equal(expected_too_run_time_arg.total_seconds(),
                     self.input_factory.create_too_scheduling_input.call_args[0][0])
        assert_not_equal(expected_too_run_time_arg,
                         scheduler_runner.estimated_too_run_timedelta,
                         msg="Estimated run time should have change")

        assert_equal(1, self.input_factory.create_normal_scheduling_input.call_count)
        assert_equal(expected_normal_run_time_arg.total_seconds(),
                     self.input_factory.create_normal_scheduling_input.call_args[0][0])
        assert_not_equal(expected_normal_run_time_arg,
                         scheduler_runner.estimated_normal_run_timedelta,
                         msg="Estimated run time should have changed")


    def test_scheduler_rerun_required_calls_schedulable_request_set_has_changed(self):
        '''scheduler_rerun_required should always call network_interface.schedulable_request_set_has_changed
        '''
        self.sched_params.run_once = True
        self.setup_mock_create_input_factory([])
        scheduler_runner = SchedulerRunner(self.sched_params, self.scheduler,
                                           self.network_interface,
                                           self.network_model,
                                           self.input_factory)

        scheduler_runner.scheduler_rerun_required()
        assert_equal(1, self.network_interface.schedulable_request_set_has_changed.call_count)


    def test_scheduler_rerun_required_does_not_call_schedulable_request_set_has_changed_on_dry_run(self):
        '''scheduler_rerun_required should always call network_interface.schedulable_request_set_has_changed
        '''
        self.sched_params.run_once = True
        self.sched_params.dry_run = True
        self.setup_mock_create_input_factory([])
        self.network_interface.current_events_has_changed = Mock(return_value=True)
        self.network_interface.schedulable_request_set_has_changed = Mock(return_value=False)
        scheduler_runner = SchedulerRunner(self.sched_params, self.scheduler,
                                           self.network_interface,
                                           self.network_model,
                                           self.input_factory)

        scheduler_runner.scheduler_rerun_required()
        assert_equal(0, self.network_interface.schedulable_request_set_has_changed.call_count)


    def test_scheduler_rerun_required_returns_true_when_current_events_has_changed(self):
        '''scheduler_rerun_required should always call network_interface.schedulable_request_set_has_changed
        '''
        self.sched_params.run_once = True
        self.setup_mock_create_input_factory([])
        for return_val in [True, False]:
            self.network_interface.current_events_has_changed = Mock(return_value=return_val)
            self.network_interface.schedulable_request_set_has_changed = Mock(return_value=False)
            scheduler_runner = SchedulerRunner(self.sched_params, self.scheduler,
                                               self.network_interface,
                                               self.network_model,
                                               self.input_factory)
            rerun_required = scheduler_runner.scheduler_rerun_required()
            assert_equal(1, self.network_interface.schedulable_request_set_has_changed.call_count)
            assert_equal(rerun_required, return_val, msg="scheduler_rerun_required returned %s when network_interface.current_events_has_changed returned %s" % (rerun_required, return_val))


class TestSchedulerInputProvider(object):

    @patch('adaptive_scheduler.scheduler_input.SchedulingInputProvider._get_json_user_request_list')
    def test_provider_doesnt_consider_blocks_running_on_resources_with_events(self, mock1):
        '''Should exclude resources with events from resource usage snapshot
        '''
        available_resource = 'available'
        unavailable_resource = 'unavailable'
        network_model = {
                         available_resource : dict(events=[]),
                         unavailable_resource : dict(events=[1]),
                         }

        sched_params = SchedulerParameters()
        mock_network_interface = Mock()
        input_provider = SchedulingInputProvider(sched_params, mock_network_interface, network_model, False)
        input_provider.refresh()
        assert_true(available_resource in input_provider.available_resources)
        assert_true(unavailable_resource not in input_provider.available_resources)
        assert_true(available_resource in mock_network_interface.resource_usage_snapshot.call_args[0][0])
        assert_true(unavailable_resource in mock_network_interface.resource_usage_snapshot.call_args[0][0])


