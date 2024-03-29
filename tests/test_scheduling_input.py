from adaptive_scheduler.scheduler_input import SchedulingInputProvider, SchedulerParameters, SchedulingInputFactory, \
    SchedulingInputUtils
from adaptive_scheduler.models import RequestError

from mock import Mock, patch

from datetime import datetime, timedelta

import pytest


class TestSchedulingInputProvider(object):

    def setup(self):
        self.sched_params = SchedulerParameters()
        self.network_interface = Mock()
        self.network_interface.get_all_request_groups = Mock(return_value=[])

        self.observation_portal_interface = Mock()
        self.observation_portal_interface.get_semester_details = Mock(
            return_value={'id': '2015A',
                          'start': datetime.utcnow() - timedelta(
                              days=30),
                          'end': datetime.utcnow() + timedelta(
                              days=30)}
        )
        self.network_interface.observation_portal_interface = self.observation_portal_interface
        self.network_model = {}

    def test_constructor(self):
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model,
                                                 is_rr_input=True)
        assert self.sched_params.rr_runtime_seconds == input_provider.estimated_rr_run_time.total_seconds()
        assert self.sched_params.normal_runtime_seconds == input_provider.estimated_normal_run_time.total_seconds()
        assert None == input_provider.scheduler_now
        assert input_provider.estimated_rr_run_time == input_provider.estimated_scheduler_runtime()
        assert None == input_provider.json_request_group_list
        assert None == input_provider.available_resources
        assert None == input_provider.resource_usage_snapshot

    def test_input_does_not_exclude_resources_with_events(self):
        self.network_model['1m0a.doma.elp'] = {'name': '1m0a.doma.elp',
                                               'events': [],
                                               'status': 'online'}
        self.network_model['1m0a.doma.lsc'] = {'name': '1m0a.doma.lsc',
                                               'events': [],
                                               'status': 'online'}
        self.network_model['1m0a.doma.lsc']['events'].append('event')
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model,
                                                 is_rr_input=True)
        input_provider.refresh()
        assert ['1m0a.doma.elp'] == input_provider.available_resources
        assert (set(['1m0a.doma.elp', '1m0a.doma.lsc']) ==
                     set(self.network_interface.resource_usage_snapshot.call_args[0][0]))

    def test_input_scheduler_now_when_not_provided_by_parameter(self):
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model,
                                                 is_rr_input=True)
        test_now = datetime.utcnow()
        input_provider.refresh()
        assert 0 == pytest.approx(
            (input_provider.scheduler_now - test_now).total_seconds(),
            abs=5
        )

    def test_input_scheduler_now_when_provided_by_parameter(self):
        simulated_now_str = '1980-06-10T08:00:00Z'
        simulated_now = datetime.strptime(simulated_now_str, '%Y-%m-%dT%H:%M:%SZ')
        self.sched_params.simulate_now = simulated_now_str
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model,
                                                 is_rr_input=True)
        input_provider.refresh()
        assert simulated_now == input_provider.scheduler_now

    def test_rr_input_estimated_scheduler_end(self):
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model,
                                                 is_rr_input=True)
        test_now = datetime.utcnow()
        input_provider.refresh()

        assert 0 ==  pytest.approx(
            (input_provider.scheduler_now - test_now).total_seconds(),
            abs=5
        )
        assert self.sched_params.rr_runtime_seconds == input_provider.estimated_scheduler_runtime().total_seconds()
        assert 1 == self.network_interface.get_all_request_groups.call_count
        assert 1 == self.network_interface.resource_usage_snapshot.call_count

        assert 0 == pytest.approx(
            (self.network_interface.resource_usage_snapshot.call_args[0][1] - test_now).total_seconds(),
            abs=5
        ), 'Snapshot start should be refresh time'

        assert self.sched_params.rr_runtime_seconds == pytest.approx(
            (self.network_interface.resource_usage_snapshot.call_args[0][2] - test_now).total_seconds(),
            abs=5
        ), 'Snapshot end should be refresh time + RR scheduling run time'

    def test_normal_input_estimated_scheduler_end(self):
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model,
                                                 is_rr_input=False)
        test_now = datetime.utcnow()
        input_provider.refresh()

        assert 0 ==  pytest.approx(
            (input_provider.scheduler_now - test_now).total_seconds(),
            abs=5
        )
        assert self.sched_params.normal_runtime_seconds == input_provider.estimated_scheduler_runtime().total_seconds()
        assert 0 == self.network_interface.get_all_request_groups.call_count
        assert 1 == self.network_interface.resource_usage_snapshot.call_count
        assert 0 == pytest.approx(
            (self.network_interface.resource_usage_snapshot.call_args[0][1] - test_now).total_seconds(),
            abs=5
        ), 'Snapshot start should be refresh time'

        assert self.sched_params.normal_runtime_seconds == pytest.approx(
            (self.network_interface.resource_usage_snapshot.call_args[0][2] - test_now).total_seconds(),
            abs=5
        ), 'Snapshot end should be refresh time + Normal scheduling run time'

    def test_normal_input_resource_usage_snapshot_start(self):
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model,
                                                 is_rr_input=False)
        last_known_state = datetime(2014, 10, 29, 5, 47, 0)
        input_provider.set_last_known_state(last_known_state)
        input_provider.refresh()

        assert last_known_state.timestamp() == pytest.approx(
            self.network_interface.resource_usage_snapshot.call_args[0][1].timestamp(),
        ),  'Snapshot start should be last known state'

    def test_set_rr_mode(self):
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model,
                                                 is_rr_input=False)
        test_now = datetime.utcnow()
        input_provider.set_rr_mode()
        assert True == input_provider.is_rr_input
        assert 0 == pytest.approx(
            (input_provider.scheduler_now - test_now).total_seconds(),
            abs=5
        )
        assert self.sched_params.rr_runtime_seconds == input_provider.estimated_scheduler_runtime().total_seconds()
        assert 1 == self.network_interface.get_all_request_groups.call_count
        assert 1 == self.network_interface.resource_usage_snapshot.call_count
        assert 0 == pytest.approx(
            (self.network_interface.resource_usage_snapshot.call_args[0][1] - test_now).total_seconds(),
            abs=5
        ), 'Snapshot start should be refresh time'
        assert self.sched_params.rr_runtime_seconds == pytest.approx(
            (self.network_interface.resource_usage_snapshot.call_args[0][2] - test_now).total_seconds(),
            abs=5
        ), 'Snapshot end should be refresh time + RR scheduling run time'

    def test_set_normal_mode(self):
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model,
                                                 is_rr_input=False)
        test_now = datetime.utcnow()
        input_provider.set_normal_mode()
        assert False == input_provider.is_rr_input
        assert 0 == pytest.approx(
            (input_provider.scheduler_now - test_now).total_seconds(),
            abs=5
        )
        assert self.sched_params.normal_runtime_seconds == input_provider.estimated_scheduler_runtime().total_seconds()
        assert 0 == self.network_interface.get_all_request_groups.call_count
        assert 1 == self.network_interface.resource_usage_snapshot.call_count
        assert 0 == pytest.approx(
            (self.network_interface.resource_usage_snapshot.call_args[0][1] - test_now).total_seconds(),
            abs=5
        ), 'Snapshot start should be refresh time'
        assert self.sched_params.normal_runtime_seconds == pytest.approx(
            (self.network_interface.resource_usage_snapshot.call_args[0][2] - test_now).total_seconds(),
            abs=5
        ), 'Snapshot end should be refresh time + Normal scheduling run time'

    def test_set_rr_run_time(self):
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model,
                                                 is_rr_input=False)
        current_estimate = input_provider.estimated_rr_run_time
        new_estimate_timedelta = current_estimate + timedelta(seconds=100)
        input_provider.set_rr_run_time(new_estimate_timedelta.total_seconds())

    def test_set_normal_run_time(self):
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model,
                                                 is_rr_input=False)
        current_estimate = input_provider.estimated_normal_run_time
        new_estimate_timedelta = current_estimate + timedelta(seconds=100)
        input_provider.set_normal_run_time(new_estimate_timedelta.total_seconds())


class TestSchedulingInputFactory(object):

    def setup(self):
        self.input = Mock()
        self.input.rr_request_groups = []
        self.input.normal_request_groups = []
        self.input.request_groups = []
        self.create_input_mock = Mock(return_value=self.input)

    def test_constructor(self):
        input_provider = Mock()
        factory = SchedulingInputFactory(input_provider)
        assert input_provider == factory.input_provider

    def test_create_rr_scheduling(self):
        input_provider = Mock()
        input_provider.sched_params = SchedulerParameters()
        input_provider.json_request_group_list = []
        with patch('adaptive_scheduler.scheduler_input.SchedulingInputFactory._create_scheduling_input',
                   self.create_input_mock, create=True):
            factory = SchedulingInputFactory(input_provider)
            factory.create_rr_scheduling_input(100)
            assert 1 == input_provider.set_rr_mode.call_count
            assert 1 == input_provider.set_rr_run_time.call_count
            assert 100 == input_provider.set_rr_run_time.call_args[0][0]

    def test_create_rr_scheduling_no_estimate_provided(self):
        input_provider = Mock()
        input_provider.sched_params = SchedulerParameters()
        input_provider.json_request_group_list = []
        with patch('adaptive_scheduler.scheduler_input.SchedulingInputFactory._create_scheduling_input',
                   self.create_input_mock, create=True):
            factory = SchedulingInputFactory(input_provider)
            factory.create_rr_scheduling_input()
            assert 1 == input_provider.set_rr_mode.call_count
            assert 0 == input_provider.set_rr_run_time.call_count

    def test_create_normal_scheduling(self):
        input_provider = Mock()
        input_provider.sched_params = SchedulerParameters()
        with patch('adaptive_scheduler.scheduler_input.SchedulingInputFactory._create_scheduling_input',
                   self.create_input_mock, create=True):
            factory = SchedulingInputFactory(input_provider)
            factory.create_normal_scheduling_input(600)
            assert 1 == input_provider.set_normal_mode.call_count
            assert 1 == input_provider.set_normal_run_time.call_count
            assert 600 == input_provider.set_normal_run_time.call_args[0][0]

    def test_create_normal_scheduling_no_estimate_provided(self):
        input_provider = Mock()
        input_provider.sched_params = SchedulerParameters()
        with patch('adaptive_scheduler.scheduler_input.SchedulingInputFactory._create_scheduling_input',
                   self.create_input_mock, create=True):
            factory = SchedulingInputFactory(input_provider)
            factory.create_normal_scheduling_input()
            assert 1 == input_provider.set_normal_mode.call_count
            assert 0 == input_provider.set_normal_run_time.call_count


class TestSchedulingInputUtils(object):

    def test_json_rgs_to_scheduler_model_rgs_returns_invalid_requests(self):
        mock_model_builder = Mock()
        mock_rg = Mock()
        mock_model_builder.build_request_group = Mock(return_value=(mock_rg, {'id': '1'}))

        utils = SchedulingInputUtils(mock_model_builder)
        mock_rgs, invalid_rgs, invalid_rs = utils.json_rgs_to_scheduler_model_rgs([{'id': 'dummy1'},
                                                                                   {'id': 'dummy2'}])
        assert 2 == len(mock_rgs)
        assert [] == invalid_rgs
        assert 2 == len(invalid_rs)

    def test_json_rgs_to_scheduler_model_rgs_returns_invalid_request_groups(self):
        mock_model_builder = Mock()
        mock_model_builder.build_request_group = Mock(side_effect=RequestError)

        utils = SchedulingInputUtils(mock_model_builder)
        model_rgs, invalid_rgs, invalid_rs = utils.json_rgs_to_scheduler_model_rgs([{'id': 'dummy1'},
                                                                                    {'id': 'dummy2'}])
        assert 0 == len(model_rgs)
        assert 2 == len(invalid_rgs)
        assert 0 == len(invalid_rs)
