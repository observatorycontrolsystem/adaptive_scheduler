from adaptive_scheduler.scheduler_input import SchedulingInputProvider, SchedulerParameters, SchedulingInputFactory
from adaptive_scheduler.interfaces import ResourceUsageSnapshot
from adaptive_scheduler.model2 import Telescope

from mock import Mock
from nose.tools import assert_equal, assert_almost_equal, assert_not_equal, assert_true, assert_false

from datetime import datetime, timedelta


class TestSchedulingInputProvider(object):
    
    def setUp(self):
        self.sched_params = SchedulerParameters()
        self.network_interface = Mock()
        self.network_interface.get_all_user_requests = Mock(return_value=[])
        self.network_model = {}
        
    
    def test_constructor(self):
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model, is_too_input=True)
        assert_equal(self.sched_params.too_run_time, input_provider._estimated_too_run_time.total_seconds())
        assert_equal(self.sched_params.normal_run_time, input_provider._estimated_normal_run_time.total_seconds())
        assert_equal(None, input_provider.scheduler_now)
        assert_equal(None, input_provider.estimated_scheduler_end)
        assert_equal(None, input_provider.json_user_request_list)
        assert_equal(None, input_provider.available_resources)
        assert_equal(None, input_provider.resource_usage_snapshot)
        
    
    def test_input_does_not_exlude_resources_with_events(self):
        self.network_model['1m0a.doma.elp'] = Telescope()
        self.network_model['1m0a.doma.lsc'] = Telescope()
        self.network_model['1m0a.doma.lsc'].events.append('event')
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model, is_too_input=True)
        input_provider.refresh()
        assert_equal(['1m0a.doma.elp'], input_provider.available_resources)
        assert_equal(['1m0a.doma.elp', '1m0a.doma.lsc'], self.network_interface.resource_usage_snapshot.call_args[0][0])
        
        
    def test_input_scheduler_now_when_not_provided_by_parameter(self):
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model, is_too_input=True)
        test_now = datetime.utcnow()
        input_provider.refresh()
        assert_almost_equal(0, (input_provider.scheduler_now - test_now).total_seconds(), delta=5)
        
        
    def test_input_scheduler_now_when_provided_by_parameter(self):
        simulated_now_str = '1980-06-10 08:00:00'
        simulated_now = datetime.strptime(simulated_now_str, '%Y-%m-%d %H:%M:%S')
        self.sched_params.simulate_now = simulated_now_str
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model, is_too_input=True)
        input_provider.refresh()
        assert_equal(simulated_now, input_provider.scheduler_now)
        
        
    def test_too_input_estimated_scheduler_end(self):
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model, is_too_input=True)
        test_now = datetime.utcnow()
        input_provider.refresh()
        
        assert_almost_equal(0, (input_provider.scheduler_now - test_now).total_seconds(), delta=5)
        assert_equal(self.sched_params.too_run_time, (input_provider.estimated_scheduler_end - input_provider.scheduler_now).total_seconds())
        assert_equal(1, self.network_interface.get_all_user_requests.call_count)
        assert_equal(1, self.network_interface.resource_usage_snapshot.call_count)
        assert_almost_equal(0, (self.network_interface.resource_usage_snapshot.call_args[0][1] - test_now).total_seconds(), delta=5, msg='Snapshot start should be refresh time')
        assert_almost_equal(self.sched_params.too_run_time, (self.network_interface.resource_usage_snapshot.call_args[0][2] - test_now).total_seconds(), delta=5, msg='Snapshot end should be refresh time + ToO scheduling run time')
        
        
    def test_normal_input_estimated_scheduler_end(self):
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model, is_too_input=False)
        test_now = datetime.utcnow()
        input_provider.refresh()
        
        assert_almost_equal(0, (input_provider.scheduler_now - test_now).total_seconds(), delta=5)
        assert_equal(self.sched_params.normal_run_time, (input_provider.estimated_scheduler_end - input_provider.scheduler_now).total_seconds())
        assert_equal(1, self.network_interface.get_all_user_requests.call_count)
        assert_equal(1, self.network_interface.resource_usage_snapshot.call_count)
        assert_almost_equal(0, (self.network_interface.resource_usage_snapshot.call_args[0][1] - test_now).total_seconds(), delta=5, msg='Snapshot start should be refresh time')
        assert_almost_equal(self.sched_params.normal_run_time, (self.network_interface.resource_usage_snapshot.call_args[0][2] - test_now).total_seconds(), delta=5, msg='Snapshot end should be refresh time + Normal scheduling run time')
        
        
    def test_normal_input_resource_usage_snapshot_start(self):
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model, is_too_input=False)
        last_known_state = datetime(2014, 10, 29, 5, 47, 0)
        input_provider.set_last_known_state(last_known_state)
        input_provider.refresh()
        
        assert_almost_equal(last_known_state, self.network_interface.resource_usage_snapshot.call_args[0][1], msg='Snapshot start should be last know state')
        
        
    def test_set_too_mode(self):
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model, is_too_input=False)
        test_now = datetime.utcnow()
        input_provider.set_too_mode()
        assert_equal(True, input_provider.is_too_input)        
        assert_almost_equal(0, (input_provider.scheduler_now - test_now).total_seconds(), delta=5)
        assert_equal(self.sched_params.too_run_time, (input_provider.estimated_scheduler_end - input_provider.scheduler_now).total_seconds())
        assert_equal(1, self.network_interface.get_all_user_requests.call_count)
        assert_equal(1, self.network_interface.resource_usage_snapshot.call_count)
        assert_almost_equal(0, (self.network_interface.resource_usage_snapshot.call_args[0][1] - test_now).total_seconds(), delta=5, msg='Snapshot start should be refresh time')
        assert_almost_equal(self.sched_params.too_run_time, (self.network_interface.resource_usage_snapshot.call_args[0][2] - test_now).total_seconds(), delta=5, msg='Snapshot end should be refresh time + ToO scheduling run time')
        
        
    def test_set_normal_mode(self):
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model, is_too_input=False)
        test_now = datetime.utcnow()
        input_provider.set_normal_mode()
        assert_equal(False, input_provider.is_too_input)
        assert_almost_equal(0, (input_provider.scheduler_now - test_now).total_seconds(), delta=5)
        assert_equal(self.sched_params.normal_run_time, (input_provider.estimated_scheduler_end - input_provider.scheduler_now).total_seconds())
        assert_equal(1, self.network_interface.get_all_user_requests.call_count)
        assert_equal(1, self.network_interface.resource_usage_snapshot.call_count)
        assert_almost_equal(0, (self.network_interface.resource_usage_snapshot.call_args[0][1] - test_now).total_seconds(), delta=5, msg='Snapshot start should be refresh time')
        assert_almost_equal(self.sched_params.normal_run_time, (self.network_interface.resource_usage_snapshot.call_args[0][2] - test_now).total_seconds(), delta=5, msg='Snapshot end should be refresh time + Normal scheduling run time')
        
        
    def test_set_too_run_time(self):
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model, is_too_input=False)
        current_estimate = input_provider._estimated_too_run_time
        new_estimate_timedelta = current_estimate + timedelta(seconds=100)
        input_provider.set_too_run_time(new_estimate_timedelta.total_seconds())
        
        
    def test_set_normal_run_time(self):
        input_provider = SchedulingInputProvider(self.sched_params, self.network_interface, self.network_model, is_too_input=False)
        current_estimate = input_provider._estimated_normal_run_time
        new_estimate_timedelta = current_estimate + timedelta(seconds=100)
        input_provider.set_normal_run_time(new_estimate_timedelta.total_seconds())
        
        
class TestSchedulingInputFactory(object):
    
    def test_constructor(self):
        input_provider = Mock()
        factory = SchedulingInputFactory(input_provider)
        assert_equal(input_provider, factory.input_provider)
        
        
    def test_create_too_scheduling(self):
        input_provider = Mock()
        factory = SchedulingInputFactory(input_provider)
        factory.create_too_scheduling_input(100, output_path=None)
        assert_equal(1, input_provider.set_too_mode.call_count)
        assert_equal(1, input_provider.set_too_run_time.call_count)
        assert_equal(100, input_provider.set_too_run_time.call_args[0][0])
        
    
    def test_create_too_scheduling_no_estimate_provided(self):
        input_provider = Mock()
        factory = SchedulingInputFactory(input_provider)
        factory.create_too_scheduling_input(output_path=None)
        assert_equal(1, input_provider.set_too_mode.call_count)
        assert_equal(0, input_provider.set_too_run_time.call_count)
        
        
    def test_create_normal_scheduling(self):
        input_provider = Mock()
        factory = SchedulingInputFactory(input_provider)
        factory.create_normal_scheduling_input(600, output_path=None)
        assert_equal(1, input_provider.set_normal_mode.call_count)
        assert_equal(1, input_provider.set_normal_run_time.call_count)
        assert_equal(600, input_provider.set_normal_run_time.call_args[0][0])
        
        
    def test_create_normal_scheduling_no_estimate_provided(self):
        input_provider = Mock()
        factory = SchedulingInputFactory(input_provider)
        factory.create_normal_scheduling_input(output_path=None)
        assert_equal(1, input_provider.set_normal_mode.call_count)
        assert_equal(0, input_provider.set_normal_run_time.call_count)
        
        