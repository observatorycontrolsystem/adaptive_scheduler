from adaptive_scheduler.model2           import ModelBuilder, RequestError
from adaptive_scheduler.utils            import iso_string_to_datetime
from schedutils.semester_service         import get_semester_block
from adaptive_scheduler.utils            import timeit

import os
import logging
import pickle
from datetime import datetime, timedelta


class SchedulingInputException(Exception):
    pass


class SchedulerParameters(object):

    def __init__(self, dry_run=False, run_once=False,
                 telescopes_file='telescopes.dat',
                 cameras_file='camera_mappings.dat', no_weather=False,
                 no_singles=False, no_compounds=False, no_too=False,
                 timelimit_seconds=None, slicesize_seconds=300,
                 horizon_days=7.0, sleep_seconds=60, simulate_now=None,
                 kernel='gurobi', input_file_name=None,
                 too_run_time=120, normal_run_time=360,
                 pond_port=12345, pond_host='scheduler.lco.gtn',
                 profiling_enabled=False, avg_reservation_save_time_seconds=0.05,
                 normal_runtime_seconds=360.0, too_runtime_seconds=120):
        self.dry_run = dry_run
        self.telescopes_file = telescopes_file
        self.cameras_file = cameras_file
        self.no_weather = no_weather
        self.no_singles = no_singles
        self.no_compounds = no_compounds
        self.no_too = no_too
        self.timelimit_seconds = timelimit_seconds
        self.slicesize_seconds = slicesize_seconds
        self.horizon_days = horizon_days
        self.run_once = run_once
        self.sleep_seconds = sleep_seconds
        self.simulate_now = simulate_now
        self.kernel = kernel
        self.input_file_name = input_file_name
        self.too_run_time = too_run_time
        self.normal_run_time = normal_run_time
        self.pond_port = pond_port
        self.pond_host = pond_host
        self.profiling_enabled = profiling_enabled
        self.avg_reservation_save_time_seconds = avg_reservation_save_time_seconds
        self.normal_runtime_seconds = normal_runtime_seconds
        self.too_runtime_seconds = too_runtime_seconds


    def get_model_builder(self):
        mb = ModelBuilder(self.telescopes_file, self.cameras_file)

        return mb


class RequestDBSchedulerParameters(SchedulerParameters):

    def __init__(self, requestdb, **kwargs):
        SchedulerParameters.__init__(self, **kwargs)
        self.requestdb_url = requestdb


class SchedulingInputFactory(object):

    def __init__(self, input_provider):
        self.input_provider = input_provider


    def _create_scheduling_input(self, input_provider, is_too_input, output_path=None):
        scheduler_input = SchedulingInput(input_provider.sched_params,
                        input_provider.scheduler_now,
                        input_provider.estimated_scheduler_end,
                        input_provider.json_user_request_list,
                        input_provider.resource_usage_snapshot,
                        input_provider.available_resources,
                        is_too_input)
        if output_path:
            file_timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
            filename = os.path.join(output_path, 'normal_scheduling_input_%s.pickle')
            if is_too_input:
                filename = os.path.join(output_path, 'too_scheduling_input_%s.pickle')
            filename = filename % file_timestamp
            scheduler_input.write_input_to_file(filename)

        return scheduler_input

    @timeit
    def create_too_scheduling_input(self, estimated_scheduling_seconds=None, output_path='/data/adaptive_scheduler/input_states/'):
        if estimated_scheduling_seconds:
            self.input_provider.set_too_run_time(estimated_scheduling_seconds)
        self.input_provider.set_too_mode()

        return self._create_scheduling_input(self.input_provider, True, output_path)


    @timeit
    def create_normal_scheduling_input(self, estimated_scheduling_seconds=None, output_path='/data/adaptive_scheduler/input_states/'):
        if estimated_scheduling_seconds:
            self.input_provider.set_normal_run_time(estimated_scheduling_seconds)
        self.input_provider.set_normal_mode()

        return self._create_scheduling_input(self.input_provider, False, output_path)


class SchedulingInputUtils(object):

    def __init__(self, model_builder):
        self.model_builder = model_builder
        self.log = logging.getLogger(__name__)


    def json_urs_to_scheduler_model_urs(self, json_user_request_list):
        scheduler_model_urs = []
        invalid_json_user_requests = []
        invalid_json_requests = []
        for json_ur in json_user_request_list:
            try:
                scheduler_model_ur, invalid_children = self.model_builder.build_user_request(json_ur)
                scheduler_model_urs.append(scheduler_model_ur)
                invalid_json_requests.extend(invalid_children)
            except RequestError as e:
                self.log.warn(e)
                invalid_json_user_requests.append(json_ur)

        return scheduler_model_urs, invalid_json_user_requests, invalid_json_requests


    def sort_scheduler_models_urs_by_type(self, scheduler_model_user_requests):
        scheduler_models_urs_by_type = {
                                        'too' : [],
                                        'normal' : []
                                        }
        for scheduler_model_ur in scheduler_model_user_requests:
            if scheduler_model_ur.has_target_of_opportunity():
                scheduler_models_urs_by_type['too'].append(scheduler_model_ur)
            else:
                scheduler_models_urs_by_type['normal'].append(scheduler_model_ur)

        return scheduler_models_urs_by_type


    def user_request_priorities(self, scheduler_model_user_requests):
        priorities_map = {ur.tracking_number : ur.get_priority() for ur in scheduler_model_user_requests}

        return priorities_map


    def too_tracking_numbers(self, scheduler_model_user_requests):
        scheduler_models_urs_by_type = self.sort_scheduler_models_urs_by_type(scheduler_model_user_requests)
        too_tracking_numbers = [ur.tracking_number for ur in scheduler_models_urs_by_type['too']]

        return too_tracking_numbers



class SchedulingInput(object):

    def __init__(self, sched_params, scheduler_now, estimated_scheduler_end, json_user_request_list, resource_usage_snapshot, available_resources, is_too_input):
        self.sched_params = sched_params
        self.scheduler_now = scheduler_now
        self.estimated_scheduler_end = estimated_scheduler_end
        self.json_user_request_list = json_user_request_list
        self.resource_usage_snapshot = resource_usage_snapshot
        self.available_resources = available_resources
        self.is_too_input = is_too_input
        self.utils = SchedulingInputUtils(sched_params.get_model_builder())

        self._scheduler_model_too_user_requests = None
        self._scheduler_model_normal_user_requests = None
        self._invalid_user_requests = []
        self._invalid_requests = []


    def _convert_json_user_requests_to_scheduler_model(self):
        scheduler_model_urs, invalid_user_requests, invalid_requests = self.utils.json_urs_to_scheduler_model_urs(self.json_user_request_list)
        self._invalid_user_requests = invalid_user_requests
        self._invalid_requests = invalid_requests
        scheduler_models_urs_by_type = self.utils.sort_scheduler_models_urs_by_type(scheduler_model_urs)
        self._scheduler_model_too_user_requests = scheduler_models_urs_by_type['too']
        self._scheduler_model_normal_user_requests = scheduler_models_urs_by_type['normal']


    @property
    def user_requests(self):
        if self.is_too_input:
            return self.too_user_requests
        else:
            return self.normal_user_requests


    @property
    def too_user_requests(self):
        if(self._scheduler_model_too_user_requests == None):
            self._convert_json_user_requests_to_scheduler_model()

        return self._scheduler_model_too_user_requests


    @property
    def normal_user_requests(self):
        if(self._scheduler_model_normal_user_requests == None):
            self._convert_json_user_requests_to_scheduler_model()

        return self._scheduler_model_normal_user_requests


    @property
    def user_request_priorities(self):
        priorities = {}
        priorities.update(self.utils.user_request_priorities(self.too_user_requests))
        priorities.update(self.utils.user_request_priorities(self.normal_user_requests))

        return priorities


    @property
    def too_tracking_numbers(self):
        return self.utils.too_tracking_numbers(self.too_user_requests)


    @property
    def invalid_request_numbers(self):
        return [r.request_number for r in self._invalid_requests]


    @property
    def invalid_tracking_numbers(self):
        return [ur.tracking_number for ur in self._invalid_user_requests]


    def write_input_to_file(self, filename):
        output = {
                  'sched_params' : self.sched_params,
                  'scheduler_now' : self.scheduler_now,
                  'estimated_scheduler_end' : self.estimated_scheduler_end,
                  'json_user_request_list' : self.json_user_request_list,
                  'resource_usage_snapshot' : self.resource_usage_snapshot,
                  'available_resources' : self.available_resources,
                  'is_too_input' : self.is_too_input
                  }
        outfile = open(filename, 'w')
        try:
            pickle.dump(output, outfile)
        except pickle.PickleError, pe:
            print pe
        outfile.close()


    @staticmethod
    def read_from_file(self, filename):
        infile = open(filename, 'r')
        input_from_file = pickle.load(infile)

        return SchedulingInput(**input_from_file)


class SchedulingInputProvider(object):

    def __init__(self, sched_params, network_interface, network_model, is_too_input=False):
        self.sched_params = sched_params
        self.network_interface = network_interface
        self.network_model = network_model
        self.is_too_input = is_too_input
        self.utils = SchedulingInputUtils(sched_params.get_model_builder())

        self._estimated_too_run_time = timedelta(seconds=self.sched_params.too_run_time)
        self._estimated_normal_run_time = timedelta(seconds=self.sched_params.normal_run_time)

        # TODO: Hide these behind read only properties
        self.scheduler_now = None
        self.estimated_scheduler_end = None
        self.json_user_request_list = None
        self.available_resources = None
        self.resource_usage_snapshot = None


    def refresh(self):
        # The order of these is important
        self.scheduler_now = self._get_scheduler_now()
        self.estimated_scheduler_end = self._get_estimated_scheduler_end()
        self.json_user_request_list = self._get_json_user_request_list()
        self.available_resources = self._get_available_resources()
        self.resource_usage_snapshot = self._get_resource_usage_snapshot()


    def set_too_run_time(self, seconds):
        self._estimated_too_run_time = timedelta(seconds=seconds)


    def set_normal_run_time(self, seconds):
        self._estimated_normal_run_time = timedelta(seconds=seconds)


    def set_too_mode(self):
        self.is_too_input = True
        self.refresh()


    def set_normal_mode(self):
        self.is_too_input = False
        self.refresh()


    def _get_scheduler_now(self):
        '''Use a static command line datetime if provided, or default to utcnow, with a
           little extra to cover the scheduler's run time.'''
        if self.sched_params.simulate_now:
            try:
                now = iso_string_to_datetime(self.sched_params.simulate_now)
            except ValueError as e:
                raise SchedulingInputException("Invalid datetime provided on command line. Try e.g. '2012-03-03 09:05:00'.")
        # ...otherwise offset 'now' to account for the duration of the scheduling run
        else:
            now = datetime.utcnow()
        return now


    def _get_estimated_scheduler_end(self):
        if self.is_too_input:
            return self.scheduler_now + self._estimated_too_run_time
        else:
            return self.scheduler_now + self._estimated_normal_run_time


    def _get_json_user_request_list(self):
        semester_start, semester_end = get_semester_block(dt=self.estimated_scheduler_end)
        ur_list = self.network_interface.get_all_user_requests(semester_start, semester_end)

        return ur_list


    def _get_available_resources(self):
        resources = []
        for resource_name, resource in self.network_model.iteritems():
            if not resource.events:
                resources.append(resource_name)

        return resources


    def _all_resources(self):
        return self.network_model.keys()


    def _get_resource_usage_snapshot(self):
        snapshot = self.network_interface.resource_usage_snapshot(self._all_resources(), self.scheduler_now, self.estimated_scheduler_end)

        return snapshot


class FileBasedSchedulingInputProvider(object):

    def __init__(self, too_input_file, normal_input_file, is_too_mode):
        self.too_input_file = too_input_file
        self.normal_input_file = normal_input_file
        self.is_too_input = is_too_mode

        self.sched_params = None
        self.scheduler_now = None
        self.estimated_scheduler_end = None
        self.json_user_request_list = None
        self.available_resources = None
        self.resource_usage_snapshot = None

        self.refresh()


    def set_too_run_time(self, seconds):
        # Do nothing, we want to use whatever came from the input file
        pass


    def set_normal_run_time(self, seconds):
        # Do nothing, we want to use whatever came from the input file
        pass


    def refresh(self):
        input_filename = self.normal_input_file
        if self.is_too_input:
            input_filename = self.too_input_file
        input_file = open(input_filename, 'r')
        pickle_input = pickle.load(input_file)
        input_file.close()

        self.sched_params = pickle_input['sched_params']
        self.scheduler_now = pickle_input['scheduler_now']
        self.estimated_scheduler_end = pickle_input['estimated_scheduler_end']
        self.json_user_request_list = pickle_input['json_user_request_list']
        self.available_resources = pickle_input['available_resources']
        self.resource_usage_snapshot = pickle_input['resource_usage_snapshot']


    def set_too_mode(self):
        self.is_too_input = True
        self.refresh()


    def set_normal_mode(self):
        self.is_too_input = False
        self.refresh()

