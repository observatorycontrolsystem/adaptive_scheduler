from adaptive_scheduler.model2           import ModelBuilder, RequestError, n_base_requests
from adaptive_scheduler.utils            import iso_string_to_datetime
from adaptive_scheduler.utils            import timeit, metric_timer, SendMetricMixin, get_reservation_datetimes
from adaptive_scheduler.valhalla_connections import ValhallaConnectionError

import os
import logging
import pickle
from datetime import datetime, timedelta


class SchedulingInputException(Exception):
    pass


class SchedulerParameters(object):

    def __init__(self, dry_run=False, run_once=False, no_weather=False,
                 no_singles=False, no_compounds=False, no_too=False,
                 timelimit_seconds=None, slicesize_seconds=300,
                 horizon_days=7.0, sleep_seconds=60, simulate_now=None,
                 kernel='gurobi', input_file_name=None, pickle=False,
                 too_run_time=120, normal_run_time=360,
                 es_endpoint=None, save_output=False, request_logs=False,
                 pond_port=12345, pond_host='scheduler.lco.gtn',
                 valhalla_url='http://valhalla.lco.gtn/',
                 configdb_url='http://configdb.lco.gtn/',
                 downtime_url='http://downtime.lco.gtn',
                 profiling_enabled=False, ignore_ipp=False, avg_reservation_save_time_seconds=0.05,
                 normal_runtime_seconds=360.0, too_runtime_seconds=120, debug=False):
        self.dry_run = dry_run
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
        self.pickle = pickle
        self.save_output = save_output
        self.request_logs = request_logs
        self.too_run_time = too_run_time
        self.normal_run_time = normal_run_time
        self.pond_port = pond_port
        self.pond_host = pond_host
        self.profiling_enabled = profiling_enabled
        self.avg_reservation_save_time_seconds = avg_reservation_save_time_seconds
        self.normal_runtime_seconds = normal_runtime_seconds
        self.too_runtime_seconds = too_runtime_seconds
        self.ignore_ipp = ignore_ipp
        self.es_endpoint = es_endpoint
        self.debug = debug
        self.valhalla_url = valhalla_url
        self.configdb_url = configdb_url
        self.downtime_url = downtime_url


class SchedulingInputFactory(object):

    def __init__(self, input_provider):
        self.input_provider = input_provider
        self.model_builder = None
        self._scheduler_model_normal_user_requests = []
        self._scheduler_model_too_user_requests = []
        self._invalid_requests = []
        self._invalid_user_requests = []


    def _convert_json_user_requests_to_scheduler_model(self, scheduled_requests_by_ur):
        self.model_builder = self.input_provider.get_model_builder()
        utils = SchedulingInputUtils(self.model_builder)
        ignore_ipp = False
        if self.input_provider.sched_params.ignore_ipp:
            ignore_ipp = self.input_provider.sched_params.ignore_ipp
        scheduler_model_urs, invalid_user_requests, invalid_requests = utils.json_urs_to_scheduler_model_urs(
            self.input_provider.json_user_request_list, scheduled_requests_by_ur, ignore_ipp=ignore_ipp)

        self._invalid_user_requests = invalid_user_requests
        self._invalid_requests = invalid_requests
        scheduler_models_urs_by_type = utils.sort_scheduler_models_urs_by_type(scheduler_model_urs)
        self._scheduler_model_too_user_requests = scheduler_models_urs_by_type['too']
        self._scheduler_model_normal_user_requests = scheduler_models_urs_by_type['normal']


    def _set_model_user_requests_scheduled_set(self, scheduled_requests_by_ur):
        for ur in self._scheduler_model_normal_user_requests:
            if ur.tracking_number in scheduled_requests_by_ur:
                ur.set_scheduled_requests(scheduled_requests_by_ur[ur.tracking_number])


    def _create_scheduling_input(self, input_provider, is_too_input, output_path=None, block_schedule = {}):
        scheduler_input = SchedulingInput(input_provider.sched_params,
                        input_provider.scheduler_now,
                        input_provider.estimated_scheduler_runtime(),
                        input_provider.json_user_request_list,
                        input_provider.resource_usage_snapshot,
                        self.model_builder,
                        input_provider.available_resources,
                        is_too_input,
                        normal_model_user_requests=self._scheduler_model_normal_user_requests,
                        too_model_user_requests=self._scheduler_model_too_user_requests,
                        block_schedule=block_schedule)
        if output_path and input_provider.sched_params.pickle:
            file_timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
            filename = os.path.join(output_path, 'normal_scheduling_input_%s.pickle')
            if is_too_input:
                filename = os.path.join(output_path, 'too_scheduling_input_%s.pickle')
            filename = filename % file_timestamp
            scheduler_input.write_input_to_file(filename)

        return scheduler_input

    @timeit
    @metric_timer('create_scheduling_input', num_requests=lambda x: n_base_requests(x.too_user_requests))
    def create_too_scheduling_input(self, estimated_scheduling_seconds=None,
                                    output_path='/data/adaptive_scheduler/input_states/',
                                    scheduled_requests_by_ur={},
                                    network_state_timestamp=None):
        if network_state_timestamp is None:
            network_state_timestamp = datetime.utcnow()
        
        if estimated_scheduling_seconds:
            self.input_provider.set_too_run_time(estimated_scheduling_seconds)
        
        self.input_provider.set_last_known_state(network_state_timestamp)
        self.input_provider.set_too_mode()
        self._convert_json_user_requests_to_scheduler_model(scheduled_requests_by_ur)

        return self._create_scheduling_input(self.input_provider, True, output_path)


    @timeit
    @metric_timer('create_scheduling_input', num_requests=lambda x: n_base_requests(x.normal_user_requests))
    def create_normal_scheduling_input(self, estimated_scheduling_seconds=None,
                                       output_path='/data/adaptive_scheduler/input_states/',
                                       scheduled_requests_by_ur={},
                                       too_schedule={},
                                       network_state_timestamp=None):
        if network_state_timestamp is None:
            network_state_timestamp = datetime.utcnow()
            
        if estimated_scheduling_seconds:
            self.input_provider.set_normal_run_time(estimated_scheduling_seconds)
            
        self.input_provider.set_last_known_state(network_state_timestamp)
        self.input_provider.set_normal_mode()
        self._set_model_user_requests_scheduled_set(scheduled_requests_by_ur)

        return self._create_scheduling_input(self.input_provider, False, output_path,
                                             block_schedule=too_schedule)


class SchedulingInputUtils(object, SendMetricMixin):

    def __init__(self, model_builder):
        self.model_builder = model_builder
        self.log = logging.getLogger(__name__)

    @timeit
    def json_urs_to_scheduler_model_urs(self, json_user_request_list, scheduled_requests_by_ur={}, ignore_ipp=False):
        scheduler_model_urs = []
        invalid_json_user_requests = []
        invalid_json_requests = []
        for json_ur in json_user_request_list:
            try:
                scheduled_requests = {}
                if json_ur['id'] in scheduled_requests_by_ur:
                    scheduled_requests = scheduled_requests_by_ur[json_ur['id']]
                scheduler_model_ur, invalid_children = self.model_builder.build_user_request(json_ur, scheduled_requests, ignore_ipp=ignore_ipp)

                scheduler_model_urs.append(scheduler_model_ur)
                invalid_json_requests.extend(invalid_children)
            except RequestError as e:
                self.log.warn(e)
                invalid_json_user_requests.append(json_ur)

        self.send_metric('invalid_child_requests.num_requests', len(invalid_json_requests))
        self.send_metric('invalid_user_requests.num_requests', len(invalid_json_user_requests))

        return scheduler_model_urs, invalid_json_user_requests, invalid_json_requests


    def sort_scheduler_models_urs_by_type(self, scheduler_model_user_requests):
        scheduler_models_urs_by_type = {
                                        'too' : [],
                                        'normal' : []
                                        }
        for scheduler_model_ur in scheduler_model_user_requests:
            if scheduler_model_ur.is_target_of_opportunity():
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

    def __init__(self, sched_params, scheduler_now, estimated_scheduler_runtime, json_user_request_list,
                 resource_usage_snapshot, model_builder, available_resources, is_too_input,
                 normal_model_user_requests=[], too_model_user_requests=[], block_schedule = {}):
        self.sched_params = sched_params
        self.scheduler_now = scheduler_now
        self.estimated_scheduler_runtime = estimated_scheduler_runtime
        self.json_user_request_list = json_user_request_list
        self.resource_usage_snapshot = resource_usage_snapshot
        self.available_resources = available_resources
        self.is_too_input = is_too_input
        self.model_builder = model_builder
        self.block_schedule = block_schedule

        self._scheduler_model_too_user_requests = too_model_user_requests
        self._scheduler_model_normal_user_requests = normal_model_user_requests


    def get_scheduling_start(self):
        if self.sched_params.input_file_name or self.sched_params.simulate_now:
            return self.scheduler_now
        return datetime.utcnow()


    def get_block_schedule_by_resource(self):
        block_schedule_by_resource = {}
        semester_start = self.model_builder.semester_details['start']

        for resource, reservations in self.block_schedule.items():
            block_schedule_by_resource[resource] = []
            for reservation in reservations:
                reservation_start, reservation_end = get_reservation_datetimes(reservation, semester_start)
                block_schedule_by_resource[resource].append((reservation_start, reservation_end))

        return block_schedule_by_resource


    @property
    def user_requests(self):
        if self.is_too_input:
            return self.too_user_requests
        else:
            return self.normal_user_requests


    @property
    def too_user_requests(self):
        return self._scheduler_model_too_user_requests


    @property
    def normal_user_requests(self):
        return self._scheduler_model_normal_user_requests


    @property
    def too_tracking_numbers(self):
        return [ur.tracking_number for ur in self.too_user_requests] 


    def write_input_to_file(self, filename):
        output = {
                  'sched_params' : self.sched_params,
                  'scheduler_now' : self.scheduler_now,
                  'estimated_scheduler_runtime' : self.estimated_scheduler_runtime,
                  'json_user_request_list' : self.json_user_request_list,
                  'resource_usage_snapshot' : self.resource_usage_snapshot,
                  'available_resources' : self.available_resources,
                  'is_too_input' : self.is_too_input,
                  'proposals_by_id': self.model_builder.proposals_by_id,
                  'semester_details': self.model_builder.semester_details
                  }
        outfile = open(filename, 'w')
        try:
            pickle.dump(output, outfile)
        except pickle.PickleError, pe:
            print pe
        outfile.close()


class SchedulingInputProvider(object):

    def __init__(self, sched_params, network_interface, network_model, is_too_input=False):
        self.sched_params = sched_params
        self.network_interface = network_interface
        self.network_model = network_model
        self.is_too_input = is_too_input
        self.estimated_too_run_time = timedelta(seconds=self.sched_params.too_run_time)
        self.estimated_normal_run_time = timedelta(seconds=self.sched_params.normal_run_time)

        # TODO: Hide these behind read only properties
        self.scheduler_now = None
        self.json_user_request_list = None
        self.available_resources = None
        self.resource_usage_snapshot = None
        self.last_known_state_timestamp = None


    def refresh(self):
        # The order of these is important
        self.scheduler_now = self.get_scheduler_now()
        if self.is_too_input:
            # only get all the schedulable requests when we are in the too loop. re-use them for the normal loop.
            self.json_user_request_list = self._get_json_user_request_list()
        self.available_resources = self._get_available_resources()
        self.resource_usage_snapshot = self._get_resource_usage_snapshot()


    def set_too_run_time(self, seconds):
        self.estimated_too_run_time = timedelta(seconds=seconds)


    def set_normal_run_time(self, seconds):
        self.estimated_normal_run_time = timedelta(seconds=seconds)


    def set_too_mode(self):
        self.is_too_input = True
        self.refresh()


    def set_normal_mode(self):
        self.is_too_input = False
        self.refresh()


    def set_last_known_state(self, timestamp):
        self.last_known_state_timestamp = timestamp


    def estimated_scheduler_runtime(self):
        if self.is_too_input:
            return self.estimated_too_run_time
        else:
            return self.estimated_normal_run_time


    def get_scheduler_now(self):
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
        now = self.get_scheduler_now()
        if self.is_too_input:
            # TODO: Might Add a pad to account for time between now and when scheduling actually starts
            return now + self.estimated_too_run_time
        else:
            # TODO: Might Add a pad to account for time between now and when scheduling actually starts
            return now + self.estimated_normal_run_time


    def _get_json_user_request_list(self):
        now = self.get_scheduler_now()
        try:
            semester_details = self.network_interface.valhalla_interface.get_semester_details(self._get_estimated_scheduler_end())
        except ValhallaConnectionError as e:
            raise SchedulingInputException("Can't retrieve current semester to get user requests.")
        ur_list = self.network_interface.get_all_user_requests(semester_details['start'],
                                                               min(now + timedelta(days=self.sched_params.horizon_days),
                                                               semester_details['end']))
        logging.getLogger(__name__).warning("_get_json_user_request_list got {} urs".format(len(ur_list)))

        return ur_list



    def _get_available_resources(self):
        resources = []
        for resource_name, resource in self.network_model.iteritems():
            if not resource['events']:
                resources.append(resource_name)

        return resources


    def _all_resources(self):
        return self.network_model.keys()


    def _get_resource_usage_snapshot(self):
        snapshot_start = self.last_known_state_timestamp if self.last_known_state_timestamp else self.scheduler_now
        snapshot = self.network_interface.resource_usage_snapshot(self._all_resources(), snapshot_start, self._get_estimated_scheduler_end())

        return snapshot


    def get_model_builder(self):
        mb = ModelBuilder(self.network_interface.valhalla_interface,
                          self.network_interface.configdb_interface)

        return mb


class FileBasedSchedulingInputProvider(object):

    def __init__(self, too_input_file, normal_input_file, network_interface, is_too_mode):
        self.too_input_file = too_input_file
        self.normal_input_file = normal_input_file
        self.is_too_input = is_too_mode

        self.sched_params = None
        self.scheduler_now = None
        self._estimated_scheduler_runtime = None
        self.json_user_request_list = None
        self.available_resources = None
        self.resource_usage_snapshot = None
        self.last_known_state_timestamp = None
        self.network_interface = network_interface
        self.semester_details = None
        self.proposals_by_id = {}

        self.refresh()


    def set_too_run_time(self, seconds):
        # Do nothing, we want to use whatever came from the input file
        pass


    def set_normal_run_time(self, seconds):
        # Do nothing, we want to use whatever came from the input file
        pass
    
    def set_last_known_state(self, timestamp):
        # Do nothing, we want to use whatever came from the input file
        pass
    
    def estimated_scheduler_runtime(self):
        return self._estimated_scheduler_runtime


    def refresh(self):
        input_filename = self.normal_input_file
        if self.is_too_input:
            input_filename = self.too_input_file
        input_file = open(input_filename, 'r')
        pickle_input = pickle.load(input_file)
        input_file.close()

        self.sched_params = pickle_input['sched_params']
        # set the filename of the current sched_params to this input filename. I don't see any reason to maintain
        # a separate current filename and 'input' filename.
        self.sched_params.input_file_name = input_filename
        self.scheduler_now = pickle_input['scheduler_now']
        self._estimated_scheduler_runtime = pickle_input['estimated_scheduler_runtime']
        self.json_user_request_list = pickle_input['json_user_request_list']
        self.available_resources = pickle_input['available_resources']
        self.resource_usage_snapshot = pickle_input['resource_usage_snapshot']
        self.semester_details = pickle_input.get('semester_details')
        self.proposals_by_id = pickle_input.get('proposals_by_id', {})


    def set_too_mode(self):
        self.is_too_input = True
        self.refresh()


    def set_normal_mode(self):
        self.is_too_input = False
        self.refresh()

    def get_scheduler_now(self):
        return self.scheduler_now

    def get_model_builder(self):
        mb = ModelBuilder(None,
                          self.network_interface.configdb_interface,
                          proposals_by_id=self.proposals_by_id,
                          semester_details=self.semester_details)

        return mb
