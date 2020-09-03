from adaptive_scheduler.models import ModelBuilder, RequestError, n_base_requests
from adaptive_scheduler.utils import iso_string_to_datetime
from adaptive_scheduler.utils import timeit, metric_timer, SendMetricMixin, get_reservation_datetimes
from adaptive_scheduler.observation_portal_connections import ObservationPortalConnectionError

import os
import logging
import pickle
from datetime import datetime, timedelta


class SchedulingInputException(Exception):
    pass


class SchedulerParameters(object):

    def __init__(self, dry_run=False, run_once=False, no_weather=False,
                 no_singles=False, no_compounds=False, no_rr=False,
                 timelimit_seconds=None, slicesize_seconds=300,
                 horizon_days=7.0, sleep_seconds=60, simulate_now=None,
                 kernel='gurobi', input_file_name=None, pickle=False,
                 rr_run_time=120, normal_run_time=360,
                 save_output=False, request_logs=False,
                 observation_portal_url='http://127.0.0.1:8000',
                 configdb_url='http://127.0.0.1:7000',
                 downtime_url='http://127.0.0.1:7500',
                 elasticsearch_url='',
                 elasticsearch_index='live-telemetry',
                 elasticsearch_excluded_observatories='',
                 profiling_enabled=False, ignore_ipp=False, avg_reservation_save_time_seconds=0.05,
                 normal_runtime_seconds=360.0, rr_runtime_seconds=120.0, debug=False):
        self.dry_run = dry_run
        self.no_weather = no_weather
        self.no_singles = no_singles
        self.no_compounds = no_compounds
        self.no_rr = no_rr
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
        self.rr_run_time = rr_run_time
        self.normal_run_time = normal_run_time
        self.profiling_enabled = profiling_enabled
        self.avg_reservation_save_time_seconds = avg_reservation_save_time_seconds
        self.normal_runtime_seconds = normal_runtime_seconds
        self.rr_runtime_seconds = rr_runtime_seconds
        self.ignore_ipp = ignore_ipp
        self.debug = debug
        self.observation_portal_url = observation_portal_url
        self.configdb_url = configdb_url
        self.downtime_url = downtime_url
        self.elasticsearch_url = elasticsearch_url
        self.elasticsearch_index = elasticsearch_index
        if elasticsearch_excluded_observatories:
            self.elasticsearch_excluded_observatories = elasticsearch_excluded_observatories.split(',')
        else:
            self.elasticsearch_excluded_observatories = []


class SchedulingInputFactory(object):

    def __init__(self, input_provider):
        self.input_provider = input_provider
        self.model_builder = None
        self._scheduler_model_normal_request_groups = []
        self._scheduler_model_rr_request_groups = []
        self._invalid_requests = []
        self._invalid_request_groups = []

    def _convert_json_request_groups_to_scheduler_model(self, scheduled_requests_by_rg):
        self.model_builder = self.input_provider.get_model_builder()
        utils = SchedulingInputUtils(self.model_builder)
        ignore_ipp = False
        if self.input_provider.sched_params.ignore_ipp:
            ignore_ipp = self.input_provider.sched_params.ignore_ipp
        scheduler_model_rgs, invalid_request_groups, invalid_requests = utils.json_rgs_to_scheduler_model_rgs(
            self.input_provider.json_request_group_list, scheduled_requests_by_rg, ignore_ipp=ignore_ipp)

        self._invalid_request_groups = invalid_request_groups
        self._invalid_requests = invalid_requests
        scheduler_models_rgs_by_type = utils.sort_scheduler_models_rgs_by_type(scheduler_model_rgs)
        self._scheduler_model_rr_request_groups = scheduler_models_rgs_by_type['rr']
        self._scheduler_model_normal_request_groups = scheduler_models_rgs_by_type['normal']

    def _set_model_request_groups_scheduled_set(self, scheduled_requests_by_rg):
        for rg in self._scheduler_model_normal_request_groups:
            if rg.id in scheduled_requests_by_rg:
                rg.set_scheduled_reservations(scheduled_requests_by_rg[rg.id])

    def _create_scheduling_input(self, input_provider, is_rr_input, block_schedule=None):
        if not block_schedule:
            block_schedule = {}
        scheduler_input = SchedulingInput(input_provider.sched_params,
                                          input_provider.scheduler_now,
                                          input_provider.estimated_scheduler_runtime(),
                                          input_provider.json_request_group_list,
                                          input_provider.resource_usage_snapshot,
                                          self.model_builder,
                                          input_provider.available_resources,
                                          is_rr_input,
                                          normal_model_request_groups=self._scheduler_model_normal_request_groups,
                                          rr_model_request_groups=self._scheduler_model_rr_request_groups,
                                          block_schedule=block_schedule)

        return scheduler_input

    @timeit
    @metric_timer('create_scheduling_input', num_requests=lambda x: n_base_requests(x.rr_request_groups))
    def create_rr_scheduling_input(self, estimated_scheduling_seconds=None,
                                   scheduled_requests_by_rg=None,
                                   network_state_timestamp=None):
        if network_state_timestamp is None:
            network_state_timestamp = datetime.utcnow()
        if scheduled_requests_by_rg is None:
            scheduled_requests_by_rg = {}

        if estimated_scheduling_seconds:
            self.input_provider.set_rr_run_time(estimated_scheduling_seconds)

        self.input_provider.set_last_known_state(network_state_timestamp)
        self.input_provider.set_rr_mode()
        self._convert_json_request_groups_to_scheduler_model(scheduled_requests_by_rg)

        return self._create_scheduling_input(self.input_provider, True)

    @timeit
    @metric_timer('create_scheduling_input', num_requests=lambda x: n_base_requests(x.normal_request_groups))
    def create_normal_scheduling_input(self, estimated_scheduling_seconds=None,
                                       scheduled_requests_by_rg=None,
                                       rr_schedule=None,
                                       network_state_timestamp=None):
        if network_state_timestamp is None:
            network_state_timestamp = datetime.utcnow()
        if scheduled_requests_by_rg is None:
            scheduled_requests_by_rg = {}
        if rr_schedule is None:
            rr_schedule = {}

        # Save off the rr parameters to save in the input file
        rr_scheduler_now = self.input_provider.scheduler_now
        rr_resource_usage_snapshot = self.input_provider.resource_usage_snapshot
        rr_estimated_runtime = self.input_provider.estimated_scheduler_runtime()

        if estimated_scheduling_seconds:
            self.input_provider.set_normal_run_time(estimated_scheduling_seconds)

        self.input_provider.set_last_known_state(network_state_timestamp)
        self.input_provider.set_normal_mode()
        self._set_model_request_groups_scheduled_set(scheduled_requests_by_rg)

        if self.input_provider.sched_params.pickle:
            SchedulingInputUtils.write_input_to_file(self.input_provider, rr_scheduler_now,
                                                     rr_resource_usage_snapshot, rr_estimated_runtime,
                                                     self.model_builder)

        return self._create_scheduling_input(self.input_provider, False, block_schedule=rr_schedule)


class SchedulingInputUtils(SendMetricMixin):

    def __init__(self, model_builder):
        self.model_builder = model_builder
        self.log = logging.getLogger(__name__)

    @timeit
    def json_rgs_to_scheduler_model_rgs(self, json_request_group_list, scheduled_requests_by_rg=None, ignore_ipp=False):
        if scheduled_requests_by_rg is None:
            scheduled_requests_by_rg = {}
        scheduler_model_rgs = []
        invalid_json_request_groups = []
        invalid_json_requests = []
        for json_rg in json_request_group_list:
            try:
                scheduled_requests = {}
                if json_rg['id'] in scheduled_requests_by_rg:
                    scheduled_requests = scheduled_requests_by_rg[json_rg['id']]
                scheduler_model_rg, invalid_children = self.model_builder.build_request_group(json_rg,
                                                                                              scheduled_requests,
                                                                                              ignore_ipp=ignore_ipp)

                scheduler_model_rgs.append(scheduler_model_rg)
                invalid_json_requests.extend(invalid_children)
            except RequestError as e:
                self.log.warn(e)
                invalid_json_request_groups.append(json_rg)

        self.send_metric('invalid_child_requests.num_requests', len(invalid_json_requests))
        self.send_metric('invalid_request_groups.num_requests', len(invalid_json_request_groups))

        return scheduler_model_rgs, invalid_json_request_groups, invalid_json_requests

    def sort_scheduler_models_rgs_by_type(self, scheduler_model_request_groups):
        scheduler_models_rgs_by_type = {
            'rr': [],
            'normal': []
        }
        for scheduler_model_rg in scheduler_model_request_groups:
            if scheduler_model_rg.is_rapid_response():
                scheduler_models_rgs_by_type['rr'].append(scheduler_model_rg)
            else:
                scheduler_models_rgs_by_type['normal'].append(scheduler_model_rg)

        return scheduler_models_rgs_by_type

    def rapid_response_ids(self, scheduler_model_request_groups):
        scheduler_models_rgs_by_type = self.sort_scheduler_models_rgs_by_type(scheduler_model_request_groups)
        rapid_response_ids = [rg.id for rg in scheduler_models_rgs_by_type['rr']]

        return rapid_response_ids

    @staticmethod
    def write_input_to_file(normal_input_provider, rr_scheduler_now, rr_resource_usage_snapshot,
                            rr_estimated_scheduler_runtime, model_builder,
                            output_path='/data/adaptive_scheduler/input_states/'):
        output = {
            'sched_params': normal_input_provider.sched_params,
            'normal': {
                'scheduler_now': normal_input_provider.scheduler_now,
                'estimated_scheduler_runtime': normal_input_provider.estimated_scheduler_runtime(),
                'resource_usage_snapshot': normal_input_provider.resource_usage_snapshot,
            },
            'rr': {
                'scheduler_now': rr_scheduler_now,
                'estimated_scheduler_runtime': rr_estimated_scheduler_runtime,
                'resource_usage_snapshot': rr_resource_usage_snapshot,
            },
            'json_request_group_list': normal_input_provider.json_request_group_list,
            'available_resources': normal_input_provider.available_resources,
            'proposals_by_id': model_builder.proposals_by_id,
            'semester_details': model_builder.semester_details
        }
        file_timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        filename = os.path.join(output_path, 'scheduling_input_{}.pickle'.format(file_timestamp))
        outfile = open(filename, 'w')
        try:
            pickle.dump(output, outfile)
        except pickle.PickleError as pe:
            print(pe)

        outfile.close()


class SchedulingInput(object):

    def __init__(self, sched_params, scheduler_now, estimated_scheduler_runtime, json_request_group_list,
                 resource_usage_snapshot, model_builder, available_resources, is_rr_input,
                 normal_model_request_groups=None, rr_model_request_groups=None, block_schedule=None):
        self.sched_params = sched_params
        self.scheduler_now = scheduler_now
        self.estimated_scheduler_runtime = estimated_scheduler_runtime
        self.json_request_group_list = json_request_group_list
        self.resource_usage_snapshot = resource_usage_snapshot
        self.available_resources = available_resources
        self.is_rr_input = is_rr_input
        self.model_builder = model_builder
        self.block_schedule = block_schedule if block_schedule else {}

        self._scheduler_model_rr_request_groups = rr_model_request_groups if rr_model_request_groups else []
        self._scheduler_model_normal_request_groups = normal_model_request_groups if normal_model_request_groups else []

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
    def request_groups(self):
        if self.is_rr_input:
            return self.rr_request_groups
        else:
            return self.normal_request_groups

    @property
    def rr_request_groups(self):
        return self._scheduler_model_rr_request_groups

    @property
    def normal_request_groups(self):
        return self._scheduler_model_normal_request_groups

    @property
    def rr_request_group_ids(self):
        return [rg.id for rg in self.rr_request_groups]


class SchedulingInputProvider(object):

    def __init__(self, sched_params, network_interface, network_model, is_rr_input=False):
        self.sched_params = sched_params
        self.network_interface = network_interface
        self.network_model = network_model
        self.is_rr_input = is_rr_input
        self.estimated_rr_run_time = timedelta(seconds=self.sched_params.rr_run_time)
        self.estimated_normal_run_time = timedelta(seconds=self.sched_params.normal_run_time)

        # TODO: Hide these behind read only properties
        self.scheduler_now = None
        self.json_request_group_list = None
        self.available_resources = None
        self.resource_usage_snapshot = None
        self.last_known_state_timestamp = None

    def refresh(self):
        # The order of these is important
        self.scheduler_now = self.get_scheduler_now()
        if self.is_rr_input:
            # only get all the schedulable requests when we are in the rr loop. re-use them for the normal loop.
            self.json_request_group_list = self._get_json_request_group_list()
        self.available_resources = self._get_available_resources()
        self.resource_usage_snapshot = self._get_resource_usage_snapshot()

    def set_rr_run_time(self, seconds):
        self.estimated_rr_run_time = timedelta(seconds=seconds)

    def set_normal_run_time(self, seconds):
        self.estimated_normal_run_time = timedelta(seconds=seconds)

    def set_rr_mode(self):
        self.is_rr_input = True
        self.refresh()

    def set_normal_mode(self):
        self.is_rr_input = False
        self.refresh()

    def set_last_known_state(self, timestamp):
        self.last_known_state_timestamp = timestamp

    def estimated_scheduler_runtime(self):
        if self.is_rr_input:
            return self.estimated_rr_run_time
        else:
            return self.estimated_normal_run_time

    def get_scheduler_now(self):
        '''Use a static command line datetime if provided, or default to utcnow, with a
           little extra to cover the scheduler's run time.'''
        if self.sched_params.simulate_now:
            try:
                now = iso_string_to_datetime(self.sched_params.simulate_now)
            except ValueError as e:
                raise SchedulingInputException(
                    "Invalid datetime provided on command line. Try e.g. '2012-03-03 09:05:00'.")
        # ...otherwise offset 'now' to account for the duration of the scheduling run
        else:
            now = datetime.utcnow()
        return now

    def _get_estimated_scheduler_end(self):
        now = self.get_scheduler_now()
        if self.is_rr_input:
            # TODO: Might Add a pad to account for time between now and when scheduling actually starts
            return now + self.estimated_rr_run_time
        else:
            # TODO: Might Add a pad to account for time between now and when scheduling actually starts
            return now + self.estimated_normal_run_time

    def _get_json_request_group_list(self):
        now = self.get_scheduler_now()
        try:
            semester_details = self.network_interface.observation_portal_interface.get_semester_details(
                self._get_estimated_scheduler_end())
        except ObservationPortalConnectionError as e:
            raise SchedulingInputException("Can't retrieve current semester to get request groups.")
        rg_list = self.network_interface.get_all_request_groups(semester_details['start'],
                                                                min(now + timedelta(
                                                                    days=self.sched_params.horizon_days),
                                                                    semester_details['end']))
        logging.getLogger(__name__).warning("_get_json_request_group_list got {} rgs".format(len(rg_list)))

        return rg_list

    def _get_available_resources(self):
        resources = []
        for resource_name, resource in self.network_model.items():
            if not resource['events']:
                resources.append(resource_name)

        return resources

    def _all_resources(self):
        return list(self.network_model.keys())

    def _get_resource_usage_snapshot(self):
        snapshot_start = self.last_known_state_timestamp if self.last_known_state_timestamp else self.scheduler_now
        snapshot = self.network_interface.resource_usage_snapshot(self._all_resources(), snapshot_start,
                                                                  self._get_estimated_scheduler_end())

        return snapshot

    def get_model_builder(self):
        mb = ModelBuilder(self.network_interface.observation_portal_interface,
                          self.network_interface.configdb_interface)

        return mb


class FileBasedSchedulingInputProvider(object):

    def __init__(self, input_file, network_interface, is_rr_mode):
        self.compatibility_mode = False
        if ',' in input_file:
            self.compatibility_mode = True
            rr_infile, normal_infile = input_file.split(',')
            self.rr_input_file = rr_infile
            self.normal_input_file = normal_infile
        self.input_file = input_file
        self.is_rr_input = is_rr_mode
        self.sched_params = None
        self.scheduler_now = None
        self._estimated_scheduler_runtime = None
        self.json_request_group_list = None
        self.available_resources = None
        self.resource_usage_snapshot = None
        self.last_known_state_timestamp = None
        self.network_interface = network_interface
        self.semester_details = None
        self.proposals_by_id = {}
        self.normal = {}
        self.rr = {}

        self.refresh()

    def set_rr_run_time(self, seconds):
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

    @staticmethod
    def _get_pickled_input(filename):
        with open(filename, 'r') as input_file:
            pickle_input = pickle.load(input_file)

        return pickle_input

    def refresh(self):
        if self.compatibility_mode:
            input_filename = self.normal_input_file
            if self.is_rr_input:
                input_filename = self.rr_input_file
            pickle_input = self._get_pickled_input(input_filename)

            self.sched_params = pickle_input['sched_params']
            # set the filename of the current sched_params to this input filename. I don't see any reason to maintain
            # a separate current filename and 'input' filename.
            self.sched_params.input_file_name = input_filename
            self.scheduler_now = pickle_input['scheduler_now']
            self._estimated_scheduler_runtime = pickle_input['estimated_scheduler_runtime']
            self.json_request_group_list = pickle_input['json_request_group_list']
            self.available_resources = pickle_input['available_resources']
            self.resource_usage_snapshot = pickle_input['resource_usage_snapshot']
            self.semester_details = pickle_input.get('semester_details')
            self.proposals_by_id = pickle_input.get('proposals_by_id', {})

        else:
            if self.is_rr_input:
                pickle_input = self._get_pickled_input(self.input_file)
                self.sched_params = pickle_input['sched_params']
                self.sched_params.input_file_name = self.input_file
                self.normal = pickle_input['normal']
                self.rr = pickle_input['rr']
                self.json_request_group_list = pickle_input['json_request_group_list']
                self.available_resources = pickle_input['available_resources']
                self.semester_details = pickle_input.get('semester_details')
                self.proposals_by_id = pickle_input.get('proposals_by_id', {})
                self._estimated_scheduler_runtime = self.rr['estimated_scheduler_runtime']
                self.scheduler_now = self.rr['scheduler_now']
                self.resource_usage_snapshot = self.rr['resource_usage_snapshot']
            else:
                self._estimated_scheduler_runtime = self.normal['estimated_scheduler_runtime']
                self.scheduler_now = self.normal['scheduler_now']
                self.resource_usage_snapshot = self.normal['resource_usage_snapshot']

    def set_rr_mode(self):
        self.is_rr_input = True
        self.refresh()

    def set_normal_mode(self):
        self.is_rr_input = False
        self.refresh()

    def get_scheduler_now(self):
        return self.scheduler_now

    def get_model_builder(self):
        mb = ModelBuilder(None,
                          self.network_interface.configdb_interface,
                          proposals_by_id=self.proposals_by_id,
                          semester_details=self.semester_details)

        return mb
