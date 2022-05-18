from __future__ import division

import time
import logging
import itertools
import json
import boto3
from collections import defaultdict
from functools import cmp_to_key

from datetime import datetime, timedelta

from adaptive_scheduler.feedback import TimingLogger
from adaptive_scheduler.interfaces import ScheduleException
from adaptive_scheduler.event_utils import report_scheduling_outcome
from time_intervals.intervals import Intervals

from adaptive_scheduler.utils import (timeit, iso_string_to_datetime, estimate_runtime, SendMetricMixin,
                                      metric_timer, set_schedule_type, NORMAL_OBSERVATION_TYPE, RR_OBSERVATION_TYPE,
                                      get_reservation_datetimes, time_in_capped_intervals, cap_intervals,
                                      merge_downtime_dicts)
from adaptive_scheduler.printing import pluralise as pl
from adaptive_scheduler.printing import plural_str
from adaptive_scheduler.printing import print_compound_reservations, summarise_rgs, log_full_rg, log_windows
from adaptive_scheduler.models import filter_out_compounds, differentiate_by_type, n_requests, DataContainer
from adaptive_scheduler.kernel_mappings import (construct_visibilities,
                                                construct_resource_windows,
                                                make_compound_reservations,
                                                make_many_type_compound_reservations,
                                                filter_for_kernel,
                                                construct_global_availability)
from adaptive_scheduler.request_filters import filter_rgs, drop_empty_requests, set_now
from adaptive_scheduler.observation_portal_connections import ObservationPortalConnectionError
from adaptive_scheduler.downtime_connections import DowntimeError, DowntimeInterface


class Scheduler(SendMetricMixin):

    def __init__(self, kernel_class, sched_params, event_bus):
        self.kernel_class = kernel_class
        self.visibility_cache = {}
        self.saved_semester = {'start': None, 'end': None}
        self.sched_params = sched_params
        self.event_bus = event_bus
        self.log = logging.getLogger(__name__)
        if self.sched_params.simulate_now:
            self.estimated_scheduler_end = iso_string_to_datetime(self.sched_params.simulate_now)
        else:
            self.estimated_scheduler_end = datetime.utcnow()
        self.scheduler_summary_messages = []

    # TODO - Move to a utils library
    def combine_excluded_intervals(self, excluded_intervals_1, excluded_intervals_2):
        ''' Combine two dictionaries where Intervals are the values '''
        for key in excluded_intervals_2:
            timepoints = excluded_intervals_2[key].toDictList()
            excluded_intervals_1.setdefault(key, Intervals([])).add(timepoints)

        return excluded_intervals_1

    def compute_optimal_combination(self, value_dict, request_group_ids, resources):
        '''
        Compute combination of telescope to request group id that has the highest value

        NOTE: This schedule assumes that each request group only needs one
              telescope to run (no compound requests).
        '''
        if len(request_group_ids) < len(resources):
            small_list = request_group_ids
            large_list = resources
            zip_combinations = lambda x: zip(x, small_list)
        else:
            large_list = request_group_ids
            small_list = resources
            zip_combinations = lambda x: zip(small_list, x)

        optimal_combination_value = -1
        optimal_combinations = []

        # Create all possible permutations of of the large list of length <= the length of the small list
        # Handles the cases where not all Rapid Response RG's are possible due to a resource being unavailable
        permutations = []
        for i in range(len(small_list)):
            permutations.extend(itertools.permutations(large_list, i + 1))

        for x in permutations:
            combinations = list(zip_combinations(x))
            value = 0
            invalid_combination = False
            for combination in combinations:
                try:
                    value += value_dict[combination]
                except KeyError:
                    # if the combination is not in the dictionary it is not a valid option
                    invalid_combination = True
                    break

            if invalid_combination:
                continue

            if value > optimal_combination_value:
                optimal_combination_value = value
                optimal_combinations = combinations

        return optimal_combinations

    def remove_singles(self, request_groups):
        self.log.info("Compound Request support (single) disabled at the command line")
        self.log.info("Compound Requests of type 'single' will be ignored")
        _, others = differentiate_by_type(operator='single', rgs=request_groups)

        return others

    def remove_compounds(self, request_groups):
        self.log.info("Compound Request support (and/oneof/many) disabled at the command line")
        self.log.info("Compound Requests of type 'and', 'oneof' or 'many' will be ignored")

        return filter_out_compounds(request_groups)

    def scheduling_horizon(self, estimated_scheduler_end):
        return estimated_scheduler_end + timedelta(days=self.sched_params.horizon_days)

    def apply_unschedulable_filters(self, request_groups, estimated_scheduler_end, running_request_ids):
        ''' Returns tuple of (schedulable, unschedulable) request groups where RG's
        in the unschedulable list will never be possible
        '''
        return request_groups, []

    def apply_window_filters(self, request_groups, estimated_scheduler_end, semester_details,
                             extra_downtime_by_resource):
        ''' Returns the set of RGs with windows adjusted to include only RGs with windows
        suitable for scheduling
        '''
        return request_groups

    def prepare_for_kernel(self, window_adjusted_rgs, semester_details):
        ''' Convert RG model to formalization expected by the scheduling kernel
        '''
        return []

    def prepare_available_windows_for_kernel(self, available_resources, resource_interval_mask, semester_details):
        ''' Construct the set of resource windows available for use in scheduling
        '''
        return []

    # TODO: replace with event bus event
    def on_run_scheduler(self, request_groups, semester_details):
        ''' Handler called at beginning run_schedule
        '''
        pass

    # TODO: replace with event bus event
    def after_unschedulable_filters(self, request_groups):
        ''' Handler called after unschedulable filters have been applied
        '''
        pass

    # TODO: replace with event bus event
    def after_window_filters(self, request_groups):
        ''' Handler called after window filters have been applied
        '''
        pass

    # TODO: replace with event bus event
    def on_new_schedule(self, new_schedule, compound_reservations, estimated_scheduler_end):
        ''' Handler called on completion of a scheduler run
        '''
        pass

    def save_schedule(self, schedule, estimated_scheduler_end, semester_details, is_rr):
        ''' Save the final schedule in a json file if save_output flag was set.
        '''
        if self.sched_params.save_output:
            semester_start = semester_details['start']
            schedule_data = {'schedule_start': estimated_scheduler_end.isoformat(),
                             'schedule_end': self.scheduling_horizon(estimated_scheduler_end).isoformat(),
                             'semester_id': semester_details['id'],
                             'horizon_days': self.sched_params.horizon_days,
                             'resources': {},
                             'total_priority_value': 0}
            for resource, reservations in schedule.items():
                schedule_data['resources'][resource] = {'reservations': [], 'dark_intervals': []}
                priority_value = 0
                for reservation in reservations:
                    reservation_start, reservation_end = get_reservation_datetimes(reservation, semester_start)
                    res_data = {'request_id': reservation.request.id,
                                'priority': reservation.priority,
                                'start': reservation_start.isoformat(),
                                'end': reservation_end.isoformat(),
                                'resource': resource}
                    priority_value += reservation.priority
                    schedule_data['resources'][resource]['reservations'].append(res_data)
                schedule_data['resources'][resource]['priority_value'] = priority_value
                schedule_data['total_priority_value'] += priority_value

                # also store the intervals at the site that were considered
                dark_intervals = self.visibility_cache[resource].get_dark_intervals()
                # get the dark intervals of the site, capped by the scheduled range of time (scheduler end + 7 days)
                capped_dark_intervals = cap_intervals(dark_intervals, estimated_scheduler_end,
                                                      self.scheduling_horizon(estimated_scheduler_end))
                for interval in capped_dark_intervals:
                    schedule_data['resources'][resource]['dark_intervals'].append({'start': interval[0].isoformat(),
                                                                                   'end': interval[1].isoformat()})

            if is_rr:
                schedule_type = RR_OBSERVATION_TYPE
            else:
                schedule_type = NORMAL_OBSERVATION_TYPE
            now = datetime.utcnow()
            day_timestamp = now.strftime('%Y-%m-%d')
            file_timestamp = now.strftime('%Y%m%d%H%M%S')
            if self.sched_params.telescope_classes:
                telescope_class_str = '_'.join(self.sched_params.telescope_classes)
            else:
                telescope_class_str = 'all'
            filename = '{}_{}_schedule_{}.json'.format(schedule_type, telescope_class_str, file_timestamp)

            # If an S3 bucket is configured, attempt to store output files in the bucket in a daydir
            if self.sched_params.s3_bucket:
                serialized_output = json.dumps(schedule_data)
                try:
                    s3 = boto3.client('s3')
                    s3.put_object(Bucket=self.sched_params.s3_bucket, Key=f'{day_timestamp}/{filename}',
                                  Body=serialized_output)
                except Exception as e:
                    logging.warning(f"Failed to store output file in S3 bucket: {repr(e)}")
            else:
                with open('/data/adaptive_scheduler/output_schedule/{}'.format(filename), 'w') as schedule_out:
                    json.dump(schedule_data, schedule_out)

    def produce_schedule_metrics(self, schedule, estimated_scheduler_end, semester_details):
        ''' Create opentsdb metrics on how full the schedule is per resource for the horizon and for the next 1 day.
        '''
        semester_start = semester_details['start']
        horizon_days = self.sched_params.horizon_days
        one_day_horizon = estimated_scheduler_end + timedelta(days=1)

        for resource, reservations in schedule.items():
            available_seconds_for_horizon = 0
            available_seconds_for_one_day = 0
            if resource in self.visibility_cache:
                dark_intervals = self.visibility_cache[resource].get_dark_intervals()
                # get the dark intervals of the site, capped by the scheduled range of time (scheduler end + horizon)
                available_seconds_for_horizon = time_in_capped_intervals(dark_intervals, estimated_scheduler_end,
                                                                         self.scheduling_horizon(
                                                                             estimated_scheduler_end))

                # get the dark intervals time for a horizon of 1 day
                if horizon_days != 1:
                    available_seconds_for_one_day = time_in_capped_intervals(dark_intervals, estimated_scheduler_end,
                                                                             one_day_horizon)

            # now get the scheduled seconds for that resource
            scheduled_seconds_for_horizon = 0
            scheduled_seconds_for_one_day = 0
            for reservation in reservations:
                reservation_start, reservation_end = get_reservation_datetimes(reservation, semester_start)
                scheduled_seconds_for_horizon += (reservation_end - reservation_start).total_seconds()
                if horizon_days != 1 and reservation_start < one_day_horizon:
                    capped_end = min(reservation_end, one_day_horizon)
                    scheduled_seconds_for_one_day += (capped_end - reservation_start).total_seconds()

            # log and record a metric for how full the telescope schedule is.
            self.log.info("telescope {} filled {:.3f} / {:.3f} hours".format(
                resource, (scheduled_seconds_for_horizon / 3600.0), (available_seconds_for_horizon / 3600.0)))
            self._send_schedule_metrics(resource, scheduled_seconds_for_horizon, available_seconds_for_horizon,
                                        horizon_days)
            if horizon_days != 1:
                self._send_schedule_metrics(resource, scheduled_seconds_for_one_day, available_seconds_for_one_day, 1)

    def produce_unscheduled_metrics(self, input_reservations, schedule):
        # Compute the # of unscheduled and length of unscheduled observations per telescope class
        scheduled_reservations = []
        [scheduled_reservations.extend(a) for a in schedule.values()]
        to_schedule_res = []
        for comp_res in input_reservations:
            to_schedule_res.extend(comp_res.reservation_list)

        not_scheduled_reservations = set(to_schedule_res) - set(scheduled_reservations)
        unscheduled_reqs_per_class = defaultdict(int)
        unscheduled_hours_per_class = defaultdict(float)
        for reservation in not_scheduled_reservations:
            # We don't currently save telescope_class, so have to parse it from beginning of inst type
            telescope_class = reservation.request.configurations[0].instrument_type[:3].lower()
            unscheduled_reqs_per_class[telescope_class] += 1
            unscheduled_hours_per_class[telescope_class] += (reservation.duration / 3600.0)
        total_reqs_per_class = defaultdict(int)
        total_hours_per_class = defaultdict(float)
        for reservation in to_schedule_res:
            telescope_class = reservation.request.configurations[0].instrument_type[:3].lower()
            total_reqs_per_class[telescope_class] += 1
            total_hours_per_class[telescope_class] += (reservation.duration / 3600.0)

        # log the results and record metrics
        for telescope_class in total_reqs_per_class.keys():
            self.log.info("telescope class {} left with {} / {} unscheduled reservations".format(
                telescope_class, unscheduled_reqs_per_class[telescope_class],
                total_reqs_per_class[telescope_class]
            ))
            self.log.info("telescope class {} left with {:.3f} / {:.3f} unscheduled hours".format(
                telescope_class, unscheduled_hours_per_class[telescope_class],
                total_hours_per_class[telescope_class]
            ))
            self.send_metric('unscheduled_requests.duration_hours', unscheduled_hours_per_class[telescope_class],
                             telescope_class=telescope_class)
            self.send_metric('unscheduled_requests.number', unscheduled_reqs_per_class[telescope_class],
                             telescope_class=telescope_class)
            self.send_metric('total_requests.duration_hours', total_hours_per_class[telescope_class],
                             telescope_class=telescope_class)
            self.send_metric('total_requests.number', total_reqs_per_class[telescope_class],
                             telescope_class=telescope_class)

    def _send_schedule_metrics(self, resource, scheduled_seconds, available_seconds, horizon_days):
        ''' Helper function to submit available, scheduled and % utilization metrics to opentsdb.
        '''
        telescope, observatory, site = resource.split('.')
        utilization = (scheduled_seconds / available_seconds) if available_seconds > 0 else 0
        self.send_metric('scheduled_time.available_seconds', available_seconds, telescope=telescope,
                         observatory=observatory, site=site, horizon_days=horizon_days)
        self.send_metric('scheduled_time.scheduled_seconds', scheduled_seconds, telescope=telescope,
                         observatory=observatory, site=site, horizon_days=horizon_days)
        self.send_metric('scheduled_time.utilization_percent', utilization, telescope=telescope,
                         observatory=observatory, site=site, horizon_days=horizon_days)

    def create_resource_mask(self, available_resources, resource_usage_snapshot, rr_request_group_ids,
                             preemption_enabled):
        resource_interval_mask = {}
        for resource_name in available_resources:
            running_request_groups = resource_usage_snapshot.request_groups_for_resource(resource_name)
            # Limit to only RR running request group when preemption is enabled
            if preemption_enabled:
                running_request_groups = [rg for rg in running_request_groups if rg.id in rr_request_group_ids]

            masked_timepoints_for_resource = []
            for rg in running_request_groups:
                for r in rg.running_requests:
                    if r.should_continue():
                        masked_timepoints_for_resource.extend(r.timepoints())
            resource_interval_mask[resource_name] = Intervals(masked_timepoints_for_resource)
            resource_interval_mask[resource_name].add(
                resource_usage_snapshot.blocked_intervals(resource_name).toDictList())

        return resource_interval_mask

        # TODO: refactor into smaller chunks

    @timeit
    @metric_timer('scheduling', num_requests=lambda x: x.count_reservations())
    def run_scheduler(self, scheduler_input, estimated_scheduler_end, semester_details, preemption_enabled=False):
        start_event = TimingLogger.create_start_event(datetime.utcnow())
        self.event_bus.fire_event(start_event)

        # ToDo: Need to be able to get unavailable resources and reason for their unavailability
        # to determine how to handle reservations currently scheduled on those resources
        # Commit schedules for unreachable resources
        # Cancel schedules for unavailable resources
        request_groups = scheduler_input.request_groups

        resource_usage_snapshot = scheduler_input.resource_usage_snapshot
        available_resources = scheduler_input.available_resources

        self.estimated_scheduler_end = estimated_scheduler_end
        self.on_run_scheduler(request_groups, semester_details)

        if self.sched_params.no_singles:
            request_groups = self.remove_singles(request_groups)

        if self.sched_params.no_compounds:
            request_groups = self.remove_compounds(request_groups)

        running_requests = resource_usage_snapshot.running_requests_for_resources(available_resources)
        # A request should only be filtered from the scheduling input if it has a chance of completing successfully
        running_request_ids = [r.id for r in running_requests if r.should_continue()]
        schedulable_rgs, unschedulable_rgs = self.apply_unschedulable_filters(request_groups, estimated_scheduler_end,
                                                                              running_request_ids)

        # Remove child request from the parent request group when the request has no windows remaining
        drop_empty_requests(schedulable_rgs)
        self.after_unschedulable_filters(schedulable_rgs)

        window_adjusted_rgs = self.apply_window_filters(schedulable_rgs, estimated_scheduler_end, semester_details,
                                                        scheduler_input.get_block_schedule_by_resource())
        self.after_window_filters(window_adjusted_rgs)

        # By default, schedule on all resources
        resources_to_schedule = list(available_resources)
        resource_interval_mask = self.create_resource_mask(available_resources, resource_usage_snapshot,
                                                           scheduler_input.rr_request_group_ids, preemption_enabled)

        compound_reservations = self.prepare_for_kernel(window_adjusted_rgs, semester_details)
        available_windows = self.prepare_available_windows_for_kernel(resources_to_schedule, resource_interval_mask,
                                                                      semester_details)

        print_compound_reservations(compound_reservations)
        # Prepare scheduler result
        scheduler_result = SchedulerResult()
        scheduler_result.schedule = {}
        scheduler_result.resource_schedules_to_cancel = list(available_resources)

        if compound_reservations:
            # Instantiate and run the scheduler
            contractual_obligations = []

            kernel = self.kernel_class(self.sched_params.kernel, compound_reservations, available_windows,
                                       contractual_obligations, self.sched_params.slicesize_seconds,
                                       self.sched_params.mip_gap, self.sched_params.warm_starts, self.sched_params.kernel_params)
            scheduler_result.schedule = kernel.schedule_all(timelimit=self.sched_params.timelimit_seconds)

            # TODO: Remove resource_schedules_to_cancel from Scheduler result, this should be managed at a higher level
            # Limit canceled resources to those where request groups were canceled
            if (preemption_enabled):
                for resource in available_resources:
                    if scheduler_result.schedule.get(resource, []) == []:
                        scheduler_result.resource_schedules_to_cancel.remove(resource)

            # Do post scheduling stuff
            self.save_schedule(scheduler_result.schedule, estimated_scheduler_end, semester_details, preemption_enabled)
            self.produce_schedule_metrics(scheduler_result.schedule, estimated_scheduler_end, semester_details)
            self.produce_unscheduled_metrics(compound_reservations, scheduler_result.schedule)
            self.on_new_schedule(scheduler_result.schedule, compound_reservations, estimated_scheduler_end)
        else:
            self.log.info("Nothing to schedule! Skipping kernel call...")
            scheduler_result.resource_schedules_to_cancel = {}

        return scheduler_result


class LCOGTNetworkScheduler(Scheduler):
    def __init__(self, kernel_class, sched_params, event_bus, network_model):
        super().__init__(kernel_class, sched_params, event_bus)

        self.visibility_cache = {}
        self.date_fmt = '%Y-%m-%d'
        self.date_time_fmt = '%Y-%m-%d %H:%M:%S'
        self.network_model = network_model

    def _log_scheduler_start_details(self, estimated_scheduler_end, semester_details):
        self.log.info("Scheduling for semester %s (%s to %s)", semester_details['id'],
                      semester_details['start'].isoformat(),
                      semester_details['end'].isoformat())
        strfmt_horizon = self.scheduling_horizon(estimated_scheduler_end).strftime(self.date_time_fmt)
        self.log.info("Scheduling horizon is %s", strfmt_horizon)

    def _log_rg_input_details(self, request_groups, estimated_scheduler_end):
        # Summarise the Request Groups we've received
        n_rgs, n_rs = n_requests(request_groups)

        self.log.info("Deserialised %s (%s) from Observation Portal", plural_str(n_rgs, 'Request Group'),
                      plural_str(n_rs, 'Request'))

        summarise_rgs(request_groups, log_msg="Received from Observation Portal")
        for rg in request_groups:
            log_full_rg(rg, estimated_scheduler_end)
            log_windows(rg, log_msg="Initial windows:")

    @metric_timer('apply_unschedulable_filters')
    def apply_unschedulable_filters(self, request_groups, estimated_scheduler_end, running_request_ids):
        ''' Returns tuple of (schedulable, unschedulable) request groups where RG's
        in the unschedulable list will never be possible
        '''
        self.log.info("Filtering for unschedulability")

        set_now(estimated_scheduler_end)
        schedulable_rgs, unschedulable_rgs = filter_rgs(request_groups, running_request_ids)
        self.log.info("Found %d unschedulable %s after filtering", *pl(len(unschedulable_rgs), 'RG'))

        return schedulable_rgs, unschedulable_rgs

    @metric_timer('apply_window_filters', num_requests=len)
    def apply_window_filters(self, request_groups, estimated_scheduler_end, semester_details,
                             extra_downtime_by_resource):
        ''' Returns the set of RGs with windows adjusted to include only RGs with windows
        suitable for scheduling
        '''
        self.log.info("Filtering on dark/rise_set")

        semester_end = semester_details['end']

        downtime_interface = DowntimeInterface(self.sched_params.downtime_url)
        try:
            downtime_intervals = downtime_interface.get_downtime_intervals_by_resource_and_instrument_type(
                start=estimated_scheduler_end,
                end=self.scheduling_horizon(
                estimated_scheduler_end)
            )
        except DowntimeError as e:
            self.log.warning("Problem getting downtime intervals: {}".format(repr(e)))
            downtime_intervals = {}

        combined_downtime_intervals = merge_downtime_dicts(downtime_intervals, extra_downtime_by_resource)
        filtered_window_request_groups = filter_for_kernel(request_groups, self.visibility_cache,
                                                           combined_downtime_intervals,
                                                           semester_details['start'], semester_end,
                                                           self.scheduling_horizon(estimated_scheduler_end))

        return filtered_window_request_groups

    @metric_timer('prepare_for_kernel', num_requests=len)
    def prepare_for_kernel(self, window_adjusted_rgs, semester_details):
        ''' Convert RG model to formalization expected by the scheduling kernel
        '''
        semester_start = semester_details['start']

        many_rgs, other_rgs = differentiate_by_type('many', window_adjusted_rgs)
        many_compound_reservations = make_many_type_compound_reservations(many_rgs, semester_start)
        other_compound_reservations = make_compound_reservations(other_rgs, semester_start)
        all_compound_reservations = many_compound_reservations + other_compound_reservations

        return all_compound_reservations

    @metric_timer('prepare_available_windows_for_kernel')
    def prepare_available_windows_for_kernel(self, available_resources, resource_interval_mask, semester_details):
        ''' Construct the set of resource windows available for use in scheduling
        '''
        semester_start = semester_details['start']

        # Translate when telescopes are available into kernel speak
        resource_windows = construct_resource_windows(self.visibility_cache, semester_start, available_resources)

        # Intersect and mask out time where Blocks are currently running
        global_windows = construct_global_availability(resource_interval_mask, semester_start,
                                                       resource_windows)

        return global_windows

    def on_run_scheduler(self, request_groups, semester_details):
        self._log_scheduler_start_details(self.estimated_scheduler_end, semester_details)
        self._log_rg_input_details(request_groups, self.estimated_scheduler_end)

        semester_start = semester_details['start']
        semester_end = semester_details['end']
        # Clear the visibility cache if the semester has changed
        if semester_details['start'] != self.saved_semester['start'] or semester_details['end'] != self.saved_semester[
            'end']:
            self.visibility_cache = {}
        self.saved_semester = semester_details.copy()

        # Construct visibility objects for each telescope if it is not cached
        if not self.visibility_cache:
            self.log.info("Constructing telescope visibilities")
            self.visibility_cache = construct_visibilities(self.network_model, semester_start, semester_end)

    def after_unschedulable_filters(self, request_groups):
        summarise_rgs(request_groups, log_msg="Passed unschedulable filters:")

        for rg in request_groups:
            log_windows(rg, log_msg="Remaining windows:")

    def after_window_filters(self, request_groups):
        self.log.info("Completed dark/rise_set filters")
        summarise_rgs(request_groups, log_msg="Passed dark/rise filters:")
        for rg in request_groups:
            log_windows(rg, log_msg="Remaining windows:")

        self.log.info('Filtering complete. Ready to construct Reservations from %d RGs.' % len(request_groups))

    def on_new_schedule(self, new_schedule, compound_reservations, estimated_scheduler_end):
        ''' Handler called on completion of a scheduler run
        '''
        scheduled_compound_reservations = []
        [scheduled_compound_reservations.extend(a) for a in new_schedule.values()]
        self.log.info(
            "Scheduling completed. Final schedule has %d Reservations." % len(scheduled_compound_reservations))

        report_scheduling_outcome(compound_reservations, scheduled_compound_reservations)


class SchedulerResult(object):
    '''Aggregates together output of a scheduler run
    '''

    def __init__(self, schedule=None, resource_schedules_to_cancel=None):
        '''
        schedule - Expected to be a dict mapping resource to scheduled reservations
        resource_schedules_to_cancel - List of resources to cancel schedules on - this is the list of all available
        resources that have any request scheduled on them. Resources with no requests scheduled on them will be
        removed from the list.
        '''
        self.schedule = schedule if schedule else {}
        self.resource_schedules_to_cancel = resource_schedules_to_cancel if resource_schedules_to_cancel else []

    def count_reservations(self):
        reservation_cnt = 0
        for reservations in self.schedule.values():
            reservation_cnt += len(reservations)

        return reservation_cnt

    def get_scheduled_requests_by_request_group_id(self):
        scheduled_requests_by_request_group_id = {}
        for reservations in self.schedule.values():
            for reservation in reservations:
                request_id = reservation.request.id
                request_group_id = reservation.request_group.id
                if not request_group_id in scheduled_requests_by_request_group_id:
                    scheduled_requests_by_request_group_id[request_group_id] = {}
                scheduled_requests_by_request_group_id[request_group_id][request_id] = DataContainer(
                    duration=reservation.duration,
                    scheduled_resource=reservation.scheduled_resource,
                    scheduled=reservation.scheduled,
                    scheduled_start=reservation.scheduled_start
                )
        return scheduled_requests_by_request_group_id

    def resources_scheduled(self):
        return list(self.schedule.keys())

    def earliest_reservation(self, resource):
        earliest = None
        reservations = list(self.schedule.get(resource, []))
        if reservations:
            reservations = sorted(
                reservations,
                key=cmp_to_key(
                    lambda x, y: ((x.scheduled_start > y.scheduled_start) - (x.scheduled_start < y.scheduled_start)))
            )
            earliest = reservations[0]

        return earliest


class SchedulerRunner(object):

    def __init__(self, sched_params, scheduler, network_interface, network_model, input_factory):
        self.run_flag = True
        self.sched_params = sched_params
        self.warm_starts_setting = sched_params.warm_starts
        self.scheduler = scheduler
        self.network_interface = network_interface
        self.network_model = network_model
        self.input_factory = input_factory
        self.normal_scheduled_requests_by_rg = {}
        self.rr_scheduled_requests_by_rg = {}
        self.log = logging.getLogger(__name__)
        # List of strings to be printed in final scheduling summary
        self.summary_events = []

        self.estimated_rr_run_timedelta = timedelta(seconds=sched_params.rr_runtime_seconds)
        self.estimated_normal_run_timedelta = timedelta(seconds=sched_params.normal_runtime_seconds)
        self.avg_save_time_per_reservation_timedelta = timedelta(seconds=sched_params.avg_reservation_save_time_seconds)
        self.first_run = True
        self.semester_details = None

    def scheduler_rerun_required(self):
        ''' Return True if scheduler should be run now
        '''
        network_has_changed = False

        if self.network_interface.current_events_has_changed():
            self.log.info("Telescope network event changes were found, turning off warm starts next run.")
            network_has_changed = True
            # Turn the warm start off for this run, since there was a change in the network telescopes
            self.sched_params.warm_starts = False
        elif self.sched_params.no_weather:
            self.log.info("Ignoring Telescope network events, but setting network change flag to True.")
            network_has_changed = True

        request_set_changed = True
        if (not self.sched_params.dry_run) and (not self.sched_params.no_weather):
            # Skipping weather checking or doing a dry run forces network/request update
            request_set_changed = self.network_interface.schedulable_request_set_has_changed(self.sched_params.telescope_classes)

        return network_has_changed or request_set_changed

    @timeit
    @metric_timer('update_network_model')
    def update_network_model(self):
        current_events = self.network_interface.get_current_events()
        available_telescopes = []
        for telescope_name, telescope in self.network_model.items():
            if telescope_name in current_events:
                telescope['events'].extend(current_events[telescope_name])
                msg = "Found network event for '%s' - removing from consideration (%s)" % (
                    telescope_name,
                    current_events[telescope_name])
                self.log.info(msg)
            else:
                available_telescopes.append(telescope_name)
                telescope['events'] = []
        return

    def get_semester_details(self, date):
        '''Attempts to get the semester details for the current date (if it is not the current semester).
           If it fails, previous semester details will be returned.
        '''
        try:
            self.semester_details = self.network_interface.observation_portal_interface.get_semester_details(date)
        except ObservationPortalConnectionError as e:
            self.log.warning("Error getting current semester: {}".format(repr(e)))
            raise ScheduleException("Unable to get current semester details. Skipping run.")
        return self.semester_details

    def run(self):
        while self.run_flag:
            self.run_once()
            self.first_run = False
            if self.sched_params.run_once:
                self.run_flag = False
            else:
                self.log.info("Sleeping for %d seconds", self.sched_params.sleep_seconds)
                time.sleep(self.sched_params.sleep_seconds)

    @timeit
    @metric_timer('total_scheduling_cycle')
    def run_once(self):
        if self.sched_params.no_weather:
            self.log.info("Weather monitoring disabled on the command line")
        else:
            self.update_network_model()

        # Always run the scheduler on the first run
        rerun_required = False
        scheduler_run_start = self.input_factory.input_provider.get_scheduler_now()
        try:
            self.semester_details = self.get_semester_details(scheduler_run_start)
            self.network_interface.configdb_interface.update_configdb_structures()
            if self.scheduler_rerun_required() or self.first_run or rerun_required:
                rerun_required = False
                self.create_new_schedule(scheduler_run_start)
                # Reset the warm starts flag back to the input setting at the end of each run
                self.sched_params.warm_starts = self.warm_starts_setting
        except (ObservationPortalConnectionError, ScheduleException, EstimateExceededException) as eee:
            # Estimated run time was exceeded so exception was raised, or web resource failed
            # We should force a rerun in any case in case the network events and requests haven't changed
            rerun_required = True
            self.log.warning("Skipping Scheduling Run: {}".format(repr(eee)))

    def call_scheduler(self, scheduler_input, estimated_scheduler_end):
        self.log.info("Using a 'now' of %s", scheduler_input.scheduler_now)
        self.log.info("Estimated scheduler run time is {:.2f} seconds".format(
            scheduler_input.estimated_scheduler_runtime.total_seconds()))
        self.log.info("Estimated scheduler end %s", estimated_scheduler_end)
        n_rgs, n_rs = n_requests(scheduler_input.request_groups)
        self.summary_events.append(
            "Received %d %s (%d %s) from the Observation Portal" % (pl(n_rgs, 'Request Group') + pl(n_rs, 'Request')))
        scheduler_result = None
        try:
            scheduler_result = self.scheduler.run_scheduler(scheduler_input, estimated_scheduler_end,
                                                            self.semester_details,
                                                            preemption_enabled=scheduler_input.is_rr_input)
        except ScheduleException as se:
            self.log.error(se, "aborting run")

        return scheduler_result

    def clear_resource_schedules(self, cancelation_date_list_by_resource, include_rr, include_normal, preemption_enabled):
        n_deleted = self.network_interface.cancel(cancelation_date_list_by_resource, include_rr, include_normal, preemption_enabled)

        return n_deleted

    def abort_running_requests(self, abort_requests):
        n_aborted = 0
        for rr, _ in abort_requests:
            n_aborted += self.network_interface.abort(rr)

        return n_aborted

    def save_resource_schedules(self, schedule, denormalization_date):
        n_submitted = self.network_interface.save(schedule, denormalization_date, False)

        return n_submitted

    def _determine_resource_cancelation_start_date(self, scheduled_reservations, running_request_groups,
                                                   default_cancelation_start_date):
        cancelation_start_date = default_cancelation_start_date
        for running_request_group in running_request_groups:
            for running_request in running_request_group.running_requests:
                if running_request.end > cancelation_start_date:
                    cancelation_start_date = running_request.end

        return cancelation_start_date

    def _determine_schedule_cancelation_start_dates(self, resources_to_cancel, schedule_by_resource,
                                                    resource_usage_snapshot, default_cancelation_start,
                                                    default_cancelation_end):
        cancelation_date_list_by_resource = {}
        for resource in resources_to_cancel:
            scheduled_at_resource = schedule_by_resource.get(resource, [])
            start = self._determine_resource_cancelation_start_date(scheduled_at_resource,
                                                                    resource_usage_snapshot.request_groups_for_resource(
                                                                        resource), default_cancelation_start)
            end = default_cancelation_end
            cancelation_date_list_by_resource[resource] = [(start, end), ]

        return cancelation_date_list_by_resource

    def _determine_schedule_cancelation_list_from_new_schedule(self, schedule_by_resource):
        cancelation_date_list_by_resource = {}
        semester_start = self.semester_details['start']

        for resource, reservations in schedule_by_resource.items():
            if resource not in cancelation_date_list_by_resource:
                cancelation_date_list_by_resource[resource] = []
            for reservation in reservations:
                reservation_start, reservation_end = get_reservation_datetimes(reservation, semester_start)
                cancelation_date_list_by_resource[resource].append((reservation_start, reservation_end))

        return cancelation_date_list_by_resource

    def _determine_abort_requests(self, running_request_groups, schedule_denoramlization_date, earliest_reservation):
        abort_requests = []

        for running_request_group in running_request_groups:
            abort_reasons = []
            for running_request in running_request_group.running_requests:
                # if it can't complete successfully, cancel it
                if not running_request.should_continue():
                    abort_reasons.append("Can not complete successfully: " + ",".join(running_request.errors()))

                # if it interferes with the new schedule, cancel it
                if earliest_reservation:
                    earliest_res_denormalized_start = schedule_denoramlization_date + timedelta(
                        seconds=earliest_reservation.scheduled_start)
                    if running_request.end > earliest_res_denormalized_start:
                        abort_reasons.append(
                            "Request interrupted to observe request: %d" % earliest_reservation.request.id)
            if abort_reasons:
                abort_requests.append((running_request, abort_reasons))

        return abort_requests

    def _can_apply_scheduler_result(self, scheduler_result, apply_deadline):
        if self.sched_params.dry_run:
            self.log.warn("Dry run. Schedule will not be saved.")
            return False

        if not scheduler_result:
            self.log.warn("Empty scheduler result. Schedule will not be saved.")
            return False

        estimated_apply_timedelta = self.avg_save_time_per_reservation_timedelta * scheduler_result.count_reservations()
        estimated_apply_completion = datetime.utcnow() + estimated_apply_timedelta
        self.log.info(
            "Estimated time to apply scheduler result is %.2f seconds" % estimated_apply_timedelta.total_seconds())
        if estimated_apply_completion > apply_deadline:
            raise EstimateExceededException(
                "Estimated end time %s is after deadline %s" % (str(estimated_apply_completion), str(apply_deadline)),
                estimated_apply_completion)

        return True

    def apply_rr_result(self, scheduler_result, scheduler_input, apply_deadline):
        ''' For the RR cycle, we want to clear the parts of the scheduler for which a RR was scheduled, but leave
            the rest of the blocks intact.
        '''
        if self._can_apply_scheduler_result(scheduler_result, apply_deadline):
            cancelation_date_list_by_resource = self._determine_schedule_cancelation_list_from_new_schedule(
                scheduler_result.schedule)

            # Find running requests that need to be aborted due to conflict with new schedule
            abort_requests = []
            semester_start = self.semester_details['start']
            semester_end = self.semester_details['end']

            all_cancelation_date_list_by_resource = self._determine_schedule_cancelation_start_dates(
                list(self.network_model.keys()),
                scheduler_result.schedule,
                scheduler_input.resource_usage_snapshot,
                apply_deadline,
                semester_end)

            for resource in scheduler_result.resources_scheduled():
                earliest_reservation = scheduler_result.earliest_reservation(resource)
                to_abort = self._determine_abort_requests(
                    scheduler_input.resource_usage_snapshot.request_groups_for_resource(resource),
                    semester_start,
                    earliest_reservation)
                abort_requests.extend(to_abort)

            # Cancel just the time slots under a newly scheduled RR
            n_deleted = self.clear_resource_schedules(cancelation_date_list_by_resource, include_rr=True,
                                                      include_normal=True, preemption_enabled=True)
            # Cancel any remaining RRs not under a newly scheduled RR (needed in case weather knocks out a telescope
            # that previously had a RR scheduled on it)
            try:
                n_deleted += self.clear_resource_schedules(all_cancelation_date_list_by_resource, include_rr=True,
                                                           include_normal=False, preemption_enabled=False)
            except ScheduleException:
                self.log.warning("Failed to cancel existing rapid responses, but will continue scheduling new rapid responses this run.")

            n_aborted = self.abort_running_requests(abort_requests)
            # TODO: Shouldn't need to pass semester start in here.  Should denormalize reservations before calling save
            n_submitted = self.save_resource_schedules(scheduler_result.schedule,
                                                       semester_start)
            self._update_summary_events(n_deleted, n_aborted, n_submitted)

            return n_submitted
        return 0

    def apply_normal_result(self, scheduler_result, scheduler_input, resources_to_clear, apply_deadline):
        if self._can_apply_scheduler_result(scheduler_result, apply_deadline):
            semester_start = self.semester_details['start']
            semester_end = self.semester_details['end']
            new_schedule_resources = scheduler_result.resources_scheduled()

            cancelation_date_list_by_resource = self._determine_schedule_cancelation_start_dates(
                resources_to_clear, scheduler_result.schedule,
                scheduler_input.resource_usage_snapshot,
                apply_deadline,
                semester_end)

            # Find running requests that need to be aborted due to conflict with new schedule
            abort_requests_by_resource = {}
            for resource in new_schedule_resources:
                earliest_reservation = scheduler_result.earliest_reservation(resource)
                to_abort = self._determine_abort_requests(
                    scheduler_input.resource_usage_snapshot.request_groups_for_resource(resource),
                    semester_start,
                    earliest_reservation)
                abort_requests_by_resource[resource] = to_abort

            n_deleted, n_aborted, n_submitted = 0, 0, 0
            cancel_exception = None
            for resource in sorted(set(new_schedule_resources + resources_to_clear)):
                # These are resources at which the schedule must be updated in some way. Save and abort will
                # have the same resources. Cancel might be different, for example if a resource just became
                # unavailable, the schedule there would need to be canceled but no new schedule submitted. For this
                # reason, check if an action has the resource before doing it.
                try:
                    if resource in cancelation_date_list_by_resource:
                        n_deleted += self.clear_resource_schedules(
                            {resource: cancelation_date_list_by_resource[resource]},
                            include_rr=False,
                            include_normal=True,
                            preemption_enabled=False
                        )
                    if resource in abort_requests_by_resource:
                        n_aborted += self.abort_running_requests(abort_requests_by_resource[resource])

                    if resource in scheduler_result.schedule:
                        # TODO: Shouldn't need to pass semester start, should denormalize reservations before calling save
                        n_submitted += self.save_resource_schedules({resource: scheduler_result.schedule[resource]},
                                                                    semester_start)
                except ScheduleException as se:
                    # Canceling could fail if the cancel endpoint detects that an in_progress observation overlaps with
                    # the cancellation range. In this case we should just leave the schedule as-is on that site and wait
                    # for the next scheduling run to correct itself.
                    self.log.warning(f"Failed to cancel schedule at {resource}, skipping saving schedule this run")
                    cancel_exception = se
            self._update_summary_events(n_deleted, n_aborted, n_submitted)
            if cancel_exception:
                # If there was an exception raised during cancelling any of the sites, re-raise it here so the scheduler
                # knows to trigger a re-run immediately.
                raise cancel_exception
            return n_submitted
        return 0

    def _update_summary_events(self, n_deleted, n_aborted, n_submitted):
        self.summary_events.append("In total, deleted %d previously scheduled %s" % pl(n_deleted, 'observation'))
        self.summary_events.append("Aborted %d previously running %s" % pl(n_aborted, 'observation'))
        self.summary_events.append("Submitted %d new %s to the Observation Portal" % pl(n_submitted, 'observation'))

    def _write_scheduling_summary_log(self, header_msg):
        self.log.info("------------------")
        self.log.info(header_msg)
        if self.sched_params.dry_run:
            self.log.info("(DRY-RUN: No cancel or submit took place)")
        self.log.info("------------------")
        for msg in self.summary_events:
            self.log.info(msg)
        self.log.info("Scheduling complete.")
        self.summary_events = []

    @timeit
    @metric_timer('create_schedule', num_reservations=lambda x: x.count_reservations(),
                  rate=lambda x: x.count_reservations())
    def create_rr_schedule(self, scheduler_input):
        rr_scheduler_result = SchedulerResult()
        if scheduler_input.request_groups:
            self.log.info("Start Rapid Response Scheduling")
            rr_scheduling_start = scheduler_input.get_scheduling_start()
            deadline = rr_scheduling_start + self.estimated_rr_run_timedelta
            rr_scheduler_result = self.call_scheduler(scheduler_input, deadline)

            try:
                self.rr_scheduled_requests_by_rg = rr_scheduler_result.get_scheduled_requests_by_request_group_id()
                self.apply_rr_result(rr_scheduler_result, scheduler_input, deadline)
                rr_scheduling_end = datetime.utcnow()
                rr_scheduling_timedelta = rr_scheduling_end - rr_scheduling_start
                self.estimated_rr_run_timedelta = estimate_runtime(self.estimated_rr_run_timedelta,
                                                                   rr_scheduling_timedelta)
                self.log.info("Rapid Response scheduling took %.2f seconds" % rr_scheduling_timedelta.total_seconds())
                self._write_scheduling_summary_log("Rapid Response Scheduling Summary")

            except EstimateExceededException as eee:
                self.log.warn(
                    "Not enough time left to apply schedule before estimated scheduler end. Schedule will not be saved.")
                rr_scheduling_timedelta = eee.new_estimate - rr_scheduling_start
                self.estimated_rr_run_timedelta = estimate_runtime(self.estimated_rr_run_timedelta,
                                                                   rr_scheduling_timedelta)
                self.log.warn(
                    "Skipping normal scheduling loop to try Rapid Response scheduling again with new runtime estimate")
                raise eee
            finally:
                self.log.info("New runtime estimate is %.2f seconds" % self.estimated_rr_run_timedelta.total_seconds())
                self.log.info("End Rapid Response Scheduling")
        else:
            self.log.warn("Received no Rapid Respond Request Groups! Skipping RR scheduling cycle")
        return rr_scheduler_result

    @timeit
    @metric_timer('create_schedule', num_reservations=lambda x: x.count_reservations(),
                  rate=lambda x: x.count_reservations())
    def create_normal_schedule(self, scheduler_input):
        # Run the scheduling loop, if there are any Request Groups
        scheduler_result = SchedulerResult()
        if scheduler_input.request_groups:
            self.log.info("Start Normal Scheduling")
            normal_scheduling_start = datetime.utcnow()
            deadline = scheduler_input.get_scheduling_start() + self.estimated_normal_run_timedelta

            if self.sched_params.profiling_enabled:
                import cProfile
                prof = cProfile.Profile()
                scheduler_result = prof.runcall(self.call_scheduler, scheduler_input)
                prof.dump_stats('call_scheduler.pstat')
            else:
                scheduler_result = self.call_scheduler(scheduler_input, deadline)
            resources_to_clear = list(self.network_model.keys())
            try:
                before_apply = datetime.utcnow()
                self.normal_scheduled_requests_by_rg = scheduler_result.get_scheduled_requests_by_request_group_id()
                n_submitted = self.apply_normal_result(scheduler_result,
                                                       scheduler_input,
                                                       resources_to_clear, deadline)
                after_apply = datetime.utcnow()
                if (n_submitted > 0):
                    self.avg_save_time_per_reservation_timedelta = timedelta(
                        seconds=(after_apply - before_apply).total_seconds() / n_submitted)
                    self.log.info(
                        "Avg save time per reservation was %.2f seconds" % self.avg_save_time_per_reservation_timedelta.total_seconds())
                normal_scheduling_end = datetime.utcnow()
                normal_scheduling_timedelta = normal_scheduling_end - normal_scheduling_start
                self.estimated_normal_run_timedelta = estimate_runtime(self.estimated_normal_run_timedelta,
                                                                       normal_scheduling_timedelta)
                self.log.info("Normal scheduling took %.2f seconds" % normal_scheduling_timedelta.total_seconds())
                self._write_scheduling_summary_log("Normal Scheduling Summary")
            except EstimateExceededException as eee:
                self.log.warn(
                    "Not enough time left to apply schedule before estimated scheduler end.  Schedule will not be saved.")
                normal_scheduling_timedelta = eee.new_estimate - normal_scheduling_start
                self.estimated_normal_run_timedelta = estimate_runtime(self.estimated_normal_run_timedelta,
                                                                       normal_scheduling_timedelta)
                raise eee
            finally:
                self.log.info(
                    "New runtime estimate is %.2f seconds" % self.estimated_normal_run_timedelta.total_seconds())
                self.log.info("End Normal Scheduling")
        else:
            self.log.warn("Received no Normal Request Groups! Skipping Normal scheduling cycle")

        return scheduler_result

    @timeit
    def create_new_schedule(self, network_state_timestamp):
        rr_scheduler_result = self.scheduling_cycle(RR_OBSERVATION_TYPE, network_state_timestamp)
        set_schedule_type(None)
        # Pass in the rr_schedule_result to the normal scheduling run to block off the times that are
        # scheduled for RRs from normal scheduling.
        self.scheduling_cycle(NORMAL_OBSERVATION_TYPE, network_state_timestamp, rr_scheduler_result)
        set_schedule_type(None)

    @metric_timer('scheduling_cycle', num_reservations=lambda x: x.count_reservations(),
                  rate=lambda x: x.count_reservations())
    def scheduling_cycle(self, schedule_type, network_state_timestamp, rr_schedule_result=None):
        set_schedule_type(schedule_type)
        result = None
        if schedule_type == NORMAL_OBSERVATION_TYPE:
            scheduler_input = self.input_factory.create_normal_scheduling_input(
                self.estimated_normal_run_timedelta.total_seconds(),
                scheduled_requests_by_rg=self.normal_scheduled_requests_by_rg,
                rr_schedule=rr_schedule_result.schedule,
                network_state_timestamp=network_state_timestamp)
            result = self.create_normal_schedule(scheduler_input)
        elif schedule_type == RR_OBSERVATION_TYPE:
            scheduler_input = self.input_factory.create_rr_scheduling_input(
                self.estimated_rr_run_timedelta.total_seconds(),
                scheduled_requests_by_rg=self.rr_scheduled_requests_by_rg,
                network_state_timestamp=network_state_timestamp)
            result = self.create_rr_schedule(scheduler_input)
        return result


class EstimateExceededException(Exception):

    def __init__(self, msg, new_estimate):
        super().__init__(self, msg)
        self.new_estimate = new_estimate
