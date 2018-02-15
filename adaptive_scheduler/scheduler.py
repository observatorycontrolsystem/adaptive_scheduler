from __future__ import division

import time
import logging
import itertools
import json

from collections import defaultdict
from datetime import datetime, timedelta

from adaptive_scheduler.feedback         import TimingLogger
from adaptive_scheduler.interfaces       import ScheduleException
from adaptive_scheduler.event_utils      import report_scheduling_outcome
from adaptive_scheduler.kernel.intervals import Intervals
from adaptive_scheduler.utils            import (timeit, iso_string_to_datetime, estimate_runtime, SendMetricMixin,
                                            metric_timer, set_schedule_type, NORMAL_SCHEDULE_TYPE, TOO_SCHEDULE_TYPE,
                                                 get_reservation_datetimes, time_in_capped_intervals, cap_intervals)
from adaptive_scheduler.printing         import pluralise as pl
from adaptive_scheduler.printing         import plural_str
from adaptive_scheduler.printing         import print_compound_reservations,summarise_urs, log_full_ur, log_windows
from adaptive_scheduler.model2           import filter_out_compounds, differentiate_by_type, n_requests
from adaptive_scheduler.kernel_mappings  import (construct_visibilities,
                                                 construct_resource_windows,
                                                 make_compound_reservations,
                                                 make_many_type_compound_reservations,
                                                 filter_for_kernel,
                                                 construct_global_availability)
from adaptive_scheduler.request_filters  import filter_urs, drop_empty_requests, set_now
from adaptive_scheduler.valhalla_connections import ValhallaConnectionError
from adaptive_scheduler.downtime_connections import DowntimeError, DowntimeInterface


class Scheduler(object, SendMetricMixin):

    def __init__(self, kernel_class, sched_params, event_bus):
        self.kernel_class = kernel_class
        self.visibility_cache = {}
        self.sched_params = sched_params
        self.event_bus = event_bus
        self.log = logging.getLogger(__name__)
        if self.sched_params.simulate_now:
            self.estimated_scheduler_end = iso_string_to_datetime(self.sched_params.simulate_now)
        else:
            self.estimated_scheduler_end = datetime.utcnow()
        self.scheduler_summary_messages = []


    def find_resources_to_preempt(self, preemtion_urs, all_urgent_urs, resources, resource_usage_snapshot, all_ur_priorities):
        ''' Preempt running requests, if needed, to run urgent user requests'''

        # make copy of resource list since it could be modified
        copy_of_resources = list(resources)

        # Don't preemt another urgent request
        # Remove any resource with running urgent requests from resource list
        all_urgent_tracking_numbers = [ur.tracking_number for ur in all_urgent_urs]
        for resource in resources:
            for running_ur in resource_usage_snapshot.user_requests_for_resource(resource):
                if running_ur.tracking_number in all_urgent_tracking_numbers:
                    copy_of_resources.remove(resource)

        value_function_dict = self.construct_value_function_dict(preemtion_urs, copy_of_resources, resource_usage_snapshot, all_ur_priorities)

        preemtion_tracking_numbers = [ur.tracking_number for ur in preemtion_urs]
        optimal_combination = self.compute_optimal_combination(value_function_dict, preemtion_tracking_numbers, copy_of_resources)

        # get resources where the cost of canceling is lowest
        resources_to_cancel = [ combination[0] for combination in optimal_combination ]

        return resources_to_cancel


    # TODO - Move to a utils library
    def combine_excluded_intervals(self, excluded_intervals_1, excluded_intervals_2):
        ''' Combine two dictionaries where Intervals are the values '''
        for key in excluded_intervals_2:
            timepoints = excluded_intervals_2[key].timepoints
            excluded_intervals_1.setdefault(key, Intervals([])).add(timepoints)

        return excluded_intervals_1

    def construct_value_function_dict(self, urgent_urs, resources, resource_usage_snapshot, ur_priorities):
        ''' Constructs a value dictionary of tuple (telescope, tracking_number) to value
    
            where value = too priority / running block priority or if no block is running at
            that telescope, value = too priority
    
            NOTE: Assumes running block priority is above 1
        '''

#         normal_tracking_numbers_dict = {ur.tracking_number : ur for ur in normal_urs}

        tracking_number_to_resource_map = defaultdict(set)
        for urgent_ur in urgent_urs:
            tracking_number = urgent_ur.tracking_number

            if urgent_ur.n_requests() > 1:
                msg = "TOO UR %s has more than one child R, which is not supported." % tracking_number
                msg += " Submit as separate requests."
                self.log.info(msg)
                continue

            for request in urgent_ur.requests:
                for resource, windows in request.windows:
                    tracking_number_to_resource_map[tracking_number].add(resource)

        value_function_dict = {};
        for resource in resources:
            running_ur_list = resource_usage_snapshot.user_requests_for_resource(resource)
            # Compute the priority of the the telescopes without ToOs
            # use a priority of 1 for telescopes without a running block
            running_request_priority = 1
            for running_at_tel in running_ur_list:
                running_tracking_number = running_at_tel.tracking_number
                normal_ur_priority = 0
                normal_ur_priority += ur_priorities.get(running_tracking_number, 0)
#                 else:
#                     # The running request wasn't included in the list of schedulable urs so we don't know it's priority
#                     # This could happen if the running request has been canceled.  In this case treat it like nothing is running
#                     # Not sure if there are other ways this can happen.  Beware....
#                     # TODO: add function unit test for this case
#                     normal_ur_priority = 1
                # Take the greater of the running priorities.  Should it be the sum? Something else?
                if normal_ur_priority > running_request_priority:
                    running_request_priority = normal_ur_priority

            for ur in urgent_urs:
                tracking_number = ur.tracking_number
                if resource in tracking_number_to_resource_map[tracking_number]:
                    too_priority = ur.get_priority()
                    # Make sure __future__ division is imported to make this work correctly
                    value_function_dict[(resource, tracking_number)] = too_priority / running_request_priority

        return value_function_dict


    def compute_optimal_combination(self, value_dict, tracking_numbers, resources):
        '''
        Compute combination of telescope to tracking number that has the highest value
    
        NOTE: This schedule assumes that each there will a tracking number only needs one
              telescope to run (no compound requests).
        '''
        if len(tracking_numbers) < len(resources):
            small_list = tracking_numbers
            large_list = resources
            zip_combinations = lambda x : zip(x, small_list)
        else:
            large_list = tracking_numbers
            small_list = resources
            zip_combinations = lambda x : zip(small_list, x)

        optimal_combination_value = -1
        optimal_combinations = []

        # Create all possible permutations of of the large list of length <= the length of the small list
        # Handles the cases where not all ToO UR's are possible due to a resource being unavailable
        permutations = []
        for i in range(len(small_list)):
            permutations.extend(itertools.permutations(large_list, i + 1))
            
        for x in permutations:
            combinations = zip_combinations(x)
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


    def remove_singles(self, user_reqs):
        self.log.info("Compound Request support (single) disabled at the command line")
        self.log.info("Compound Requests of type 'single' will be ignored")
        singles, others = differentiate_by_type(cr_type='single', crs=user_reqs)

        return others


    def remove_compounds(self, user_reqs):
        self.log.info("Compound Request support (and/oneof/many) disabled at the command line")
        self.log.info("Compound Requests of type 'and', 'oneof' or 'many' will be ignored")

        return filter_out_compounds(user_reqs)


    def scheduling_horizon(self, estimated_scheduler_end):
        return estimated_scheduler_end + timedelta(days=self.sched_params.horizon_days)


    def apply_unschedulable_filters(self, user_reqs, estimated_scheduler_end, running_request_numbers):
        ''' Returns tuple of (schedulable, unschedulable) user requests where UR's
        in the unschedulable list will never be possible
        '''
        return user_reqs, []


    def apply_window_filters(self, user_reqs, estimated_scheduler_end, semester_details):
        ''' Returns the set of URs with windows adjusted to include only URs with windows
        suitable for scheduling
        '''
        return user_reqs


    def prepare_for_kernel(self, window_adjusted_urs, semester_details):
        ''' Convert UR model to formalization expected by the scheduling kernel
        '''
        return []


    def prepare_available_windows_for_kernel(self, available_resources, resource_interval_mask, semester_details):
        ''' Construct the set of resource windows available for use in scheduling
        '''
        return []


    # TODO: replace with event bus event
    def on_run_scheduler(self, user_requests, semester_details):
        ''' Handler called at beginning run_schedule
        '''
        pass


    # TODO: replace with event bus event
    def after_unschedulable_filters(self, user_requests):
        ''' Handler called after unschedulable filters have been applied
        '''
        pass


    # TODO: replace with event bus event
    def after_window_filters(self, user_requests):
        ''' Handler called after window filters have been applied
        '''
        pass


    # TODO: replace with event bus event
    def on_new_schedule(self, new_schedule, compound_reservations, estimated_scheduler_end):
        ''' Handler called on completion of a scheduler run
        '''
        pass


    def save_schedule(self, schedule, estimated_scheduler_end, semester_details, is_too):
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
                    res_data = {'request_number': reservation.request.request_number,
                                'priority': reservation.priority,
                                'start': reservation_start.isoformat(),
                                'end': reservation_end.isoformat(),
                                'resource': resource}
                    priority_value += reservation.priority
                    schedule_data['resources'][resource]['reservations'].append(res_data)
                schedule_data['resources'][resource]['priority_value'] = priority_value
                schedule_data['total_priority_value'] += priority_value

                #also store the intervals at the site that were considered
                dark_intervals = self.visibility_cache[resource][1]()
                # get the dark intervals of the site, capped by the scheduled range of time (scheduler end + 7 days)
                capped_dark_intervals = cap_intervals(dark_intervals, estimated_scheduler_end,
                                                      self.scheduling_horizon(estimated_scheduler_end))
                for interval in capped_dark_intervals:
                    schedule_data['resources'][resource]['dark_intervals'].append({'start': interval[0].isoformat(),
                                                                      'end': interval[1].isoformat()})


            if is_too:
                schedule_type = TOO_SCHEDULE_TYPE
            else:
                schedule_type = NORMAL_SCHEDULE_TYPE
            file_timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
            filename = '{}_schedule_{}.json'.format(schedule_type, file_timestamp)
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
                dark_intervals = self.visibility_cache[resource][1]()
                # get the dark intervals of the site, capped by the scheduled range of time (scheduler end + horizon)
                available_seconds_for_horizon = time_in_capped_intervals(dark_intervals, estimated_scheduler_end,
                                                             self.scheduling_horizon(estimated_scheduler_end))

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
            self.log.info("telescope {} filled {} / {} hours".format(
                resource, (scheduled_seconds_for_horizon / 3600.0), (available_seconds_for_horizon / 3600.0)))
            self._send_schedule_metrics(resource, scheduled_seconds_for_horizon, available_seconds_for_horizon, horizon_days)
            if horizon_days != 1:
                self._send_schedule_metrics(resource, scheduled_seconds_for_one_day, available_seconds_for_one_day, 1)

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


    def create_resource_mask(self, available_resources, resource_usage_snapshot, too_tracking_numbers, preemption_enabled):
        resource_interval_mask = {}
        for resource_name in available_resources:
            running_user_requests = resource_usage_snapshot.user_requests_for_resource(resource_name)
            # Limit to only ToO running user request when preemption is enabled
            if preemption_enabled:
                running_user_requests = [ur for ur in running_user_requests if ur.tracking_number in too_tracking_numbers]
            
            masked_timepoints_for_resource = []
            for ur in running_user_requests:
                for r in ur.running_requests:
                    if r.should_continue():
                        masked_timepoints_for_resource.extend(r.timepoints())
            resource_interval_mask[resource_name] = Intervals(masked_timepoints_for_resource)
            resource_interval_mask[resource_name].add(resource_usage_snapshot.blocked_intervals(resource_name).timepoints)
            
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
        user_reqs = scheduler_input.user_requests

        resource_usage_snapshot = scheduler_input.resource_usage_snapshot
        available_resources = scheduler_input.available_resources
        all_ur_priorities = scheduler_input.user_request_priorities

        self.estimated_scheduler_end = estimated_scheduler_end
        self.on_run_scheduler(user_reqs, semester_details)

        if self.sched_params.no_singles:
            user_reqs = self.remove_singles(user_reqs)

        if self.sched_params.no_compounds:
            user_reqs = self.remove_compounds(user_reqs)

        running_requests = resource_usage_snapshot.running_requests_for_resources(available_resources)
        # A request should only be filtered from the scheduling input if it has a chance of completing successfully
        running_request_numbers = [r.request_number for r in running_requests if r.should_continue()]
        schedulable_urs, unschedulable_urs = self.apply_unschedulable_filters(user_reqs, estimated_scheduler_end,
                                                                              running_request_numbers)

        # Remove child request from the parent user request when the request has no windows remaining
        drop_empty_requests(schedulable_urs)
        self.after_unschedulable_filters(schedulable_urs)

        window_adjusted_urs = self.apply_window_filters(schedulable_urs, estimated_scheduler_end, semester_details)
        self.after_window_filters(window_adjusted_urs)

# Optimization of ToO output for least impact doesn't work as planned.  See https://issues.lcogt.net/issues/7851
#         # Pre-empt running blocks
#         if preemption_enabled:
#             resource_schedules_to_cancel = self.find_resources_to_preempt(window_adjusted_urs, user_reqs, resources_to_schedule, resource_usage_snapshot, all_ur_priorities)
#             # Need a copy because the original is modified inside the loop
#             copy_of_resources_to_schedule = list(resources_to_schedule)
#             for resource in copy_of_resources_to_schedule:
#                 if not resource in resource_schedules_to_cancel:
#                     self.log.info("Removing %s from schedulable resources.  Not needed for ToO." % resource)
#                     resources_to_schedule.remove(resource)
#         else:
#             resource_schedules_to_cancel = available_resources

        # By default, schedule on all resources
        resources_to_schedule = list(available_resources)
        resource_interval_mask = self.create_resource_mask(available_resources, resource_usage_snapshot,
                                                           scheduler_input.too_tracking_numbers, preemption_enabled)

        compound_reservations = self.prepare_for_kernel(window_adjusted_urs, semester_details)
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

            kernel = self.kernel_class(compound_reservations, available_windows, contractual_obligations,
                                       self.sched_params.slicesize_seconds)
            scheduler_result.schedule = kernel.schedule_all(timelimit=self.sched_params.timelimit_seconds)

            # TODO: Remove resource_schedules_to_cancel from Scheduler result, this should be managed at a higher level
            # Limit canceled resources to those where user_requests were canceled
            if(preemption_enabled):
                for resource in available_resources:
                    if scheduler_result.schedule.get(resource, []) == []:
                        scheduler_result.resource_schedules_to_cancel.remove(resource)

            # Do post scheduling stuff
            self.save_schedule(scheduler_result.schedule, estimated_scheduler_end, semester_details, preemption_enabled)
            self.produce_schedule_metrics(scheduler_result.schedule, estimated_scheduler_end,
                                          semester_details)
            self.on_new_schedule(scheduler_result.schedule, compound_reservations, estimated_scheduler_end)
        else:
            self.log.info("Nothing to schedule! Skipping kernel call...")
            scheduler_result.resource_schedules_to_cancel = {}

        return scheduler_result


class LCOGTNetworkScheduler(Scheduler):

    def __init__(self, kernel_class, sched_params, event_bus, network_model):
        Scheduler.__init__(self, kernel_class, sched_params, event_bus)

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


    def _log_ur_input_details(self, user_reqs, estimated_scheduler_end):
        # Summarise the User Requests we've received
        n_urs, n_rs = n_requests(user_reqs)

        self.log.info("Deserialised %s (%s) from Request DB", plural_str(n_urs, 'User Request'),
                                                         plural_str(n_rs, 'Request'))

        summarise_urs(user_reqs, log_msg="Received from Request DB")
        for ur in user_reqs:
            log_full_ur(ur, estimated_scheduler_end)
            log_windows(ur, log_msg="Initial windows:")

    @metric_timer('apply_unschedulable_filters')
    def apply_unschedulable_filters(self, user_reqs, estimated_scheduler_end, running_request_numbers):
        ''' Returns tuple of (schedulable, unschedulable) user requests where UR's
        in the unschedulable list will never be possible
        '''
        # Filter by window, and set UNSCHEDULABLE on the Request DB as necessary
        self.log.info("Filtering for unschedulability")

        set_now(estimated_scheduler_end)
        schedulable_urs, unschedulable_urs = filter_urs(user_reqs, running_request_numbers)
        self.log.info("Found %d unschedulable %s after filtering", *pl(len(unschedulable_urs), 'UR'))

        return schedulable_urs, unschedulable_urs

    @metric_timer('apply_window_filters', num_requests=lambda x: len(x))
    def apply_window_filters(self, user_reqs, estimated_scheduler_end, semester_details):
        ''' Returns the set of URs with windows adjusted to include only URs with windows
        suitable for scheduling
        '''
        self.log.info("Filtering on dark/rise_set")

        semester_end = semester_details['end']

        downtime_interface = DowntimeInterface(self.sched_params.downtime_url)
        try:
            downtime_intervals = downtime_interface.get_downtime_intervals_by_resource(start=estimated_scheduler_end,
                                                                                      end=self.scheduling_horizon(estimated_scheduler_end))
        except DowntimeError as e:
            self.log.warning("Problem getting downtime intervals: {}".format(repr(e)))
            downtime_intervals = {}

        filtered_window_user_reqs = filter_for_kernel(user_reqs, self.visibility_cache, downtime_intervals,
                                                      estimated_scheduler_end, semester_end,
                                                      self.scheduling_horizon(estimated_scheduler_end))

        return filtered_window_user_reqs

    @metric_timer('prepare_for_kernel', num_requests=lambda x: len(x))
    def prepare_for_kernel(self, window_adjusted_urs, semester_details):
        ''' Convert UR model to formalization expected by the scheduling kernel
        '''
        # Uncomment these next lines to make the output match the old scheduler/requestdb
        # window_adjusted_urs.sort(key=lambda x: int(x.tracking_number))
        # for ur in window_adjusted_urs:
        #     ur.requests.sort(key=lambda x: int(x.request_number))

        # Convert CompoundRequests -> CompoundReservations

        semester_start = semester_details['start']

        many_urs, other_urs = differentiate_by_type('many', window_adjusted_urs)
        many_compound_reservations = make_many_type_compound_reservations(many_urs, self.visibility_cache,
                                                                semester_start)
        other_compound_reservations = make_compound_reservations(other_urs, self.visibility_cache,
                                                       semester_start)
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


    def on_run_scheduler(self, user_requests, semester_details):
        self._log_scheduler_start_details(self.estimated_scheduler_end, semester_details)
        self._log_ur_input_details(user_requests, self.estimated_scheduler_end)

        semester_start = semester_details['start']
        semester_end = semester_details['end']

        # Construct visibility objects for each telescope
        self.log.info("Constructing telescope visibilities")
        if not self.visibility_cache:
            self.visibility_cache = construct_visibilities(self.network_model, semester_start, semester_end)


    def after_unschedulable_filters(self, user_requests):
        summarise_urs(user_requests, log_msg="Passed unschedulable filters:")

        for ur in user_requests:
            log_windows(ur, log_msg="Remaining windows:")


    def after_window_filters(self, user_requests):
        self.log.info("Completed dark/rise_set filters")
        summarise_urs(user_requests, log_msg="Passed dark/rise filters:")
        for ur in user_requests:
            log_windows(ur, log_msg="Remaining windows:")

        self.log.info('Filtering complete. Ready to construct Reservations from %d URs.' % len(user_requests))


    def on_new_schedule(self, new_schedule, compound_reservations, estimated_scheduler_end):
        ''' Handler called on completion of a scheduler run
        '''
        scheduled_compound_reservations = []
        [scheduled_compound_reservations.extend(a) for a in new_schedule.values()]
        self.log.info("Scheduling completed. Final schedule has %d Reservations." % len(scheduled_compound_reservations))

        report_scheduling_outcome(compound_reservations, scheduled_compound_reservations)


class SchedulerResult(object):
    '''Aggregates together output of a scheduler run
    '''

    def __init__(self, schedule={}, resource_schedules_to_cancel=[]):
        '''
        schedule - Expected to be a dict mapping resource to scheduled reservations
        resource_schedules_to_cancel - List of resources to cancel schedules on
        unschedulable_user_request_numbers - Tracking numbers of user requests considered unschedulable
        unschedulable_request_numbers = Request numbsers of requests considered unschedulable
        '''
        self.schedule = schedule
        self.resource_schedules_to_cancel = resource_schedules_to_cancel


    def count_reservations(self):
        reservation_cnt = 0
        for resource, reservations in self.schedule.items():
            reservation_cnt += len(reservations)

        return reservation_cnt


    def get_scheduled_requests_by_tracking_num(self):
        scheduled_requests_by_tracking_num = {}
        for reservations in self.schedule.values():
            for reservation in reservations:
                request_num = reservation.request.request_number
                tracking_num = reservation.user_request.tracking_number
                if not tracking_num in scheduled_requests_by_tracking_num:
                    scheduled_requests_by_tracking_num[tracking_num] = {}
                scheduled_requests_by_tracking_num[tracking_num][request_num] = reservation
        return scheduled_requests_by_tracking_num


    def resources_scheduled(self):
        return self.schedule.keys()


    def earliest_reservation(self, resource):
        earliest = None
        reservations = list(self.schedule.get(resource, []))
        if reservations:
            reservations.sort(cmp=lambda x, y : cmp(x.scheduled_start, y.scheduled_start))
            earliest = reservations[0]

        return earliest


class SchedulerRunner(object):

    def __init__(self, sched_params, scheduler, network_interface, network_model, input_factory):
        self.run_flag = True
        self.sched_params = sched_params
        self.scheduler = scheduler
        self.network_interface = network_interface
        self.network_model = network_model
        self.input_factory = input_factory
        self.normal_scheduled_requests_by_ur = {}
        self.too_scheduled_requests_by_ur = {}
        self.log = logging.getLogger(__name__)
        # List of strings to be printed in final scheduling summary
        self.summary_events = []

        self.estimated_too_run_timedelta = timedelta(seconds=sched_params.too_runtime_seconds)
        self.estimated_normal_run_timedelta = timedelta(seconds=sched_params.normal_runtime_seconds)
        self.avg_save_time_per_reservation_timedelta = timedelta(seconds=sched_params.avg_reservation_save_time_seconds)
        self.first_run = True
        self.semester_details = None


    def scheduler_rerun_required(self):
        ''' Return True if scheduler should be run now
        '''
        network_has_changed = False

        if self.network_interface.current_events_has_changed():
            self.log.info("Telescope network events were found.")
            network_has_changed = True
        elif self.sched_params.no_weather:
            self.log.info("Ignoring Telescope network events, but setting network change flag to True.")
            network_has_changed = True

        # Must call this function to force the requestdb to update it's internal states
        # but don't do on a dry run because it changes the network state
        request_set_changed = True
        if not self.sched_params.dry_run:
            request_set_changed = self.network_interface.schedulable_request_set_has_changed()

        return network_has_changed or request_set_changed


    @timeit
    @metric_timer('update_network_model')
    def update_network_model(self):
        current_events = self.network_interface.get_current_events()
        available_telescopes = []
        for telescope_name, telescope in self.network_model.iteritems():
            if telescope_name in current_events:
                telescope['events'].extend(current_events[telescope_name])
                msg = "Found network event for '%s' - removing from consideration (%s)" % (
                                                                    telescope_name,
                                                                    current_events[telescope_name])
                self.log.info(msg)
            else:
                available_telescopes.append(telescope_name)
                telescope['events'] = []
        self.network_interface.send_available_telescope_state_events(available_telescopes)
        return

    def get_semester_details(self, date):
        '''Attempts to get the semester details for the current date (if it is not the current semester).
           If it fails, previous semester details will be returned.
        '''
        try:
            self.semester_details = self.network_interface.valhalla_interface.get_semester_details(date)
        except ValhallaConnectionError as e:
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
        scheduler_run_start = self.input_factory.input_provider.get_scheduler_now()
        try:
            self.semester_details = self.get_semester_details(scheduler_run_start)
            self.network_interface.configdb_interface.update_configdb_structures()
            if self.scheduler_rerun_required() or self.first_run:
                self.create_new_schedule(scheduler_run_start)
        except (ValhallaConnectionError, ScheduleException, EstimateExceededException) as eee:
            self.log.warning("Skipping Scheduling Run: {}".format(repr(eee)))
            # Estimated run time was exceeded so exception was raised
            # to short circuit to exit.  Just try again.  Run time
            # estimate should have been updated.
            pass


    def call_scheduler(self, scheduler_input, estimated_scheduler_end):
        self.log.info("Using a 'now' of %s", scheduler_input.scheduler_now)
        self.log.info("Estimated scheduler run time is %.2f seconds", scheduler_input.estimated_scheduler_runtime.total_seconds())
        self.log.info("Estimated scheduler end %s", estimated_scheduler_end)
        n_urs, n_rs = n_requests(scheduler_input.user_requests)
        self.summary_events.append("Received %d %s (%d %s) from Request DB" % (pl(n_urs, 'User Request') + pl(n_rs, 'Request')))
        scheduler_result = None
        try:
            scheduler_result = self.scheduler.run_scheduler(scheduler_input, estimated_scheduler_end,
                                                            self.semester_details,
                                                            preemption_enabled=scheduler_input.is_too_input)
        except ScheduleException as se:
            self.log.error(se, "aborting run")

        return scheduler_result


    def clear_resource_schedules(self, cancelation_dates_by_resource):
        n_deleted = self.network_interface.cancel(cancelation_dates_by_resource, "Superceded by new schedule")

        return n_deleted

    def abort_running_requests(self, abort_requests):
        for rr, reasons in abort_requests:
            reason = ', '.join(reasons)
            self.network_interface.abort(rr, reason)

    def save_resource_schedules(self, schedule, denormalization_date):
        n_submitted = self.network_interface.save(schedule, denormalization_date, False)

        return n_submitted

    def _determine_resource_cancelation_start_date(self, scheduled_reservations, running_user_requests, default_cancelation_start_date):
        cancelation_start_date = default_cancelation_start_date
        for running_user_request in running_user_requests:
            for running_request in running_user_request.running_requests:
                if running_request.end > cancelation_start_date:
                    cancelation_start_date = running_request.end

        return cancelation_start_date

    def _determine_schedule_cancelation_start_dates(self, resources_to_cancel, schedule_by_resource, resource_usage_snapshot, default_cancelation_start, default_cancelation_end):
        cancelation_dates_by_resource = {}
        for resource in resources_to_cancel:
            scheduled_at_resource = schedule_by_resource.get(resource, [])
            start = self._determine_resource_cancelation_start_date(scheduled_at_resource, resource_usage_snapshot.user_requests_for_resource(resource), default_cancelation_start)
            end = default_cancelation_end
            cancelation_dates_by_resource[resource] = (start, end)

        return cancelation_dates_by_resource


    def _determine_abort_requests(self, running_user_requests, schedule_denoramlization_date, earliest_reservation):
        abort_requests = []

        for running_user_request in running_user_requests:
            abort_reasons = []
            for running_request in running_user_request.running_requests:
                # if it can't complete successfully, cancel it
                if not running_request.should_continue():
                    abort_reasons.append("Can not complete successfully: " + ",".join(running_request.errors()))

                # if it interferes with the new schedule, cancel it
                if earliest_reservation:
                    earlist_res_denormalized_start = schedule_denoramlization_date + timedelta(seconds=earliest_reservation.scheduled_start)
                    if running_request.end > earlist_res_denormalized_start:
                            abort_reasons.append("Request interrupted to observe request: %s" % earliest_reservation.request.request_number)
            if abort_reasons:
                abort_requests.append((running_request, abort_reasons))

        return abort_requests


    def apply_scheduler_result(self, scheduler_result, scheduler_input, resources_to_clear, apply_deadline):
        if self.sched_params.dry_run:
            self.log.warn("Dry run. Schedule will not be saved.")
            return 0

        if not scheduler_result:
            self.log.warn("Empty scheduler result. Schedule will not be saved.")
            return 0

        estimated_apply_timedelta = self.avg_save_time_per_reservation_timedelta * scheduler_result.count_reservations()
        estimated_apply_completion = datetime.utcnow() + estimated_apply_timedelta
        self.log.info("Estimated time to apply scheduler result is %.2f seconds" % estimated_apply_timedelta.total_seconds())
        if estimated_apply_completion > apply_deadline:
            raise EstimateExceededException("Estimated end time %s is after deadline %s" % (str(estimated_apply_completion), str(apply_deadline)), estimated_apply_completion)

        semester_start = self.semester_details['start']
        semester_end = self.semester_details['end']

        # TODO: make sure this cancels anything currently running in the ToO case
        cancelation_dates_by_resource = self._determine_schedule_cancelation_start_dates(
            resources_to_clear, scheduler_result.schedule,
            scheduler_input.resource_usage_snapshot,
            apply_deadline,
            semester_end)

        # Find running requests that need to be aborted due to conflict with new schedule
        abort_requests = []
        for resource in scheduler_result.resources_scheduled():
            earliest_reservation = scheduler_result.earliest_reservation(resource)
            to_abort = self._determine_abort_requests(scheduler_input.resource_usage_snapshot.user_requests_for_resource(resource),
                                                      semester_start,
                                                      earliest_reservation)
            abort_requests.extend(to_abort)

        n_deleted = self.clear_resource_schedules(cancelation_dates_by_resource)
        n_aborted = self.abort_running_requests(abort_requests)
        # TODO: Shouldn't need to pass semester start in here.  Should denormalize reservations before calling save
        n_submitted = self.save_resource_schedules(scheduler_result.schedule,
                                                   semester_start)
        self.summary_events.append("In total, deleted %d previously scheduled %s" % pl(n_deleted, 'block'))
        self.summary_events.append("Submitted %d new %s to the POND" % pl(n_submitted, 'block'))

        return n_submitted

    def _write_scheduling_summary_log(self, header_msg):
        self.log.info("------------------")
        self.log.info(header_msg)
        if self.sched_params.dry_run:
            self.log.info("(DRY-RUN: No delete or submit took place)")
        self.log.info("------------------")
        for msg in self.summary_events:
            self.log.info(msg)
        self.log.info("Scheduling complete.")
        self.summary_events = []


    @timeit
    @metric_timer('create_schedule', num_reservations=lambda x: x.count_reservations(), rate=lambda x: x.count_reservations())
    def create_too_schedule(self, scheduler_input):
        too_scheduler_result = SchedulerResult()
        if scheduler_input.user_requests:
            self.log.info("Start ToO Scheduling")
            too_scheduling_start = scheduler_input.get_scheduling_start()
            deadline = too_scheduling_start + self.estimated_too_run_timedelta
            too_scheduler_result = self.call_scheduler(scheduler_input, deadline)
            # Find resource scheduled by ToO run and cancel their schedules
            too_resources = []
            if too_scheduler_result:
                for too_resource, reservation_list in too_scheduler_result.schedule.iteritems():
                    if reservation_list:
                        too_resources.append(too_resource)
            try:
                self.too_scheduled_requests_by_ur = too_scheduler_result.get_scheduled_requests_by_tracking_num()
                self.apply_scheduler_result(too_scheduler_result,
                                                scheduler_input,
                                                too_resources,
                                                deadline)
                too_scheduling_end = datetime.utcnow()
                too_scheduling_timedelta = too_scheduling_end - too_scheduling_start
                self.estimated_too_run_timedelta = estimate_runtime(self.estimated_too_run_timedelta, too_scheduling_timedelta)
                self.log.info("ToO scheduling took %.2f seconds" % too_scheduling_timedelta.total_seconds())
                self._write_scheduling_summary_log("ToO Scheduling Summary")

            except EstimateExceededException, eee:
                self.log.warn("Not enough time left to apply schedule before estimated scheduler end.  Schedule will not be saved.")
                too_scheduling_timedelta = eee.new_estimate - too_scheduling_start
                self.estimated_too_run_timedelta = estimate_runtime(self.estimated_too_run_timedelta, too_scheduling_timedelta)
                self.log.warn("Skipping normal scheduling loop to try ToO scheduling again with new runtime estimate")
                raise eee
            finally:
                self.log.info("New runtime estimate is %.2f seconds" % self.estimated_too_run_timedelta.total_seconds())
                self.log.info("End ToO Scheduling")
        else:
            self.log.warn("Received no ToO User Requests! Skipping ToO scheduling cycle")
        return too_scheduler_result


    @timeit
    @metric_timer('create_schedule', num_reservations=lambda x: x.count_reservations(), rate=lambda x: x.count_reservations())
    def create_normal_schedule(self, scheduler_input, dont_cancel_resources):
        # Run the scheduling loop, if there are any User Requests
        scheduler_result = SchedulerResult()
        if scheduler_input.user_requests:
            self.log.info("Start Normal Scheduling")
            normal_scheduling_start =  datetime.utcnow()
            deadline = scheduler_input.get_scheduling_start() + self.estimated_normal_run_timedelta
            
            if self.sched_params.profiling_enabled:
                import cProfile
                prof = cProfile.Profile()
                scheduler_result = prof.runcall(self.call_scheduler, scheduler_input)
                prof.dump_stats('call_scheduler.pstat')
            else:
                scheduler_result = self.call_scheduler(scheduler_input, deadline)
            resources_to_clear = self.network_model.keys()
            for resource in dont_cancel_resources:
                    resources_to_clear.remove(resource)
            try:
                before_apply = datetime.utcnow()
                self.normal_scheduled_requests_by_ur = scheduler_result.get_scheduled_requests_by_tracking_num()
                n_submitted = self.apply_scheduler_result(scheduler_result,
                                            scheduler_input,
                                            resources_to_clear, deadline)
                after_apply = datetime.utcnow()
                if(n_submitted > 0):
                    self.avg_save_time_per_reservation_timedelta = timedelta(seconds=(after_apply - before_apply).total_seconds() / n_submitted)
                    self.log.info("Avg save time per reservation was %.2f seconds" % self.avg_save_time_per_reservation_timedelta.total_seconds())
                normal_scheduling_end = datetime.utcnow()
                normal_scheduling_timedelta = normal_scheduling_end - normal_scheduling_start
                self.estimated_normal_run_timedelta = estimate_runtime(self.estimated_normal_run_timedelta, normal_scheduling_timedelta)
                self.log.info("Normal scheduling took %.2f seconds" % normal_scheduling_timedelta.total_seconds())
                self._write_scheduling_summary_log("Normal Scheduling Summary")
            except EstimateExceededException as eee:
                self.log.warn("Not enough time left to apply schedule before estimated scheduler end.  Schedule will not be saved.")
                normal_scheduling_timedelta = eee.new_estimate - normal_scheduling_start
                self.estimated_normal_run_timedelta = estimate_runtime(self.estimated_normal_run_timedelta, normal_scheduling_timedelta)
                raise eee
            finally:
                self.log.info("New runtime estimate is %.2f seconds" % self.estimated_normal_run_timedelta.total_seconds())
                self.log.info("End Normal Scheduling")
        else:
            self.log.warn("Received no Normal User Requests! Skipping Normal scheduling cycle")

        return scheduler_result

    @timeit
    def create_new_schedule(self, network_state_timestamp):
        too_scheduler_result = self.scheduling_cycle(TOO_SCHEDULE_TYPE, network_state_timestamp)
        set_schedule_type(None)
        # Find resource scheduled by ToO run and don't cancel their schedules
        # during normal scheduling run
        too_resources = []
        if too_scheduler_result:
            for too_resource, reservation_list in too_scheduler_result.schedule.iteritems():
                if reservation_list:
                    too_resources.append(too_resource)
        self.scheduling_cycle(NORMAL_SCHEDULE_TYPE, network_state_timestamp, too_resources)
        set_schedule_type(None)

    @metric_timer('scheduling_cycle', num_reservations=lambda x: x.count_reservations(), rate=lambda x: x.count_reservations())
    def scheduling_cycle(self, schedule_type, network_state_timestamp, too_resources=None):
        set_schedule_type(schedule_type)
        result = None
        if schedule_type == NORMAL_SCHEDULE_TYPE:
            scheduler_input = self.input_factory.create_normal_scheduling_input(self.estimated_normal_run_timedelta.total_seconds(),
                                                                                scheduled_requests_by_ur=self.normal_scheduled_requests_by_ur,
                                                                     network_state_timestamp=network_state_timestamp)
            result = self.create_normal_schedule(scheduler_input, too_resources)
        elif schedule_type == TOO_SCHEDULE_TYPE:
            scheduler_input = self.input_factory.create_too_scheduling_input(self.estimated_too_run_timedelta.total_seconds(),
                                                                             scheduled_requests_by_ur=self.too_scheduled_requests_by_ur,
                                                                   network_state_timestamp=network_state_timestamp)
            result = self.create_too_schedule(scheduler_input)
        return result


class EstimateExceededException(Exception):

    def __init__(self, msg, new_estimate):
        Exception.__init__(self, msg)
        self.new_estimate = new_estimate
