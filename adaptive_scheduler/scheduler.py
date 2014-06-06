from __future__ import division

import time
import sys
import logging
import itertools

from collections import defaultdict
from datetime import datetime, timedelta

from reqdb.requests                      import Request
from schedutils.semester_service         import get_semester_block
from adaptive_scheduler.feedback         import TimingLogger
from schedutils.semester_service         import get_semester_code
from adaptive_scheduler.interfaces       import ScheduleException
from adaptive_scheduler.model2           import ModelBuilder
from adaptive_scheduler.event_utils      import report_scheduling_outcome
from adaptive_scheduler.kernel.intervals import Intervals
from adaptive_scheduler.utils            import timeit, iso_string_to_datetime
from adaptive_scheduler.printing         import pluralise as pl
from adaptive_scheduler.printing import (print_schedule, print_compound_reservations,
                                         summarise_urs, log_full_ur, log_windows)
from adaptive_scheduler.model2   import (filter_out_compounds,
                                         differentiate_by_type, n_requests,
                                         RequestError)
from adaptive_scheduler.kernel_mappings import (construct_visibilities,
                                                 construct_resource_windows,
                                                 make_compound_reservations,
                                                 make_many_type_compound_reservations,
                                                 filter_for_kernel,
                                                 construct_global_availability)
from adaptive_scheduler.request_filters import (filter_urs,
                                                drop_empty_requests,
                                                find_unschedulable_ur_numbers,
                                                set_now)


class SchedulerParameters(object):
    
    def __init__(self, dry_run=False, run_once=False, telescopes_file='telescopes.dat', cameras_file='camera_mappings', no_weather=False, no_singles=False, no_compounds=False, no_too=False, timelimit_seconds=None, slicesize_seconds=300, horizon_days=7.0, sleep_seconds=60, simulate_now=None, kernel='gurobi'):
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
        
        
    def get_model_builder(self):
        mb = ModelBuilder(self.telescopes_file, self.cameras_file)
        
        return mb


class Scheduler(object):
    
    def __init__(self, kernel_class, sched_params, event_bus):
        self.kernel_class = kernel_class
        self.visibility_cache = {}
        self.sched_params = sched_params
        self.event_bus = event_bus
        self.log = logging.getLogger(__name__)
        self.estimated_scheduler_end = datetime.utcnow()
    
    
    def find_resources_to_preempt(self, preemtion_urs, all_urgent_urs, resources, resource_usage_snapshot):
        ''' Preempt running requests, if needed, to run urgent user requests'''
    
        #make copy of resource list since it could be modified
        copy_of_resources = list(resources)
    
        # Don't preemt another urgent request
        # Remove any resource with running urgent requests from resource list
        all_urgent_tracking_numbers = [ur.tracking_number for ur in all_urgent_urs]           
        for resource in resources:
            for running_ur in resource_usage_snapshot.user_requests_for_resource(resource):
                if running_ur.tracking_number in all_urgent_tracking_numbers:
                    copy_of_resources.remove(resource)
    
        value_function_dict = self.construct_value_function_dict(preemtion_urs, copy_of_resources, resource_usage_snapshot)
    
        preemtion_tracking_numbers = [ur.tracking_number for ur in preemtion_urs]
        optimal_combination = self.compute_optimal_combination(value_function_dict, preemtion_tracking_numbers, copy_of_resources)
    
        # get resources where the cost of canceling is lowest
        resources_to_cancel = [ combination[0] for combination in optimal_combination ]
    
        return resources_to_cancel
    
    
    
    #TODO - Move to a utils library
    def combine_excluded_intervals(self, excluded_intervals_1, excluded_intervals_2):
        ''' Combine two dictionaries where Intervals are the values '''
        for key in excluded_intervals_2:
            timepoints = excluded_intervals_2[key].timepoints
            excluded_intervals_1.setdefault(key, Intervals([])).add(timepoints)
    
        return excluded_intervals_1
    
    def construct_value_function_dict(self, urgent_urs, resources, resource_usage_snapshot):
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
                normal_ur_prioirty = 0
                normal_ur_prioirty += resource_usage_snapshot.get_priority(running_tracking_number)
#                 else:
#                     # The running request wasn't included in the list of schedulable urs so we don't know it's priority
#                     # This could happen if the running request has been canceled.  In this case treat it like nothing is running
#                     # Not sure if there are other ways this can happen.  Beware....
#                     # TODO: add function unit test for this case
#                     normal_ur_prioirty = 1
                # Take the greater of the running priorities.  Should it be the sum? Something else?
                if normal_ur_prioirty > running_request_priority:
                    running_request_priority = normal_ur_prioirty
    
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
    
        for x in itertools.permutations(large_list, len(small_list)):
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
        ONE_MONTH = timedelta(weeks=4)
        ONE_WEEK  = timedelta(weeks=1)
        
        return estimated_scheduler_end + ONE_WEEK
            
            
    def apply_unschedulable_filters(self, user_reqs, resource_usage_snapshot, estimated_scheduler_end):
        ''' Returns tuple of (schedulable, unschedulable) user requests where UR's
        in the unschedulable list will never be possible
        '''
        return user_reqs, []
    
    
    def apply_window_filters(self, user_reqs, estimated_scheduler_end):
        ''' Returns the set of URs with windows adjusted to include only URs with windows
        suitable for scheduling
        '''
        return user_reqs
    
    
    def prepare_for_kernel(self, window_adjusted_urs, estimated_scheduler_end):
        ''' Convert UR model to formalization expected by the scheduling kernel
        '''
        return []
    
    
    def prepare_available_windows_for_kernel(self, available_resources, resource_usage_snapshot, estimated_scheduler_end):
        ''' Construct the set of resource windows available for use in scheduling
        '''
        return []
    
    
    # TODO: replace with event bus event
    def on_run_scheduler(self, user_requests):
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
    
    
    def unscheduleable_ur_numbers(self, unschedulable_urs):
        return find_unschedulable_ur_numbers(unschedulable_urs)
    
    
    def filter_unscheduleable_child_requests(self, user_requests):
        '''Remove child request from the parent user request when the
        request has no windows remaining and return a list of dropped
        request numbers 
        '''
        return drop_empty_requests(user_requests)
    
    
    # TODO: refactor into smaller chunks
    @timeit
    def run_scheduler(self, user_reqs, resource_usage_snapshot, available_resources, estimated_scheduler_end, preemption_enabled=False):
    
        start_event = TimingLogger.create_start_event(datetime.utcnow())
        self.event_bus.fire_event(start_event)
        self.estimated_scheduler_end = estimated_scheduler_end
        self.on_run_scheduler(user_reqs)
        
        if self.sched_params.no_singles:
            user_reqs = self.remove_singles(user_reqs)
    
        if self.sched_params.no_compounds:
            user_reqs = self.remove_compounds(user_reqs)
        
        schedulable_urs, unschedulable_urs = self.apply_unschedulable_filters(user_reqs, resource_usage_snapshot, estimated_scheduler_end)
        unschedulable_ur_numbers = self.unscheduleable_ur_numbers(unschedulable_urs)
        unschedulable_r_numbers  = self.filter_unscheduleable_child_requests(schedulable_urs)
        self.after_unschedulable_filters(schedulable_urs)
        
        window_adjusted_urs = self.apply_window_filters(schedulable_urs, estimated_scheduler_end)
        self.after_window_filters(window_adjusted_urs)
        
        # By default, cancel on all resources
        resources_to_schedule = list(available_resources)
        resource_schedules_to_cancel = []
        
        # Pre-empt running blocks
        if preemption_enabled:
            resource_schedules_to_cancel = self.find_resources_to_preempt(window_adjusted_urs, user_reqs, resources_to_schedule, resource_usage_snapshot) 
            # Need a copy because the original is modified inside the loop
            copy_of_resources_to_schedule = list(resources_to_schedule)
            for resource in copy_of_resources_to_schedule:
                if not resource in resource_schedules_to_cancel:
                    self.log.info("Removing %s from schedulable resources.  Not needed for ToO." % resource)
                    resources_to_schedule.remove(resource)
        else:
            resource_schedules_to_cancel = available_resources
            
        compound_reservations = self.prepare_for_kernel(window_adjusted_urs, estimated_scheduler_end)        
        available_windows = self.prepare_available_windows_for_kernel(resources_to_schedule, resource_usage_snapshot, estimated_scheduler_end)
    
        print_compound_reservations(compound_reservations)
    
        # Prepare scheduler result
        scheduler_result = SchedulerResult()
        scheduler_result.schedule = None
        scheduler_result.resource_schedules_to_cancel = resource_schedules_to_cancel
        scheduler_result.unschedulable_user_request_numbers = unschedulable_ur_numbers
        scheduler_result.unschedulable_request_numbers = unschedulable_r_numbers
        
        if compound_reservations:
            # Instantiate and run the scheduler
            contractual_obligations = []
        
            self.log.info("Starting scheduling kernel")
            kernel   = self.kernel_class(compound_reservations, available_windows, contractual_obligations, resources_to_schedule, self.sched_params.slicesize_seconds)
            scheduler_result.schedule = kernel.schedule_all(timelimit=self.sched_params.timelimit_seconds)
            self.log.info("Completed scheduling kernel")
            
            # Do post scheduling stuff
            self.on_new_schedule(scheduler_result.schedule, compound_reservations, estimated_scheduler_end)
        else:
            self.log.info("Nothing to schedule! Skipping kernel call...")
            scheduler_result.resource_schedules_to_cancel = {}
       
       
        return scheduler_result


class LCOGTNetworkScheduler(Scheduler):
    
    def __init__(self, kernel_class, sched_params, event_bus, network_model):
        Scheduler.__init__(self, kernel_class, sched_params, event_bus)
        
        self.visibility_cache = {}
        self.date_fmt      = '%Y-%m-%d'
        self.date_time_fmt = '%Y-%m-%d %H:%M:%S'
        self.network_model = network_model
    
    
    @timeit
    def blacklist_running_user_requests(self, ur_list, resource_usage_snapshot):
        self.log.info("Before applying running blacklist, %d schedulable %s", *pl(len(ur_list), 'UR'))
        all_tns = [ur.tracking_number for ur in ur_list]
        unblocked_running_ur_tracking_numbers = self._find_unblocked_running_urs_blocked_by_outage_events(resource_usage_snapshot)
        schedulable_tns = set(all_tns) - set(unblocked_running_ur_tracking_numbers)
        schedulable_urs = [ur for ur in ur_list if ur.tracking_number in schedulable_tns]
        self.log.info("After running blacklist, %d schedulable %s", *pl(len(schedulable_urs), 'UR'))
    
        return schedulable_urs
    
    
    def _find_unblocked_running_urs_blocked_by_outage_events(self, resource_usage_snapshot):
        unblocked_ur_tracking_numbers = []
        for running_ur in resource_usage_snapshot.running_user_requests():
            if not self._is_running_ur_blocked_by_network_outage(running_ur):
                unblocked_ur_tracking_numbers.append(running_ur.tracking_number)
                
        return unblocked_ur_tracking_numbers
        
    
    def _is_running_ur_blocked_by_network_outage(self, running_ur):
        telescopes_with_outage_events = [tel_name for tel_name, tel_model in self.network_model.items() if len(tel_model.events) > 0]
        unblocked_running_requests = 0
        for running_r in running_ur.running_requests:
            if not running_r.resource in telescopes_with_outage_events:
                unblocked_running_requests += 1
        
        return unblocked_running_requests < 1
    
    
    def _log_scheduler_start_details(self, estimated_scheduler_end):
        semester_start, semester_end = get_semester_block(dt=estimated_scheduler_end)
        self.log.info("Scheduling for semester %s (%s to %s)", get_semester_code(),
                                                         semester_start.strftime(self.date_fmt),
                                                         semester_end.strftime(self.date_fmt))
        strfmt_horizon = self.scheduling_horizon(estimated_scheduler_end).strftime(self.date_time_fmt)
        self.log.info("Scheduling horizon is %s", strfmt_horizon)
    
    
    def _log_ur_input_details(self, user_reqs, estimated_scheduler_end):
        # Summarise the User Requests we've received
        self.log.info("Received %s from Request DB", pl(len(user_reqs), 'User Request'))
        n_urs, n_rs = n_requests(user_reqs)
    
        self.log.info("Deserialised %s (%s) from Request DB", pl(n_urs, 'User Request'),
                                                         pl(n_rs, 'Request'))
    
        summarise_urs(user_reqs, log_msg="Received from Request DB")
        for ur in user_reqs:
            log_full_ur(ur, estimated_scheduler_end)
            log_windows(ur, log_msg="Initial windows:")
            
            
    def apply_unschedulable_filters(self, user_reqs, resource_usage_snapshot, estimated_scheduler_end):
        ''' Returns tuple of (schedulable, unschedulable) user requests where UR's
        in the unschedulable list will never be possible
        '''
        self.log.info("Starting unschedulable filters")
        running_ur_tracking_numbers = resource_usage_snapshot.running_tracking_numbers()
        tag = 'RunningBlock'
        for ur in user_reqs:
            if ur.tracking_number in running_ur_tracking_numbers:
                msg = 'User Request %s is running' % resource_usage_snapshot.running_user_request(ur.tracking_number)
                ur.emit_user_feedback(msg, tag)
                break
    
        # Remove running user requests from consideration, and get the availability edge
        user_reqs = self.blacklist_running_user_requests(user_reqs, resource_usage_snapshot)
    
        # Filter by window, and set UNSCHEDULABLE on the Request DB as necessary
        self.log.info("Filtering for unschedulability")
        
        set_now(estimated_scheduler_end)
        schedulable_urs, unschedulable_urs = filter_urs(user_reqs)
        self.log.info("Found %d unschedulable %s after filtering", *pl(len(unschedulable_urs), 'UR'))
        self.log.info("Completed unschedulable filters")
        
        return schedulable_urs, unschedulable_urs
    
    
    def apply_window_filters(self, user_reqs, estimated_scheduler_end):
        ''' Returns the set of URs with windows adjusted to include only URs with windows
        suitable for scheduling
        '''
        self.log.info("Starting window filters")
        self.log.info("Filtering on dark/rise_set")
    
        semester_start, semester_end = get_semester_block(estimated_scheduler_end)
        filtered_window_user_reqs = filter_for_kernel(user_reqs, self.visibility_cache,
                                        estimated_scheduler_end, semester_end, self.scheduling_horizon(estimated_scheduler_end))
        self.log.info("Completed window filters")
        
        return filtered_window_user_reqs
    
    
    def prepare_for_kernel(self, window_adjusted_urs, estimated_scheduler_end):
        ''' Convert UR model to formalization expected by the scheduling kernel
        '''
        # Convert CompoundRequests -> CompoundReservations
        semester_start, semester_end = get_semester_block(estimated_scheduler_end)
        many_urs, other_urs = differentiate_by_type('many', window_adjusted_urs)
        many_compound_reservations = make_many_type_compound_reservations(many_urs, self.visibility_cache,
                                                                semester_start)
        other_compound_reservations = make_compound_reservations(other_urs, self.visibility_cache,
                                                       semester_start)
        all_compound_reservations = many_compound_reservations + other_compound_reservations
        
        return all_compound_reservations
    
    
    def prepare_available_windows_for_kernel(self, available_resources, resource_usage_snapshot, estimated_scheduler_end):
        ''' Construct the set of resource windows available for use in scheduling
        '''
        semester_start, semester_end = get_semester_block(estimated_scheduler_end)
        # Translate when telescopes are available into kernel speak
        resource_windows = construct_resource_windows(self.visibility_cache, semester_start)
    
        # Intersect and mask out time where Blocks are currently running
        global_windows = construct_global_availability(available_resources, semester_start,
                                                       resource_usage_snapshot, resource_windows)
        
        return global_windows
    
    
    def on_run_scheduler(self, user_requests):
        self._log_scheduler_start_details(self.estimated_scheduler_end)
        self._log_ur_input_details(user_requests, self.estimated_scheduler_end)
        
        semester_start, semester_end = get_semester_block(dt=self.estimated_scheduler_end)
        
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
    
        # Summarise the schedule in normalised epoch (kernel) units of time
        semester_start, semester_end = get_semester_block(estimated_scheduler_end)
        print_schedule(new_schedule, semester_start, semester_end)


class SchedulerResult(object):
    
    def __init__(self):
        self.schedule = None
        self.resource_schedules_to_cancel = None
        self.unschedulable_user_request_numbers = None
        self.unschedulable_request_numbers = None
        
        
class SchedulerRunner(object):
    
    def __init__(self, sched_params, scheduler, network_interface, network_model, input_factory):
        self.run_flag = True
        self.sched_params = sched_params
        self.scheduler = scheduler
        self.network_interface = network_interface
        self.network_model = network_model
        self.input_factory = input_factory
        self.log = logging.getLogger(__name__)
        
    
    
    def scheduler_rerun_required(self):
        ''' Return True if scheduler should be run now
        '''
        network_has_changed = False
        
        if self.network_interface.current_events_has_changed():
            self.log.info("Telescope network events were found.")
            network_has_changed = True
            
        return network_has_changed or self.network_interface.schedulable_request_set_has_changed()
    
    
    def update_network_model(self):
        current_events = self.network_interface.get_current_events()
        for telescope_name, telescope in self.network_model.iteritems():
            if telescope_name in current_events:
                telescope.events.extend(current_events[telescope_name])
                msg = "Found network event for '%s' - removing from consideration (%s)" % (
                                                                    telescope_name,
                                                                    current_events[telescope_name])
                self.log.info(msg)
    
        return
    
    
    def run(self):
        first_run = True
        while self.run_flag:
            if self.sched_params.no_weather:
                self.log.info("Weather monitoring disabled on the command line")
            else:
                self.update_network_model()
            
            # Always run the scheduler on the first run    
            if first_run or self.scheduler_rerun_required():
                self.create_new_schedule()
                
            if self.sched_params.run_once:
                self.run_flag = False
            else:
                self.log.info("Sleeping for %d seconds", self.sched_params.sleep_seconds)
                time.sleep(self.sched_params.sleep_seconds)
            first_run = False
            
            
    def _write_scheduler_input_files(self, json_user_request_list, resource_usage_snapshot):
        import pickle
        output = {
                  'json_user_request_list' : json_user_request_list,
                  'resource_usage_snapshot' : resource_usage_snapshot
                  }
        outfile = open('/tmp/scheduler_input.pickle', 'w')
        pickle.dump(output, outfile)
        outfile.close()
    
    
    def call_scheduler(self, scheduler_input, preemption_enabled):
        self.log.info("Using a 'now' of %s", scheduler_input.scheduler_now)
        n_urs, n_rs = n_requests(scheduler_input.user_requests)
        semester_start, semester_end = get_semester_block(dt=scheduler_input.estimated_scheduler_end)
        try:
            scheduler_result = self.scheduler.run_scheduler(scheduler_input.user_requests, scheduler_input.resource_usage_snapshot, scheduler_input.available_resources, scheduler_input.estimated_scheduler_end, preemption_enabled=preemption_enabled)
            
            if not self.sched_params.dry_run:
                # Set the states of the Requests and User Requests
                self.network_interface.set_requests_to_unschedulable(scheduler_result.unschedulable_request_numbers)
                self.network_interface.set_user_requests_to_unschedulable(scheduler_result.unschedulable_user_request_numbers)
                
                # Delete old schedule
                # TODO: make sure this cancels anything currently running
                n_deleted = self.network_interface.cancel(scheduler_input.estimated_scheduler_end, semester_end, self.sched_params.dry_run, scheduler_result.telescope_schedules_to_cancel)
                
                # Write new schedule
                n_submitted = self.network_interface.save(scheduler_result.new_schedule, semester_start, self.sched_params.cameras_file, self.sched_params.dry_run)
                #TODO: Lost this logging function during refactor somewhere
#                 self.write_scheduling_log(n_urs, n_rs, n_deleted, n_submitted, self.sched_params.dry_run)
        except ScheduleException, se:
            self.log.error(se, "aborting run")
        
            
    def create_new_schedule(self):
#         self.log.info("Received %d ToO User Requests" % len(too_user_requests))
#         self.log.info("Received %d Normal User Requests" % len(normal_user_requests))
                
#         self._write_scheduler_input_files(json_user_request_list, resource_usage_snapshot)
        scheduler_input = self.input_factory.create_too_scheduling_input()
        if scheduler_input.user_requests:
            self.log.info("Start ToO Scheduling")
            self.call_scheduler(scheduler_input, preemption_enabled=True)
            self.log.info("End ToO Scheduling")
        else:
            self.log.warn("Received no ToO User Requests! Skipping ToO scheduling cycle")
    
        # Run the scheduling loop, if there are any User Requests
        scheduler_input = self.input_factory.create_normal_scheduling_input()
        if scheduler_input.user_requests:
            self.log.info("Start Normal Scheduling")
            self.call_scheduler(scheduler_input, preemption_enabled=False)
            self.log.info("End Normal Scheduling")
        else:
            self.log.warn("Received no Normal User Requests! Skipping Normal scheduling cycle")
        
        # Only clear the change state if scheduling is succesful and not a dry run
        if not self.sched_params.dry_run:
            self.network_interface.clear_schedulable_request_set_changed_state()
        sys.stdout.flush()


class SchedulingInputFactory():
    
    def __init__(self, input_provider):
        self.input_provider = input_provider
        
    
    def _create_scheduling_input(self, input_provider, is_too_input):
        scheduler_input = SchedulingInput(input_provider.sched_params,
                        input_provider.scheduler_now,
                        input_provider.estimated_scheduling_end,
                        input_provider.json_user_request_list,
                        input_provider.resource_usage_snapshot,
                        input_provider.available_resources)
        file_timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        filename = '/tmp/too_scheduling_input_%s.pickle'
        if is_too_input:
            filename = '/tmp/normal_scheduling_input_%s.pickle'
        filename = filename % file_timestamp
        scheduler_input.write_input_to_file(filename)
        
        return scheduler_input
    
    
    def create_too_scheduling_input(self):
        self.input_provider.set_too_mode()
        return self._create_scheduling_input(self.input_provider, True)
    
    
    def create_normal_scheduling_input(self):
        self.input_provider.clear_too_mode()
        return self._create_scheduling_input(self.input_provider, False)


class SchedulingInputUtils(object):
    
    def __init__(self, model_builder):
        self.model_builder = model_builder

    def json_urs_to_scheduler_model_urs(self, json_user_request_list):
        scheduler_model_urs = []
        for json_ur in json_user_request_list:
            try:
                scheduler_model_ur = self.model_builder.build_user_request(json_ur)
                scheduler_model_urs.append(scheduler_model_ur)
            except RequestError as e:
                self.log.warn(e)
        
        return scheduler_model_urs
    
    
    def sort_scheduler_models_urs_by_type(self, schedule_model_user_requests):
        scheduler_models_urs_by_type = {
                                        'too' : [],
                                        'normal' : []
                                        }
        for scheduler_model_ur in schedule_model_user_requests:
            if scheduler_model_ur.has_target_of_opportunity():
                scheduler_models_urs_by_type['too'].append(scheduler_model_ur)
            else:
                scheduler_models_urs_by_type['normal'].append(scheduler_model_ur)
                
        return scheduler_models_urs_by_type
    
    
    def user_request_priorities(self, json_user_request_list):
        scheduler_model_urs = self.json_urs_to_scheduler_model_urs(json_user_request_list)
        priorities_map = {ur.tracking_number : ur.get_priority() for ur in scheduler_model_urs}
         
        return priorities_map
    
    
    def too_tracking_numbers(self, json_user_request_list):
        scheduler_model_urs = self.json_urs_to_scheduler_model_urs(json_user_request_list)
        scheduler_models_urs_by_type = self.sort_scheduler_models_urs_by_type(scheduler_model_urs)
        too_tracking_numbers = [ur.tracking_number for ur in scheduler_models_urs_by_type['too']]
         
        return too_tracking_numbers

import pickle

class SchedulingInput(object):
    
    def __init__(self, sched_params, scheduler_now, estimated_scheduler_end, json_user_request_list, resource_usage_snapshot, available_resources, is_too_input=False):
        self.sched_params = sched_params
        self.scheduler_now = scheduler_now
        self.estimated_scheduler_end = estimated_scheduler_end
        self.json_user_request_list = json_user_request_list
        self.resource_usage_snapshot = resource_usage_snapshot
        self.available_resources = available_resources
        self.is_too_input = is_too_input
        self.utils = SchedulingInputUtils(sched_params.get_model_builder())
    
        
    @property
    def user_requests(self):
        scheduler_model_urs = self.utils.json_urs_to_scheduler_model_urs(self.json_user_request_list)
        scheduler_models_urs_by_type = self.utils.sort_scheduler_models_urs_by_type(scheduler_model_urs)
        
        if self.is_too_input:
            return scheduler_models_urs_by_type['too']
        else:
            return scheduler_models_urs_by_type['normal']
    
    
    @property
    def user_request_priorities(self):
        return self.utils.user_request_priorities(self.json_user_request_list)
     
    
    @property 
    def too_tracking_numbers(self):
        return self.utils.too_tracking_numbers(self.json_user_request_list)
    
    
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
        pickle.dump(output, outfile)
        outfile.close()
        
    
    @staticmethod
    def read_from_file(self, filename):
        infile = open(filename, 'r')
        input_from_file = pickle.load(infile)
        
        return SchedulingInput(**input_from_file)


class SchedulingInputException(Exception):
    pass


class SchedulingInputProvider(object):
    
    def __init__(self, sched_params, network_interface, network_model, is_too_input=False):
        self.sched_params = sched_params
        self.network_interface = network_interface
        self.network_model = network_model
        self.is_too_input = is_too_input
        self.utils = SchedulingInputUtils(sched_params.get_model_builder())
    
        # TODO: Hide these behind read only properties
        self.scheduler_now = None
        self.estimated_scheduling_end = None
        self.json_user_request_list = None
        self.available_resources = None
        self.resource_usage_snapshot = None
        
        self.refresh()
    
    
    def refresh(self):
        # The order of these is important
        self.scheduler_now = self._get_scheduler_now()
        self.estimated_scheduling_end = self._get_estimated_scheduling_end()
        self.json_user_request_list = self._get_json_user_request_list()
        self.available_resources = self._get_available_resources()
        self.resource_usage_snapshot = self._get_resource_usage_snapshot()
        
    
    def set_too_mode(self):
        self.is_too_input = True
        self.refresh()
    
    
    def clear_too_mode(self):
        self.is_too_input = False
        self.resource_usage_snapshot = self._get_resource_usage_snapshot()
    
    
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
    
    
    def _get_estimated_scheduling_end(self):
        if self.is_too_input:
            return self.scheduler_now + timedelta(minutes=2)
        else:
            return self.scheduler_now + timedelta(minutes=6)
    
    
    def _get_json_user_request_list(self):
        semester_start, semester_end = get_semester_block(dt=self.estimated_scheduling_end)
        ur_list =  self.network_interface.get_all_user_requests(semester_start, semester_end)
        
        return ur_list
    
    
    def _get_available_resources(self):
        resources = []
        for resource_name, resource in self.network_model.iteritems():
            if not resource.events:
                resources.append(resource_name)
                
        return resources
    
    
    def _get_resource_usage_snapshot(self):
        user_request_priorities = self.utils.user_request_priorities(self.json_user_request_list)
        too_tracking_numbers = self.utils.too_tracking_numbers(self.json_user_request_list)
        snapshot = self.network_interface.resource_usage_snapshot(self.available_resources, self.scheduler_now, self.estimated_scheduling_end, user_request_priorities, too_tracking_numbers)
        
        return snapshot
        
    
