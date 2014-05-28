import time
import sys
import logging

from datetime import datetime, timedelta
from reqdb.requests import Request
from schedutils.semester_service         import get_semester_block
from adaptive_scheduler.feedback        import TimingLogger
from schedutils.semester_service import get_semester_code
from adaptive_scheduler.printing import (print_schedule, print_compound_reservations,
                                          summarise_urs, log_full_ur, log_windows)
from adaptive_scheduler.model2          import (filter_out_compounds,
                                                 differentiate_by_type, n_requests,
                                                  )
from adaptive_scheduler.kernel_mappings import (construct_visibilities,
                                                 construct_resource_windows,
                                                 make_compound_reservations,
                                                 make_many_type_compound_reservations,
                                                 filter_for_kernel,
                                                 construct_global_availability)
from adaptive_scheduler.request_filters import filter_urs, drop_empty_requests, find_unschedulable_ur_numbers, set_now
from adaptive_scheduler.event_utils import report_scheduling_outcome
from adaptive_scheduler.kernel.intervals import Intervals
from adaptive_scheduler.utils import timeit, iso_string_to_datetime
from adaptive_scheduler.printing import pluralise as pl
from adaptive_scheduler.interfaces import ScheduleException
from timeit import itertools
from collections import defaultdict
from adaptive_scheduler.model2           import ModelBuilder



class SchedulerParameters(object):
    
    def __init__(self, dry_run=False, run_once=False, telescopes_file='telescopes.dat', cameras_file='camera_mappings', no_weather=False, no_singles=False, no_compounds=False, no_too=False, timelimit_seconds=None, slicesize_seconds=300, horizon_days=7.0, sleep_seconds=60, simulate_now=None):
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
        
        self.date_fmt      = '%Y-%m-%d'
        self.date_time_fmt = '%Y-%m-%d %H:%M:%S'
    
    
    @timeit
    def blacklist_running_user_requests(self, ur_list, running_ur_tracking_numbers):
        self.log.info("Before applying running blacklist, %d schedulable %s", *pl(len(ur_list), 'UR'))
        all_tns = [ur.tracking_number for ur in ur_list]
        schedulable_tns = set(all_tns) - set(running_ur_tracking_numbers)
        schedulable_urs = [ur for ur in ur_list if ur.tracking_number in schedulable_tns]
        self.log.info("After running blacklist, %d schedulable %s", *pl(len(schedulable_urs), 'UR'))
    
        return schedulable_urs
    
    
    def find_tels_to_preempt(self, visible_too_urs, all_too_urs, normal_urs, tels, network_snapshot):
        ''' Preempt running blocks, if needed, to run Target of Opportunity user requests'''
    
        #make copy of tels since it could be modified
        tels_copy = dict(tels)
    
        # Don't preemt another ToO
        # Remove tels with running too from tels
        all_too_tracking_numbers = [ur.tracking_number for ur in all_too_urs]           
        for tel in tels.keys():
            if network_snapshot.user_request_for_telescope(tel) in all_too_tracking_numbers:
                del tels_copy[tel]
    
        value_function_dict = self.construct_value_function_dict(visible_too_urs, normal_urs, tels, network_snapshot)
    
        visible_too_tracking_numbers = [ur.tracking_number for ur in visible_too_urs]
        optimal_combination = self.compute_optimal_combination(value_function_dict, visible_too_tracking_numbers, tels)
    
        # get telescopes where the cost of canceling is lowest and there is a running block
        tels_to_cancel = [ combination[0] for combination in optimal_combination if network_snapshot.user_request_for_telescope(combination[0])]
    
        return tels_to_cancel
    
    
    
    #TODO - Move to a utils library
    def combine_excluded_intervals(self, excluded_intervals_1, excluded_intervals_2):
        ''' Combine two dictionaries where Intervals are the values '''
        for key in excluded_intervals_2:
            timepoints = excluded_intervals_2[key].timepoints
            excluded_intervals_1.setdefault(key, Intervals([])).add(timepoints)
    
        return excluded_intervals_1
    
    
    def construct_value_function_dict(self, too_urs, normal_urs, tels, network_snapshot):
        ''' Constructs a value dictionary of tuple (telescope, tracking_number) to value
    
            where value = too priority / running block priority or if no block is running at
            that telescope, value = too priority
    
            NOTE: Assumes running block priority is above 1
        '''
    
        normal_tracking_numbers_dict = {ur.tracking_number : ur for ur in normal_urs}
    
        tracking_number_to_telescopes = defaultdict(set)
        for ur in too_urs: 
            tracking_number = ur.tracking_number
    
            if ur.n_requests > 1:
                msg = "TOO UR %s has more than one child R, which is not supported." % tracking_number
                msg += " Submit as separate requests."
                self.log.info(msg)
                continue
    
            for request in ur.requests:
                for window in request.windows:
                    tracking_number_to_telescopes[tracking_number].add(window.resource)
    
        value_function_dict = {};
        for tel in tels:
            running_at_tel = network_snapshot.user_request_for_telescope(tel) 
            # Compute the priority of the the telescopes without ToOs
            if running_at_tel:
                running_request_priority = 0;
                running_tracking_number = running_at_tel
                running_request_priority += normal_tracking_numbers_dict[running_tracking_number].get_priority()
            else:
                # use a priority of 1 for telescopes without a running block
                running_request_priority = 1
    
            for ur in too_urs:
                tracking_number = ur.tracking_number
                if tel in tracking_number_to_telescopes[tracking_number]:
                    too_priority = ur.get_priority()
                    value_function_dict[(tel, tracking_number)] = too_priority / running_request_priority
    
        return value_function_dict
    
    
    def compute_optimal_combination(self, value_dict, tracking_numbers, telescopes):
        '''
        Compute combination of telescope to tracking number that has the highest value
    
        NOTE: This schedule assumes that each there will a tracking number only needs one
              telescope to run (no compound requests).
        '''
        if len(tracking_numbers) < len(telescopes):
            small_list = tracking_numbers
            large_list = telescopes
            zip_combinations = lambda x : zip(x, small_list)
        else:
            large_list = tracking_numbers
            small_list = telescopes
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
            
            
    def apply_unschedulable_filters(self, user_reqs, network_snapshot, estimated_scheduler_end):
        ''' Returns tuple of (schedulable, unschedulable) user requests where UR's
        in the unschedulable list will never be possible
        '''
        running_ur_tracking_numbers = network_snapshot.running_tracking_numbers()
        tag = 'RunningBlock'
        for ur in user_reqs:
            if ur.tracking_number in running_ur_tracking_numbers:
                msg = 'User Request is running' % network_snapshot.running_user_request(ur.tracking_number)
                ur.emit_user_feedback(msg, tag)
                break
    
        # Remove running user requests from consideration, and get the availability edge
        user_reqs = self.blacklist_running_user_requests(user_reqs, running_ur_tracking_numbers)
    
        # Filter by window, and set UNSCHEDULABLE on the Request DB as necessary
        self.log.info("Filtering for unschedulability")
        
        set_now(estimated_scheduler_end)
        schedulable_urs, unschedulable_urs = filter_urs(user_reqs)
        
        return schedulable_urs, unschedulable_urs
    
    
    def apply_window_filters(self, user_reqs, network_model, estimated_scheduler_end):
        ''' Returns the set of URs with windows adjusted to include only URs with windows
        suitable for scheduling
        '''
        # Do another check on duration and operator soundness, after dark/rise checking
        self.log.info("Filtering on dark/rise_set")
    
        semester_start, semester_end = get_semester_block(estimated_scheduler_end)
        for tel_name, tel in network_model.iteritems():
            if tel.events:
                self.log.info("Bypassing visibility calcs for %s" % tel_name)
    
        filtered_window_user_reqs = filter_for_kernel(user_reqs, self.visibility_cache, network_model,
                                        estimated_scheduler_end, semester_end, self.scheduling_horizon(estimated_scheduler_end))
        
        return filtered_window_user_reqs
    
    
    def prepare_for_kernel(self, window_adjusted_urs, network_model, estimated_scheduler_end):
        ''' Convert UR model to formalization expected by the scheduling kernel
        '''
        # Convert CompoundRequests -> CompoundReservations
        semester_start, semester_end = get_semester_block(estimated_scheduler_end)
        many_urs, other_urs = differentiate_by_type('many', window_adjusted_urs)
        many_compound_reservations = make_many_type_compound_reservations(many_urs, network_model, self.visibility_cache,
                                                                semester_start)
        other_compound_reservations = make_compound_reservations(other_urs, network_model, self.visibility_cache,
                                                       semester_start)
        all_compound_reservations = many_compound_reservations + other_compound_reservations
        
        return all_compound_reservations
    
    
    def prepare_available_windows_for_kernel(self, network_model, network_snapshot, estimated_scheduler_end):
        ''' Construct the set of resource windows available for use in scheduling
        '''
        semester_start, semester_end = get_semester_block(estimated_scheduler_end)
        # Translate when telescopes are available into kernel speak
        resource_windows = construct_resource_windows(self.visibility_cache, semester_start)
    
        # Intersect and mask out time where Blocks are currently running
        global_windows = construct_global_availability(network_model, semester_start,
                                                       network_snapshot, resource_windows)
        
        return global_windows
    
    
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
        
    
    def unscheduleable_ur_numbers(self, unschedulable_urs):
        return find_unschedulable_ur_numbers(unschedulable_urs)
    
    def filter_unscheduleable_child_requests(self, schedulable_urs):
        return drop_empty_requests(schedulable_urs)
    
    
    # TODO: refactor into smaller chunks
    @timeit
    def run_scheduler(self, user_reqs_dict, network_snapshot, network_model, estimated_scheduler_end):
    
        start_event = TimingLogger.create_start_event(datetime.utcnow())
        self.event_bus.fire_event(start_event)
    
        run_type = user_reqs_dict['type']
        user_reqs = user_reqs_dict[run_type]
        normal_user_requests = user_reqs_dict[Request.NORMAL_OBSERVATION_TYPE]
        too_user_requests = user_reqs_dict[Request.TARGET_OF_OPPORTUNITY]
    
        self._log_scheduler_start_details(estimated_scheduler_end)
        self._log_ur_input_details(user_reqs, estimated_scheduler_end)
    
        if self.sched_params.no_singles:
            user_reqs = self.remove_singles(user_reqs)
    
        if self.sched_params.no_compounds:
            user_reqs = self.remove_compounds(user_reqs)
        
        semester_start, semester_end = get_semester_block(dt=estimated_scheduler_end)
        
        # Construct visibility objects for each telescope
        self.log.info("Constructing telescope visibilities")
        if not self.visibility_cache:
            self.visibility_cache = construct_visibilities(network_model, semester_start, semester_end)

        schedulable_urs, unschedulable_urs = self.apply_unschedulable_filters(user_reqs, network_snapshot, estimated_scheduler_end)


        self.log.info("Found %d unschedulable %s after filtering", *pl(len(unschedulable_urs), 'UR'))
        unschedulable_ur_numbers = self.unscheduleable_ur_numbers(unschedulable_urs)
        unschedulable_r_numbers  = self.filter_unscheduleable_child_requests(schedulable_urs)
        

        self.log.info("Completed unschedulable filters")
        summarise_urs(schedulable_urs, log_msg="Passed unschedulable filters:")
    
        for ur in schedulable_urs:
            log_windows(ur, log_msg="Remaining windows:")
        
        window_adjusted_urs = self.apply_window_filters(schedulable_urs, network_model, estimated_scheduler_end)
        
        self.log.info("Completed dark/rise_set filters")
        summarise_urs(window_adjusted_urs, log_msg="Passed dark/rise filters:")
        for ur in window_adjusted_urs:
            log_windows(ur, log_msg="Remaining windows:")
    
        self.log.info('Filtering complete. Ready to construct Reservations from %d URs.' % len(window_adjusted_urs))
    
        # By default, cancel on all telescopes
        tels_to_cancel = dict(network_model)
        
        # TODO: Change this to preemt or not preemt but not care about ToO
        # Pre-empt running blocks
        if run_type == Request.TARGET_OF_OPPORTUNITY:
            tels_to_cancel = self.find_tels_to_preempt(window_adjusted_urs, too_user_requests, normal_user_requests, network_model, network_snapshot);  
        
        # TODO: This logic is questionable.  exlculde_intervals in ToO case don't look correct
        # Get TOO requests scheduled in pond, combine with excluded_intervals
#         if run_type == Request.NORMAL_OBSERVATION_TYPE and too_user_requests:
#             excluded_intervals = self.combine_excluded_intervals(network_interface.current_user_request_intervals_by_telescope(),
#                                                                  network_interface.too_user_request_intervals_by_telescope())
#         else:
#             excluded_intervals = network_interface.current_user_request_intervals_by_telescope()
            
        
        compound_reservations = self.prepare_for_kernel(window_adjusted_urs, network_model, estimated_scheduler_end)        
        available_windows = self.prepare_available_windows_for_kernel(network_model, network_snapshot, estimated_scheduler_end)
    
        print_compound_reservations(compound_reservations)
    
        # Prepare scheduler result
        scheduler_result = SchedulerResult()
        scheduler_result.schedule = None
        scheduler_result.resource_schedules_to_cancel = tels_to_cancel
        scheduler_result.unschedulable_user_request_numbers = unschedulable_ur_numbers
        scheduler_result.unschedulable_request_numbers = unschedulable_r_numbers
        
        if compound_reservations:
            # Instantiate and run the scheduler
            time_slicing_dict = {}
            for t in network_model:
                time_slicing_dict[t] = [0, self.sched_params.slicesize_seconds]
        
            contractual_obligations = []
        
            self.log.info("Instantiating and running kernel")
            kernel   = self.kernel_class(compound_reservations, available_windows, contractual_obligations, time_slicing_dict)
            new_schedule = kernel.schedule_all(timelimit=self.sched_params.timelimit_seconds)
            
            # Put new schedule in result object
            scheduler_result.schedule = new_schedule
            
            # Do post scheduling stuff
            self.on_new_schedule(new_schedule, compound_reservations, estimated_scheduler_end)
        else:
            self.log.info("Nothing to schedule! Skipping kernel call...")
            scheduler_result.resource_schedules_to_cancel = {}
       
       
        return scheduler_result


class NetworkSanpshot(object):
    
    def __init__(self, timestamp, running_user_requests, extra_block_intervals):
        self.timestamp = timestamp
        self.running_user_requests = running_user_requests
        self.extra_blocked_intervals = extra_block_intervals
        
    def running_tracking_numbers(self):
        return [ur.tracking_number for ur in self.running_user_requests.tracking_number]
    
    def running_user_request(self, tracking_number):
        return self.running_user_requests.get(tracking_number, None)
    
    def user_request_for_telescope(self, telescope):
        for ur in self.running_user_requests:
            if ur.telescope == telescope:
                return ur
        
        return None
    
    def blocked_intervals(self):
        intervals = list(self.blocked_intervals())
        for ur in self.running_user_requests:
            intervals.append(ur.to_interval())
        
        return intervals


class SchedulerResult(object):
    
    def __init__(self):
        self.schedule = None
        self.resource_schedules_to_cancel = None
        self.unschedulable_user_request_numbers = None
        self.unschedulable_request_numbers = None
        
        
class SchedulerRunner(object):
    
    def __init__(self, sched_params, scheduler, network_interface, network_model):
        self.run_flag = True
        self.sched_params = sched_params
        self.scheduler = scheduler
        self.network = network_interface
        self.network_model = network_model
        self.log = logging.getLogger(__name__)
        
    def determine_scheduler_now(self):
        '''Use a static command line datetime if provided, or default to utcnow, with a
           little extra to cover the scheduler's run time.'''
        if self.sched_params.simulate_now:
            try:
                now = iso_string_to_datetime(self.sched_params.simulate_now)
            except ValueError as e:
                self.log.critical(e)
                self.log.critical("Invalid datetime provided on command line. Try e.g. '2012-03-03 09:05:00'.")
                self.log.critical("Aborting scheduler run.")
                sys.exit()
        # ...otherwise offset 'now' to account for the duration of the scheduling run
        else:
            now = datetime.utcnow()
    
        self.log.info("Using a 'now' of %s", now)
    
        return now
    
    def scheduler_rerun_required(self):
        ''' Return True if scheduler should be run now
        '''
        network_has_changed = False
        
        if self.network.current_events_has_changed():
            self.log.info("Telescope network events were found.")
            network_has_changed = True
            
        return network_has_changed or self.network.schedulable_request_set_has_changed()
    
    
    def update_network_model(self):
        current_events = self.network.get_current_events()
        for telescope_name, telescope in self.network_model.iteritems():
            if telescope_name in current_events:
                telescope.events.extend(current_events[telescope_name])
                msg = "Found network event for '%s' - removing from consideration (%s)" % (
                                                                    telescope_name,
                                                                    current_events[telescope_name])
                self.log.info(msg)
    
        return
    
    
    def run(self):
        while self.run_flag:
            if self.sched_params.no_weather:
                self.log.info("Weather monitoring disabled on the command line")
            else:
                self.update_network_model()
                
            if self.scheduler_rerun_required():
                self.create_new_schedule()
                
            if self.sched_params.run_once:
                self.run_flag = False
                
            self.log.info("Sleeping for %d seconds", self.sched_params.sleep_seconds)
            time.sleep(self.sched_params.sleep_seconds)
            
    def create_new_schedule(self):
        now = self.determine_scheduler_now();
        estimated_scheduler_end = self.now + timedelta(minutes=6)
        short_estimated_scheduler_end = self.now + timedelta(minutes=2)
        semester_start, semester_end = get_semester_block(dt=short_estimated_scheduler_end)
        
        
        normal_user_requests = []
        too_user_requests    = []
        for ur in self.network.get_all_user_requests(semester_start, semester_end):
            if not self.sched_params.no_too and ur.has_target_of_opportunity():
                too_user_requests.append(ur)
            else:
                normal_user_requests.append(ur)
    
        self.log.info("Received %d ToO User Requests" % len(too_user_requests))
        self.log.info("Received %d Normal User Requests" % len(normal_user_requests))
    
        user_requests_dict = {
                              Request.NORMAL_OBSERVATION_TYPE : normal_user_requests,
                              Request.TARGET_OF_OPPORTUNITY : too_user_requests
                              }
        
        if too_user_requests:
            self.log.info("Start ToO Scheduling")
            user_requests_dict['type'] = Request.TARGET_OF_OPPORTUNITY
            n_urs, n_rs = n_requests(too_user_requests)
            
            try:
                network_snapshot = NetworkSnapshot(datetime.utcnow(),
                                                   self.network_interface.running_user_requests(),
                                                   self.network_interface.too_user_request_intervals_by_telescope())
                scheduler_result = self.scheduler.run_scheduler(user_requests_dict, network_snapshot, self.network_model, short_estimated_scheduler_end)
                
                if not self.sched_params.dry_run:
                    # Set the states of the Requests and User Requests
                    self.network.set_requests_to_unschedulable(scheduler_result.unschedulable_request_numbers)
                    self.network.set_user_requests_to_unschedulable(scheduler_result.unschedulable_user_request_numbers)
                
                # Delete old schedule
                n_deleted = self.network.cancel(short_estimated_scheduler_end, semester_end, self.sched_params.dry_run, scheduler_result.telescope_schedules_to_cancel)
                
                # Write new schedule
                n_submitted = self.network.save(scheduler_result.new_schedule, semester_start, self.sched_params.camreras_file, self.sched_params.dry_run)
                self.write_scheduling_log(n_urs, n_rs, n_deleted, n_submitted, self.sched_params.dry_run)
            except ScheduleException, se:
                self.log.error(pfe, "aborting run")
                
            self.log.info("End ToO Scheduling")
    
        # Run the scheduling loop, if there are any User Requests
        if normal_user_requests:
            self.log.info("Start Normal Scheduling")
            user_requests_dict['type'] = Request.NORMAL_OBSERVATION_TYPE
            n_urs, n_rs = n_requests(normal_user_requests)
            
            try:
                network_snapshot = NetworkSnapshot(datetime.utcnow(),
                                                   self.network_interface.running_user_requests(),
                                                   self.network_interface.too_user_request_intervals_by_telescope())                
                scheduler_result = self.scheduler.run_scheduler(user_requests_dict, network_snapshot, self.network_model, estimated_scheduler_end)
                if not self.sched_params.dry_run:
                    # Set the states of the Requests and User Requests
                    self.network.set_requests_to_unschedulable(scheduler_result.unschedulable_request_numbers)
                    self.network.set_user_requests_to_unschedulable(scheduler_result.unschedulable_user_request_numbers)
                
                # Delete old schedule
                n_deleted = self.network.cancel(short_estimated_scheduler_end, semester_end, self.sched_params.dry_run, scheduler_result.telescope_schedules_to_cancel)
                
                # Write new schedule
                n_submitted = self.network.save(scheduler_result.new_schedule, semester_start, self.sched_params.camreras_file, self.sched_params.dry_run)
                self.write_scheduling_log(n_urs, n_rs, n_deleted, n_submitted, self.sched_params.dry_run)
            except ScheduleException, se:
                self.log.error(pfe, "aborting run")
            
            self.log.info("End Normal Scheduling")
    
        else:
            self.log.warn("Received no User Requests! Skipping this scheduling cycle")
        sys.stdout.flush()
        
            
