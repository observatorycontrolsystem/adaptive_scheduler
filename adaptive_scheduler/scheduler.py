import time
import sys
import logging

from datetime import datetime, timedelta
from reqdb.requests import Request
from adaptive_scheduler.model2 import RequestError, n_requests
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
from adaptive_scheduler.kernel.fullscheduler_gurobi import FullScheduler_gurobi as FullScheduler
from adaptive_scheduler.kernel.intervals import Intervals
from adaptive_scheduler.utils import timeit, iso_string_to_datetime
from adaptive_scheduler.printing import plural_str as pl
from timeit import itertools
from collections import defaultdict

class Scheduler(object):
    
    def __init__(self, sched_params, network, event_bus, scheduler_runner_factory):
        self.visibility_cache = {}
        self.sched_params = sched_params
        self.network = network
        self.event_bus = event_bus
        self.scheduler_runner_factory = scheduler_runner_factory
        self.run_flag = True
        self.log = logging.getLogger(__name__)
        
        # Special timing variables used throughout scheduler
        self.now = None
        self.estimated_scheduler_end = None
        self.short_estimated_scheduler_end = None
        self.semester_start, self.semester_end = None
    
        
    def scheduler_rerun_required(self):
        ''' Return True if scheduler should be run now
        '''
        network_has_changed = False
        
        if self.network.has_changed():
            self.log.info("Telescope network events were found.")
            network_has_changed = True
            
        return network_has_changed
    
    
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
    
    
    def update_telescope_events(self, tels, current_events):
        for telescope_name, telescope in tels.iteritems():
            if telescope_name in current_events:
                telescope.events.extend(current_events[telescope_name])
                msg = "Found network event for '%s' - removing from consideration (%s)" % (
                                                                    telescope_name,
                                                                    current_events[telescope_name])
                self.log.info(msg)
    
        return
    
    
    @timeit
    def blacklist_running_user_requests(self, ur_list, running_ur_tracking_numbers):
        self.log.info("Before applying running blacklist, %d schedulable %s", *pl(len(ur_list), 'UR'))
        all_tns = [ur.tracking_number for ur in ur_list]
        schedulable_tns = set(all_tns) - set(running_ur_tracking_numbers)
        schedulable_urs = [ur for ur in ur_list if ur.tracking_number in schedulable_tns]
        self.log.info("After running blacklist, %d schedulable %s", *pl(len(schedulable_urs), 'UR'))
    
        return schedulable_urs
    
    
    def find_tels_to_preempt(self, visible_too_urs, all_too_urs, normal_urs, tels, scheduler_runner):
        ''' Preempt running blocks, if needed, to run Target of Opportunity user requests'''
    
        #make copy of tels since it could be modified
        tels = dict(tels)
    
        running_user_requests_by_tel = scheduler_runner.running_user_requests_by_telescope()
    
        # Don't preemt another ToO
        # Remove tels with running too from tels
        all_too_tracking_numbers = [ur.tracking_number for ur in all_too_urs]           
        for tel, tracking_number in running_user_requests_by_tel:
            if tracking_number in all_too_tracking_numbers:
                del tels[tel]
    
        value_function_dict = self.construct_value_function_dict(visible_too_urs, normal_urs, tels, running_user_requests_by_tel)
    
        visible_too_tracking_numbers = [ur.tracking_number for ur in visible_too_urs]
        optimal_combination = self.compute_optimal_combination(value_function_dict, visible_too_tracking_numbers, tels)
    
        # get telescopes where the cost of canceling is lowest and there is a running block
        tels_to_cancel = [ combination[0] for combination in optimal_combination
                          if combination[0] in running_user_requests_by_tel and running_user_requests_by_tel[combination[0]]]
    
        return tels_to_cancel
    
    
    
    #TODO - Move to a utils library
    def combine_excluded_intervals(self, excluded_intervals_1, excluded_intervals_2):
        ''' Combine two dictionaries where Intervals are the values '''
        for key in excluded_intervals_2:
            timepoints = excluded_intervals_2[key].timepoints
            excluded_intervals_1.setdefault(key, Intervals([])).add(timepoints)
    
        return excluded_intervals_1
    
    
    def construct_value_function_dict(self, too_urs, normal_urs, tels, running_user_requests_by_tel):
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
            # Compute the priority of the the telescopes without ToOs
            if tel in running_user_requests_by_tel and running_user_requests_by_tel[tel]:
                running_request_priority = 0;
                running_tracking_number = running_user_requests_by_tel[tel]
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
    
    
    # TODO: refactor into smaller chunks
    @timeit
    def run_scheduler(self, user_reqs_dict, scheduler_run):
    
        start_event = TimingLogger.create_start_event(datetime.utcnow())
        self.event_bus.fire_event(start_event)
    
        ONE_MONTH = timedelta(weeks=4)
        ONE_WEEK  = timedelta(weeks=1)
        scheduling_horizon = estimated_scheduler_end + ONE_WEEK
        date_fmt      = '%Y-%m-%d'
        date_time_fmt = '%Y-%m-%d %H:%M:%S'
    
        run_type = user_reqs_dict['type']
        user_reqs = user_reqs_dict[run_type]
        normal_user_requests = user_reqs_dict[Request.NORMAL_OBSERVATION_TYPE]
        too_user_requests = user_reqs_dict[Request.TARGET_OF_OPPORTUNITY]
    
        self.log.info("Scheduling for semester %s (%s to %s)", get_semester_code(),
                                                         self.semester_start.strftime(date_fmt),
                                                         self.semester_end.strftime(date_fmt))
        self.log.info("Scheduling horizon is %s", scheduling_horizon.strftime(date_time_fmt))
    
        self.log.info("Received %s from Request DB", pl(len(user_reqs), 'User Request'))
    
        # Summarise the User Requests we've received
        n_urs, n_rs = n_requests(user_reqs)
    
        self.log.info("Deserialised %s (%s) from Request DB", pl(n_urs, 'User Request'),
                                                         pl(n_rs, 'Request'))
    
        summarise_urs(user_reqs, log_msg="Received from Request DB")
        for ur in user_reqs:
            log_full_ur(ur, estimated_scheduler_end)
            log_windows(ur, log_msg="Initial windows:")
    
    
        if self.sched_params.no_singles:
            self.log.info("Compound Request support (single) disabled at the command line")
            self.log.info("Compound Requests of type 'single' will be ignored")
            singles, others = differentiate_by_type(cr_type='single', crs=user_reqs)
            user_reqs = others
    
        if self.sched_params.no_compounds:
            self.log.info("Compound Request support (and/oneof/many) disabled at the command line")
            self.log.info("Compound Requests of type 'and', 'oneof' or 'many' will be ignored")
            user_reqs = filter_out_compounds(user_reqs)
    
        # TODO: Swap to tels2
        tels = scheduler_run.sched_params.model_builder.tel_network.telescopes
        self.log.info("Available telescopes:")
        for t in sorted(tels):
            self.log.info(str(t))
    
        # TODO: Move this into scheduler run
        # Look for weather events unless weather monitoring has been disabled
        if self.sched_params.no_weather:
            self.log.info("Weather monitoring disabled on the command line")
        else:
            self.update_telescope_events(tels, scheduler_run.current_events)
    
        # Construct visibility objects for each telescope
        self.log.info("Constructing telescope visibilities")
        if not self.visibility_from:
            self.visibility_from = construct_visibilities(tels, self.semester_start, self.semester_end)
    
        running_urs = scheduler_run.running_user_requests_by_tracking_number()
        running_ur_tracking_numbers = running_urs.keys()
        tag = 'RunningBlock'
        for ur in user_reqs:
            if ur.tracking_number in running_ur_tracking_numbers:
                msg = 'User Request is running' % running_urs[ur.tracking_number]
                ur.emit_user_feedback(msg, tag)
                break
    
        # Remove running user requests from consideration, and get the availability edge
        running_ur_tracking_numbers = [running_r.tracking_number for running_r in running_urs]
        user_reqs = self.blacklist_running_user_requests(user_reqs, running_ur_tracking_numbers)
    
        # Filter by window, and set UNSCHEDULABLE on the Request DB as necessary
        self.log.info("Filtering for unschedulability")
        
        set_now(estimated_scheduler_end)
        schedulable_urs, unschedulable_urs = filter_urs(user_reqs)
        self.log.info("Found %d unschedulable %s after filtering", *pl(len(unschedulable_urs), 'UR'))
        unschedulable_ur_numbers = find_unschedulable_ur_numbers(unschedulable_urs)
        unschedulable_r_numbers = drop_empty_requests(schedulable_urs)    
    
        self.log.info("Completed unschedulable filters")
        summarise_urs(schedulable_urs, log_msg="Passed unschedulable filters:")
    
        for ur in schedulable_urs:
            log_windows(ur, log_msg="Remaining windows:")
    
        # Do another check on duration and operator soundness, after dark/rise checking
        self.log.info("Filtering on dark/rise_set")
    
        for tel_name, tel in tels.iteritems():
            if tel.events:
                self.log.info("Bypassing visibility calcs for %s" % tel_name)
    
        visible_urs = filter_for_kernel(schedulable_urs, self.visibility_from, tels,
                                        estimated_scheduler_end, self.semester_end, scheduling_horizon)
    
    
        self.log.info("Completed dark/rise_set filters")
        summarise_urs(visible_urs, log_msg="Passed dark/rise filters:")
        for ur in visible_urs:
            log_windows(ur, log_msg="Remaining windows:")
    
        self.log.info('Filtering complete. Ready to construct Reservations from %d URs.' % len(visible_urs))
    
        # By default, cancel on all telescopes
        tels_to_cancel = dict(tels)
        # Pre-empt running blocks
        if run_type == Request.TARGET_OF_OPPORTUNITY:
            tels_to_cancel = self.find_tels_to_preempt(visible_urs, too_user_requests, normal_user_requests, tels, scheduler_run);  
        
        # TODO: This logic is questionable.  exlculde_intervals in ToO case don't look correct
        # Get TOO requests scheduled in pond, combine with excluded_intervals
        if run_type == Request.NORMAL_OBSERVATION_TYPE and too_user_requests:
            excluded_intervals = self.combine_excluded_intervals(scheduler_run.current_user_request_intervals_by_telescope,
                                                                 scheduler_run.too_user_request_intervals_by_telescope())
        else:
            excluded_intervals = scheduler_run.current_user_request_intervals_by_telescope
    
        # Convert CompoundRequests -> CompoundReservations
        many_urs, other_urs = differentiate_by_type('many', visible_urs)
        to_schedule_many = make_many_type_compound_reservations(many_urs, tels, self.visibility_from,
                                                                self.semester_start)
        to_schedule_other = make_compound_reservations(other_urs, tels, self.visibility_from,
                                                       self.semester_start)
        to_schedule = to_schedule_many + to_schedule_other
    
        # Translate when telescopes are available into kernel speak
        resource_windows = construct_resource_windows(self.visibility_from, self.semester_start)
    
        # Intersect and mask out time where Blocks are currently running
        global_windows = construct_global_availability(self.semester_start,
                                                       excluded_intervals, resource_windows)
    
        print_compound_reservations(to_schedule)
    
        if not to_schedule:
            self.log.info("Nothing to schedule! Skipping kernel call...")
            return self.visibility_from
    
        # Instantiate and run the scheduler
        time_slicing_dict = {}
        for t in tels:
            time_slicing_dict[t] = [0, self.sched_params.slicesize_seconds]
    
        contractual_obligations = []
    
        self.log.info("Instantiating and running kernel")
    
        kernel   = FullScheduler(to_schedule, global_windows, contractual_obligations,
                                 time_slicing_dict)
    
        schedule = kernel.schedule_all(timelimit=self.sched_params.timelimit_seconds)
    
        scheduled_reservations = []
        [scheduled_reservations.extend(a) for a in schedule.values()]
        self.log.info("Scheduling completed. Final schedule has %d Reservations." % len(scheduled_reservations))
    
        report_scheduling_outcome(to_schedule, scheduled_reservations)
    
    
        # Summarise the schedule in normalised epoch (kernel) units of time
        print_schedule(schedule, self.semester_start, self.semester_end)
    
        scheduler_run.set_schedule(schedule)
        scheduler_run.set_tels_to_cancel(tels_to_cancel)
        scheduler_run.set_unschedulable_ur_numbers
        scheduler_run.set_unschedulable_r_numbers
        return scheduler_run

    
    def run(self):
        while self.run_flag:
            current_events = []
            if not self.sched_params.no_weather:
                current_events = self.network.update()
                
            if self.scheduler_rerun_required():
                now = self.determine_scheduler_now();
                scheduler_runner = self.scheduler_runner_factory.create_scheduler_runner(self.sched_params, now, current_events)
                scheduler_runner.create_new_schedule()
                
            if self.sched_params.run_once:
                self.run_flag = False
                
            self.log.info("Sleeping for %d seconds", self.sched_params.sleep_seconds)
            time.sleep(self.sched_params.sleep_seconds)
    
    
    #######
    #
    # Subclasses should override methods below here
    #
    #######
        
    def get_all_user_requests(self):
        return []
    
    
    def set_requests_to_unschedulable(self, unschedulable_r_numbers):
        pass
    
    
    def set_user_requests_to_unschedulable(self, unschedulable_ur_numbers):
        pass



class SchedulerRunnerFactory(object):

    def create_scheduler_runner(self, sched_params, now, current_events):
         return PondSchedulerRun(self.sched_params, now, current_events)
            
