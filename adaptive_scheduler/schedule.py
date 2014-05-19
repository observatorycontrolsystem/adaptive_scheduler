from adaptive_scheduler.printing import pluralise as pl
from schedutils.semester_service         import get_semester_block
from reqdb.requests import Request
from adaptive_scheduler.model2 import RequestError, n_requests

import sys
import logging
from datetime import datetime, timedelta
# log = logging.getLogger(__name__)



class ScheduleException(Exception):
    pass



class RunningUserRequest(object):
    
    def __init__(self, tracking_number, *running_requests):
        self.tracking_number = tracking_number
        self.running_requests = running_requests
        
    def add_running_request(self, running_request):
        self.running_requests.append(running_request)
    
    def __str__(self):
        r_str = ','.join(['(%s)' % str(r) for r in self.running_requests])
        ur_str = 'Tracking Number: %s, Running Requests: [%s]' % (self.trackig_number, r_str)
        
        return ur_str


class RunningRequest(object):

    def __init__(self, request_number):
        self.request_number = request_number
        
    def __str__(self):
        return 'Request Number: %s' % (self.request_number)
    

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
        
        
class RequestDBSchedulerParameters(SchedulerParameters):
    
    def __init__(self, requestdb_url, **kwargs):
        SchedulerParameters.__init__(self, **kwargs)
        self.requestdb_url = requestdb_url
    

# import time
# import sys
# from datetime import datetime, timedelta
# from reqdb.requests import Request
# from adaptive_scheduler.model2 import RequestError, n_requests
# from schedutils.semester_service         import get_semester_block
# from adaptive_scheduler.feedback        import TimingLogger
# from schedutils.semester_service import get_semester_code
# from adaptive_scheduler.printing import (print_schedule, print_compound_reservations,
#                                           summarise_urs, log_full_ur, log_windows)
# from adaptive_scheduler.model2          import (filter_out_compounds,
#                                                  differentiate_by_type, n_requests,
#                                                   )
# from adaptive_scheduler.kernel_mappings import (construct_visibilities,
#                                                  construct_resource_windows,
#                                                  make_compound_reservations,
#                                                  make_many_type_compound_reservations,
#                                                  filter_for_kernel,
#                                                  construct_global_availability)
# from adaptive_scheduler.request_filters import filter_urs, drop_empty_requests, find_unschedulable_ur_numbers, set_now
# from adaptive_scheduler.event_utils import report_scheduling_outcome
# from adaptive_scheduler.kernel.fullscheduler_gurobi import FullScheduler_gurobi as FullScheduler
# from adaptive_scheduler.kernel.intervals import Intervals
# from adaptive_scheduler.utils import timeit
# from timeit import itertools
# from collections import defaultdict

        
from adaptive_scheduler.scheduler import Scheduler
from reqdb.client import SchedulerClient, ConnectionError
from adaptive_scheduler.utils            import timeit, iso_string_to_datetime
from reqdb.client                import ConnectionError, RequestDBError
from adaptive_scheduler.request_parser  import TreeCollapser
from adaptive_scheduler.tree_walker     import RequestMaxDepthFinder

class RequestDBScheduler(Scheduler):
    
    def __init__(self, sched_params, network):
        Scheduler.__init__(sched_params, network)
        self.requestdb_client = SchedulerClient(sched_params.requestdb_url)
        # Force a reschedule when first started
        self.requestdb_client.set_dirty_flag()
        self.log = logging.getLogger(__name__)
    
    
    def request_db_dirty_flag_is_invalid(self, dirty_response):
        try:
            dirty_response['dirty']
            return False
        except TypeError as e:
            self.log.critical("Request DB could not update internal state. Aborting current scheduling loop.")
            return True
    
    
    @timeit
    def get_dirty_flag(self):
        dirty_response = dict(dirty=False)
        try:
            dirty_response = self.requestdb_client.get_dirty_flag()
        except ConnectionError as e:
            self.log.warn("Error retrieving dirty flag from DB: %s", e)
            self.log.warn("Skipping this scheduling cycle")
    
        #TODO: HACK to handle not a real error returned from Request DB
        if self.request_db_dirty_flag_is_invalid(dirty_response):
            dirty_response = dict(dirty=False)
    
        if dirty_response['dirty'] is False:
            self.log.info("Request DB is still clean - nothing has changed")
    
        else:
            msg  = "Got dirty flag (DB needs reading) with timestamp"
            msg += " %s (last updated %s)" % (dirty_response['timestamp'],
                                              dirty_response['last_updated'])
            self.log.info(msg)
    
        return dirty_response
    
    
    def clear_dirty_flag(self):
        # Clear the dirty flag
        self.log.info("Clearing dirty flag")
        try:
            self.requestdb_client.clear_dirty_flag()
            return True
        except ConnectionError as e:
            self.log.critical("Error clearing dirty flag on DB: %s", e)
            self.log.critical("Aborting current scheduling loop.")
    
        return False
        
    def was_dirty_and_cleared(self):
        dirty_response = self.get_dirty_flag()
    
        if dirty_response['dirty'] is True:
            if self.clear_dirty_flag():
                return True
    
        return False
    
    
    def get_requests(self, start, end):
        from adaptive_scheduler.requestdb     import get_requests_from_db
        # Try and get the requests
        try:
            requests = get_requests_from_db(self.requestdb_client.url, 'dummy arg',
                                            start, end)
            self.log.info("Got %d %s from Request DB", *pl(len(requests), 'User Request'))
            return requests
    
        except ConnectionError as e:
            self.log.warn("Error retrieving Requests from DB: %s", e)
            self.log.warn("Skipping this scheduling cycle")
        
        return []
    
    
    def collapse_requests(self, requests):
        collapsed_reqs = []
        for i, req_dict in enumerate(requests):
    
            tc = TreeCollapser(req_dict)
            tc.collapse_tree()
    
            if tc.is_collapsible:
                self.log.debug("Request %d was successfully collapsed!", i)
    
                depth_finder = RequestMaxDepthFinder(tc.collapsed_tree)
                depth_finder.walk()
    
                # The scheduling kernel can't handle more than one level of nesting
                if depth_finder.max_depth > 1:
                    self.log.debug("Request %d is still too deep (%d levels) - skipping.", i,
                                                                      depth_finder.max_depth)
    
                else:
    #                self.log.debug("Request %d has depth %d - continuing.", i,
    #                                                                  depth_finder.max_depth)
                    collapsed_reqs.append(tc.collapsed_tree)
    
            else:
                self.log.debug("Request %d could not be collapsed - skipping.", i)
    
    
        return collapsed_reqs
    
    
    # Overrides superclass method
    def scheduler_rerun_required(self):
        db_is_dirty         = False
    
        if self.was_dirty_and_cleared():
            self.self.log.info("Dirty flag was found set and cleared.")
            db_is_dirty = True
            
        return db_is_dirty or Scheduler.scheduler_rerun_required(self)
    
    
    # Overrides superclass method
    def get_all_user_requests(self, start, end):
        json_user_requests = self.get_requests(start, end)
    
        # Collapse each request tree
        json_user_requests = self.collapse_requests(json_user_requests)
        model_builder = self.sched_params.get_model_builder()
    
        all_user_requests = []
        for json_user_request in json_user_requests:
            try:
                user_request = model_builder.build_user_request(json_user_request)
                all_user_requests.append(user_request)
            except RequestError as e:
                self.log.warn(e)
                
        return all_user_requests
                
                
    # Overrides superclass method
    def set_requests_to_unschedulable(self, unschedulable_r_numbers):
        '''Update the state of all the unschedulable Requests in the DB in one go.'''
        try:
            self.requestdb_client.set_request_state('UNSCHEDULABLE', unschedulable_r_numbers)
        except ConnectionError as e:
            self.log.error("Problem setting Request states to UNSCHEDULABLE: %s" % str(e))
        except RequestDBError as e:
            msg = "Internal RequestDB error when setting UNSCHEDULABLE Request states: %s" % str(e)
            self.log.error(msg)
    
        return
    
    
    # Overrides superclass method
    def set_user_requests_to_unschedulable(self, unschedulable_ur_numbers):
        '''Update the state of all the unschedulable User Requests in the DB in one go.'''
        try:
            self.requestdb_client.set_user_request_state('UNSCHEDULABLE', unschedulable_ur_numbers)
        except ConnectionError as e:
            self.log.error("Problem setting User Request states to UNSCHEDULABLE: %s" % str(e))
        except RequestDBError as e:
            msg = "Internal RequestDB error when setting UNSCHEDULABLE User Request states: %s" % str(e)
            self.log.error(msg)
    
        return


    
class SchedulerRun(object):
    
    def __init__(self, sched_params, now, current_events, too_tracking_numbers):
        self.sched_params = sched_params
        self.current_events = current_events
        self.too_tracking_numbers= too_tracking_numbers
        
        self.now = now
        self.estimated_scheduler_end = self.now + timedelta(minutes=6)
        self.short_estimated_scheduler_end = self.now + timedelta(minutes=2)
        self.semester_start, self.semester_end = get_semester_block(dt=self.short_estimated_scheduler_end)
    
    
    def create_new_schedule(self):
        normal_user_requests = []
        too_user_requests    = []
        for ur in self.get_all_user_requests(self.semester_start, self.semester_end):
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
                self.run_scheduler(user_requests_dict, self)
                
                if not self.sched_params.dry_run:
                    # Set the states of the Requests and User Requests
                    self.set_requests_to_unschedulable(self.unschedulable_r_numbers)
                    self.set_user_requests_to_unschedulable(self.unschedulable_ur_numbers)
                
                # Delete old schedule
                n_deleted = self.cancel(self.short_estimated_scheduler_end, self.semester_end, self.sched_params.dry_run, self.tels_to_cancel)
                
                # Write new schedule
                n_submitted = self.save(self.new_schedule, self.semester_start, self.sched_params.camreras_file, self.sched_params.dry_run)
                self.write_scheduling_log(n_urs, n_rs, n_deleted, n_submitted, self.sched_params.dry_run)
            except ScheduleException, pfe:
                self.log.error(pfe, "aborting run")
                
            self.log.info("End ToO Scheduling")
    
        # Run the scheduling loop, if there are any User Requests
        if normal_user_requests:
            self.log.info("Start Normal Scheduling")
            user_requests_dict['type'] = Request.NORMAL_OBSERVATION_TYPE
            n_urs, n_rs = n_requests(normal_user_requests)
            
            try:
                self.run_scheduler(user_requests_dict, self)
                if not self.sched_params.dry_run:
                    # Set the states of the Requests and User Requests
                    self.set_requests_to_unschedulable(self.unschedulable_r_numbers)
                    self.set_user_requests_to_unschedulable(self.unschedulable_ur_numbers)
                
                # Delete old schedule
                n_deleted = self.cancel(self.short_estimated_scheduler_end, self.semester_end, self.sched_params.dry_run, self.tels_to_cancel)
                
                # Write new schedule
                n_submitted = self.save(self.new_schedule, self.semester_start, self.sched_params.camreras_file, self.sched_params.dry_run)
                self.write_scheduling_log(n_urs, n_rs, n_deleted, n_submitted, self.sched_params.dry_run)
            except ScheduleException, pfe:
                self.log.error(pfe, "aborting run")
            
            self.log.info("End Normal Scheduling")
    
        else:
            self.log.warn("Received no User Requests! Skipping this scheduling cycle")
        sys.stdout.flush()
    
    
    def set_schedule(self, schedule):
        self.new_schedule = schedule
    
    def set_tels_to_cancel(self, tels_to_cancel):
        self.tels_to_cancel = tels_to_cancel
        
    def set_unschedulable_ur_numbers(self, unschdulable_ur_numbers):
        self.unschdulable_ur_numbers = unschdulable_ur_numbers
        
    def set_unschedulable_r_numbers(self, unschedulable_r_numbers):
        self.unschdulable_r_numbers = unschedulable_r_numbers
    
    
    def running_user_requests_by_tracking_number(self):
        ''' Return RunningUserRequest objects indexed by tracking number
        '''
        pass
    
    def running_user_requests_by_telescope(self):
        ''' Return RunningUserRequest objects indexed by telescope
        '''
        pass
        
    def running_user_request_intervals_by_telescope(self):
        ''' Return the current run intervals by telescope
        '''
        pass
       
    def too_user_request_intervals_by_telescope(self):
        ''' Return the schedule ToO intervals for the supplied telescope
        '''
        pass
    
    
    def cancel(self, start, end, dry_run=False, tels=None):
        ''' Cancel the current scheduler between start and end
        Return the number of deleted requests
        '''
        pass
            
    def save(self, schedule, semester_start, camera_mappings, dry_run=False):
        ''' Save the provided observing schedule
        Return the number of submitted requests
        '''
        pass


class RunState(object):
    
    def __init__(self, tracking_number, start, end):
        self.tracking_number = tracking_number
        self.start = start
        self.end = end
    


from pond import get_network_running_blocks, get_network_running_intervals, cancel_schedule, PondFacadeException, send_schedule_to_pond, get_blocks_by_tracking_number

class PondRunningRequest(RunningRequest):
    
    def __init__(self, request_number, block_id, start, end):
        RunningRequest.__init__(self, request_number)
        self.block_id = block_id
        self.start = start
        self.end = end

    def __str__(self):
        return RunningRequest.__str__(self) + ", block_id: %s, start: %s, end: %s" % (self.block_id, self.start, self.end)


class PondSchedulerRun(SchedulerRun):
    
    def __init__(self, sched_params, now, current_events, too_tracking_numbers):
        SchedulerRun.__init__(self, sched_params, now, current_events, too_tracking_numbers)
        
        #Fetch the data
        self.running_blocks_by_telescope = self._fetch_running_blocks()
        self.running_intervals_by_telescope = get_network_running_intervals(self.running_blocks)
        self.too_intervals_by_telescope = self._fetch_too_blocks()

    
    def _fetch_running_blocks(self):
        try:
            running_blocks = get_network_running_blocks(self.telescopes, self.blocks_end_after, self.blocks_running_if_starts_before, self.blocks_start_before)
        except PondFacadeException, pfe:
            raise ScheduleException(pfe, "Unable to get running blocks from POND")
        
        # This is just logging held over from when this was in the scheduling loop
        all_running_blocks = []
        for blocks in self.running_blocks.values():
            all_running_blocks += blocks
        self.log.info("%d %s in the running list", *pl(len(all_running_blocks), 'POND Block'))
        for block in all_running_blocks:
            msg = "UR %s has a running block (id=%d, finishing at %s)" % (
                                                         block.tracking_num_set()[0],
                                                         block.id,
                                                         block.end
                                                       )
            self.log.debug(msg)
        # End of logging block
        
        return running_blocks 
    
    def _fetch_too_blocks(self):
        too_blocks = get_blocks_by_tracking_number(self.too_tracking_numbers, self.telescopes,
                                           self.blocks_ends_after,
                                           self.blocks_start_before)
        
        return too_blocks
    
    
    def running_user_requests_by_tracking_number(self):
        running_urs = {}
        for blocks in self.running_blocks_by_telescope.values():
            for block in blocks:
                tracking_number = block.tracking_num_set()[0]
                running_ur = running_urs.setdefault(tracking_number, RunningUserRequest(tracking_number))
                running_ur.add_running_request(blocks.request_number_set[0], block.id, block.start, block.end)
            
        return running_urs
    
    def running_user_requests_by_telescope(self):
        result = {}
        for tel in self.telescopes:
            result[tel] = None
            if self.running_blocks_by_telescope.get(tel, []):
                result[tel] = self.running_blocks_by_telescope.get(tel)
        
        return result
        
        
    def running_user_request_intervals_by_telescope(self):
        ''' Return the current run intervals by telescope
        '''
        return self.running_intervals_by_telescope
       
    def too_user_request_intervals_by_telescope(self):
        ''' Return the schedule ToO intervals for the supplied telescope
        '''
        return self.too_intervals_by_telescope
    
    
    def cancel(self, start, end, dry_run=False, tels=None):
        ''' Cancel the current scheduler between start and end
        '''
        # Clean out all existing scheduled blocks during a normal run but not ToO
        if tels is None:
            tels = self.telescopes
            
        n_deleted = 0
        try:
            n_deleted = cancel_schedule(self.telescopes, start, end, dry_run)
        except PondFacadeException, pfe:
            raise ScheduleException(pfe, "Unable to cancel POND schedule")
            
        return n_deleted
            
    def save(self, schedule, semester_start, camera_mappings, dry_run=False):
        ''' Save the provided observing schedule
        '''
        # Convert the kernel schedule into POND blocks, and send them to the POND
        n_submitted = send_schedule_to_pond(schedule, semester_start,
                                            camera_mappings, dry_run)
        
        return n_submitted