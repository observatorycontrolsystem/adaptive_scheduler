from adaptive_scheduler.printing         import pluralise as pl
from adaptive_scheduler.model2           import RequestError
from pond                                import get_network_running_blocks, get_network_running_intervals, cancel_schedule, PondFacadeException, send_schedule_to_pond, get_blocks_by_tracking_number
from adaptive_scheduler.utils            import timeit
from reqdb.client                        import ConnectionError, RequestDBError
from adaptive_scheduler.request_parser   import TreeCollapser
from adaptive_scheduler.tree_walker      import RequestMaxDepthFinder

import logging



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
    

class PondRunningRequest(RunningRequest):
    
    def __init__(self, request_number, block_id, start, end):
        RunningRequest.__init__(self, request_number)
        self.block_id = block_id
        self.start = start
        self.end = end

    def __str__(self):
        return RunningRequest.__str__(self) + ", block_id: %s, start: %s, end: %s" % (self.block_id, self.start, self.end)


class RequestDBInterface(object):
    
    def __init__(self, requestdb_client):
        self.requestdb_client = requestdb_client
        # Force a reschedule when first started
        self.requestdb_client.set_dirty_flag()
        self.log = logging.getLogger(__name__)
    
    
    def _request_db_dirty_flag_is_invalid(self, dirty_response):
        try:
            dirty_response['dirty']
            return False
        except TypeError as e:
            self.log.critical("Request DB could not update internal state. Aborting current scheduling loop.")
            return True
    
    
    @timeit
    def _get_dirty_flag(self):
        dirty_response = dict(dirty=False)
        try:
            dirty_response = self.requestdb_client.get_dirty_flag()
        except ConnectionError as e:
            self.log.warn("Error retrieving dirty flag from DB: %s", e)
            self.log.warn("Skipping this scheduling cycle")
    
        #TODO: HACK to handle not a real error returned from Request DB
        if self._request_db_dirty_flag_is_invalid(dirty_response):
            dirty_response = dict(dirty=False)
    
        if dirty_response['dirty'] is False:
            self.log.info("Request DB is still clean - nothing has changed")
    
        else:
            msg  = "Got dirty flag (DB needs reading) with timestamp"
            msg += " %s (last updated %s)" % (dirty_response['timestamp'],
                                              dirty_response['last_updated'])
            self.log.info(msg)
    
        return dirty_response
    
    
    def _clear_dirty_flag(self):
        # Clear the dirty flag
        self.log.info("Clearing dirty flag")
        try:
            self.requestdb_client.clear_dirty_flag()
            return True
        except ConnectionError as e:
            self.log.critical("Error clearing dirty flag on DB: %s", e)
            self.log.critical("Aborting current scheduling loop.")
    
        return False
        
    def _was_dirty_and_cleared(self):
        dirty_response = self._get_dirty_flag()
    
        if dirty_response['dirty'] is True:
            if self._clear_dirty_flag():
                return True
    
        return False
    
    
    def _get_requests(self, start, end):
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
    
    
    def _collapse_requests(self, requests):
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
    
    
    def is_dirty(self):
        '''True if list of schedulable requests has changed since
        dirty flag was last cleared
        '''
        db_is_dirty = False
    
        if self._was_dirty_and_cleared():
            self.self.log.info("Dirty flag was found set and cleared.")
            db_is_dirty = True
            
        return db_is_dirty
    
    def get_all_user_requests(self, start, end):
        '''Get all user requests waiting for scheduling between
        start and end date
        '''
        json_user_requests = self._get_requests(start, end)
    
        # Collapse each request tree
        json_user_requests = self._collapse_requests(json_user_requests)
        model_builder = self.sched_params.get_model_builder()
    
        all_user_requests = []
        for json_user_request in json_user_requests:
            try:
                user_request = model_builder.build_user_request(json_user_request)
                all_user_requests.append(user_request)
            except RequestError as e:
                self.log.warn(e)
                
        return all_user_requests
                
                
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



class PondScheduleInterface(object):
    
    def __init__(self):
        
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
    


class NetworkInterface(object):
    
    def __init__(self, schedule_interface, user_request_interface, network_state_interface):
        self.network_schedule_interface = schedule_interface
        self.user_request_interface = user_request_interface
        self.network_state_interface = network_state_interface
        
    def schedulable_request_set_has_changed(self):
        '''True if set of schedulable requests has changed
        '''
        return self.user_request_interface.is_dirty()
    
    def get_all_user_requests(self, start, end):
        '''Get all user requests waiting for scheduling between
        start and end date
        '''
        return self.user_request_interface.get_all_user_requests(start, end)            
                
    def set_requests_to_unschedulable(self, unschedulable_r_numbers):
        '''Update the state of all the unschedulable Requests in the DB in one go.'''
        return self.user_request_interface.set_requests_to_unschedulable(unschedulable_r_numbers)
    
    def set_user_requests_to_unschedulable(self, unschedulable_ur_numbers):
        '''Update the state of all the unschedulable User Requests in the DB in one go.'''
        return self.user_request_interface.set_user_requests_to_unschedulable(unschedulable_ur_numbers)
    
    def running_user_requests_by_tracking_number(self):
        ''' Return RunningUserRequest objects indexed by tracking number
        '''
        return self.network_schedule_interface.running_user_requests_by_tracking_number()
    
    def running_user_requests_by_telescope(self):
        ''' Return RunningUserRequest objects indexed by telescope
        '''
        return self.network_schedule_interface.running_user_requests_by_telescope()
        
    def running_user_request_intervals_by_telescope(self):
        ''' Return the current run intervals by telescope
        '''
        return self.network_schedule_interface.running_user_request_intervals_by_telescope()
       
    def too_user_request_intervals_by_telescope(self):
        ''' Return the schedule ToO intervals for the supplied telescope
        '''
        return self.network_schedule_interface.too_user_request_intervals_by_telescope()
    
    def cancel(self, start, end, dry_run=False, tels=None):
        ''' Cancel the current scheduler between start and end
        Return the number of deleted requests
        '''
        return self.network_schedule_interface.cancel(start, end, dry_run, tels)
            
    def save(self, schedule, semester_start, camera_mappings, dry_run=False):
        ''' Save the provided observing schedule
        Return the number of submitted requests
        '''
        return self.network_schedule_interface.save(schedule, semester_start, camera_mappings, dry_run)
    
    def get_current_events(self):
        ''' Get the current network events
        '''
        return self.network_state_interface.update()
    
    def current_evenvts_has_changed(self):
        ''' Return True if the current network state is different from
            the previous network state.
        '''
        return self.network_state_interface.has_changed()
    

