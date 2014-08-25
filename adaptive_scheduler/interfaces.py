from adaptive_scheduler.kernel.intervals import Intervals
from adaptive_scheduler.kernel.timepoint import Timepoint

import pickle
# import logging
from datetime import datetime



class ScheduleException(Exception):
    pass


class RunningUserRequest(object):
    
    def __init__(self, tracking_number, *running_requests):
        self.tracking_number = tracking_number
        self.running_requests = list(running_requests)
        
    def add_running_request(self, running_request):
        self.running_requests.append(running_request)
    
    def __str__(self):
        r_str = ','.join(['(%s)' % str(r) for r in self.running_requests])
        ur_str = 'Tracking Number: %s, Running Requests: [%s]' % (self.tracking_number, r_str)
        
        return ur_str


class RunningRequest(object):

    def __init__(self, resource, request_number, start, end):
        self.resource = resource
        self.request_number = request_number
        self.start = start
        self.end = end
        self._errors = []
        
    
    def add_error(self, err_str):
        self._errors.append(err_str)
        
    
    def errors(self):
        return list(self._errors)
    
    
    def should_continue(self):
        return len(self.errors()) == 0
        
    def __str__(self):
        return 'Request Number: %s at telescope %s, start: %s, end: %s' % (self.request_number, self.resource, self.start, self.end)
    

class ResourceUsageSnapshot(object):
    
    def __init__(self, timestamp, running_user_requests, extra_block_intervals):
        self.timestamp = timestamp
        self.running_user_requests_by_tracking_number = {}
        self.running_user_requests_by_resource = {}
        for running_ur in running_user_requests:
            self.running_user_requests_by_tracking_number[running_ur.tracking_number] = running_ur
            for running_request in running_ur.running_requests:
                running_user_request_list = self.running_user_requests_by_resource.setdefault(running_request.resource, [])
                running_user_request_list.append(running_ur)
        self.extra_blocked_intervals = extra_block_intervals
        
    def running_tracking_numbers(self):
        return self.running_user_requests_by_tracking_number.keys()
    
    def running_user_requests(self):
        return self.running_user_requests_by_tracking_number.values()
    
    def running_user_request(self, tracking_number):
        return self.running_user_requests_by_tracking_number.get(tracking_number, None)
    
    def user_requests_for_resource(self, resource):
        return self.running_user_requests_by_resource.get(resource, [])
    
    def blocked_intervals(self, resource):
        return self.extra_blocked_intervals.get(resource, Intervals([]))
    
    def running_intervals(self, resource):
        timepoint_list = []
        running_urs = self.running_user_requests_by_resource.get(resource, [])
        for running_ur in running_urs:
            for running_r in running_ur.running_requests:
                timepoint_list.append(Timepoint(running_r.start, 'start'))
                timepoint_list.append(Timepoint(running_r.end, 'end'))
        intervals = Intervals(timepoint_list)
        
        return intervals
    
    
    def running_requests_for_resources(self, resources):
        '''Get the set of running requests for the named resource'''
        running_requests = []
        for running_ur in self.running_user_requests():
            for running_r in running_ur.running_requests:
                if running_r.resource in resources:
                    running_requests.append(running_r)
        
        return running_requests


class NetworkInterface(object):
    
    def __init__(self, schedule_interface, user_request_interface, network_state_interface):
        self.network_schedule_interface = schedule_interface
        self.user_request_interface = user_request_interface
        self.network_state_interface = network_state_interface
        
    def _running_user_requests_by_tracking_number(self):
        ''' Return RunningUserRequest objects indexed by tracking number
        '''
        return self.network_schedule_interface.running_user_requests_by_tracking_number()
            
    def _too_user_request_intervals_by_telescope(self):
        ''' Return the schedule ToO intervals for the supplied telescope
        '''
        return self.network_schedule_interface.too_user_request_intervals_by_telescope()
        
    def schedulable_request_set_has_changed(self):
        '''True if set of schedulable requests has changed
        '''
        return self.user_request_interface.is_dirty()
    
    def clear_schedulable_request_set_changed_state(self):
        '''True if set of schedulable requests has changed
        '''
        return self.user_request_interface.clear_dirty_flag()
    
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
    
    def cancel(self, cancelation_dates_by_resource, reason):
        ''' Cancel the current scheduler between start and end
        Return the number of deleted requests
        '''
        return self.network_schedule_interface.cancel(cancelation_dates_by_resource)
    
    def abort(self, running_request, reason):
        return self.network_schedule_interface.abort(running_request, reason)
            
    def save(self, schedule, semester_start, camera_mappings, dry_run=False):
        ''' Save the provided observing schedule
        Return the number of submitted requests
        '''
        return self.network_schedule_interface.save(schedule, semester_start, camera_mappings, dry_run)
    
    def get_current_events(self):
        ''' Get the current network events
        '''
        return self.network_state_interface.update()
    
    def current_events_has_changed(self):
        ''' Return True if the current network state is different from
            the previous network state.
        '''
        return self.network_state_interface.has_changed()
    
    def resource_usage_snapshot(self, resources, snapshot_start, snapshot_end):
        now = datetime.utcnow()
        self.network_schedule_interface.fetch_data(resources, snapshot_start, snapshot_end)
        
        return ResourceUsageSnapshot(now,
                              self._running_user_requests_by_tracking_number().values(),
                              self._too_user_request_intervals_by_telescope())
        


class CachedInputNetworkInterface(object):
    
    def __init__(self, input_file_name):
        self.input_file_name = input_file_name
        input_file = open(self.input_file_name, 'r')
        input_data = pickle.load(input_file)
        input_file.close()
        self.json_user_request_list = input_data['json_user_request_list']
        self.resource_usage_snapshot_data = input_data['resource_usage_snapshot']
        
    def schedulable_request_set_has_changed(self):
        '''True if set of schedulable requests has changed
        '''
        return True
    
    def clear_schedulable_request_set_changed_state(self):
        '''True if set of schedulable requests has changed
        '''
        return True
    
    def get_all_user_requests(self, start, end):
        '''Get all user requests waiting for scheduling between
        start and end date
        '''
        return self.json_user_request_list            
                
    def set_requests_to_unschedulable(self, unschedulable_r_numbers):
        '''Update the state of all the unschedulable Requests in the DB in one go.'''
        pass
    
    def set_user_requests_to_unschedulable(self, unschedulable_ur_numbers):
        '''Update the state of all the unschedulable User Requests in the DB in one go.'''
        pass
    
    def cancel(self, cancelation_dates_by_resource):
        ''' Cancel the current scheduler between start and end
        Return the number of deleted requests
        '''
        return 0
    
    def abort(self, running_request, reasons):
        ''' Abort a running request"
        '''
        pass
            
    def save(self, schedule, semester_start, camera_mappings, dry_run=False):
        ''' Save the provided observing schedule
        Return the number of submitted requests
        '''
        return 0
    
    def get_current_events(self):
        ''' Get the current network events
        '''
        return {}
    
    def current_events_has_changed(self):
        ''' Return True if the current network state is different from
            the previous network state.
        '''
        return False
    
    # TODO: Remove too_tracking_numbers, the scheduler should be able to remember what is scheduled during last run
    def resource_usage_snapshot(self, resources, snapshot_start, snapshot_end, user_request_priorities, too_tracking_numbers):
        return self.resource_usage_snapshot_data
    

