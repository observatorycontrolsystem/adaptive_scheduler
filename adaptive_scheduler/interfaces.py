from time_intervals.intervals import Intervals

import os
import pickle
import logging
from datetime import datetime, timedelta
from redis import Redis

redis = Redis(host=os.getenv('REDIS_URL', 'redisdev'), db=0, password='schedulerpass', socket_connect_timeout=15,
              socket_timeout=30)

logger = logging.getLogger(__name__)


class ScheduleException(Exception):
    pass


class RunningRequestGroup(object):
    
    def __init__(self, id, *running_requests):
        self.id = int(id)
        self.running_requests = list(running_requests)
        
    def add_running_request(self, running_request):
        self.running_requests.append(running_request)
    
    def __str__(self):
        r_str = ','.join(['(%s)' % str(r) for r in self.running_requests])
        rg_str = 'Request Group Id: %s, Running Requests: [%s]' % (self.id, r_str)
        
        return rg_str


class RunningRequest(object):

    def __init__(self, resource, id, start, end):
        self.resource = resource
        self.id = int(id)
        self.start = start
        self.end = end
        self._errors = []

    def add_error(self, err_str):
        self._errors.append(err_str)

    def errors(self):
        return list(self._errors)
    
    def should_continue(self):
        return len(self.errors()) == 0

    def timepoints(self):
        timepoint_list = []
        timepoint_list.append({'time': self.start, 'type': 'start'})
        timepoint_list.append({'time': self.end, 'type': 'end'})
        
        return timepoint_list

    def __str__(self):
        return 'Request Number: %s at telescope %s, start: %s, end: %s' % (self.id, self.resource, self.start, self.end)
    

class ResourceUsageSnapshot(object):
    
    def __init__(self, timestamp, running_request_groups, extra_block_intervals):
        self.timestamp = timestamp
        self.running_request_groups_by_id = {}
        self.running_request_groups_by_resource = {}
        for running_rg in running_request_groups:
            self.running_request_groups_by_id[running_rg.id] = running_rg
            for running_request in running_rg.running_requests:
                running_request_group_list = self.running_request_groups_by_resource.setdefault(running_request.resource, [])
                running_request_group_list.append(running_rg)
        self.extra_blocked_intervals = extra_block_intervals
        
    def running_request_group_ids(self):
        return self.running_request_groups_by_id.keys()
    
    def running_request_groups(self):
        return self.running_request_groups_by_id.values()
    
    def running_request_group(self, request_group_id):
        return self.running_request_groups_by_id.get(request_group_id, None)
    
    def request_groups_for_resource(self, resource):
        return self.running_request_groups_by_resource.get(resource, [])

    def _running_intervals(self, resource):
        intervals_list = []
        running_rgs = self.running_request_groups_by_resource.get(resource, [])
        for running_rg in running_rgs:
            for running_r in running_rg.running_requests:
                # Only consider the interval running if the request should continue running 
                if running_r.should_continue():
                    intervals_list.append((running_r.start, running_r.end))
        intervals = Intervals(intervals_list)
        
        return intervals

    def blocked_intervals(self, resource):
        return self.extra_blocked_intervals.get(resource, Intervals([]))

    def running_intervals(self, resource):
        return self._running_intervals(resource)

    def running_requests_for_resources(self, resources):
        '''Get the set of running requests for the named resource'''
        running_requests = []
        for running_rg in self.running_request_groups():
            for running_r in running_rg.running_requests:
                if running_r.resource in resources:
                    running_requests.append(running_r)
        
        return running_requests


class NetworkInterface(object):
    
    def __init__(self, schedule_interface, observation_portal_interface, network_state_interface, configdb_interface):
        self.network_schedule_interface = schedule_interface
        self.observation_portal_interface = observation_portal_interface
        self.configdb_interface = configdb_interface
        self.network_state_interface = network_state_interface
        
    def _running_request_groups_by_id(self):
        ''' Return RunningRequestGroup objects indexed by tracking number
        '''
        return self.network_schedule_interface.running_request_groups_by_id()
            
    def _rr_request_group_intervals_by_telescope(self):
        ''' Return the schedule RR intervals for the supplied telescope
        '''
        return self.network_schedule_interface.rr_request_group_intervals_by_telescope()
        
    def schedulable_request_set_has_changed(self):
        '''True if set of schedulable requests or observations have changed
        '''
        try:
            last_changed_check = redis.get('scheduler_last_changed_check_time')
            if not last_changed_check:
                last_changed_check = datetime.utcnow() - timedelta(days=365)
            else:
                last_changed_check = pickle.loads(last_changed_check)
            now = datetime.utcnow()
            last_changed = self.observation_portal_interface.get_last_changed()
            redis.set('scheduler_last_changed_check_time', pickle.dumps(now))
            if last_changed > last_changed_check:
                return True
            else:
                return False
        except Exception as e:
            logger.warn("Failed to check the last_changed time properly: {}".format(repr(e)))
            return True

    def get_all_request_groups(self, start, end):
        '''Get all user requests waiting for scheduling between
        start and end date
        '''
        return self.observation_portal_interface.get_all_request_groups(start, end)

    def cancel(self, cancelation_date_list_by_resource, include_rr, include_normal):
        ''' Cancel the current scheduler between start and end
        Return the number of deleted requests
        '''
        return self.network_schedule_interface.cancel(cancelation_date_list_by_resource, include_rr,
                                                      include_normal)
    
    def abort(self, running_request):
        return self.network_schedule_interface.abort(running_request)
            
    def save(self, schedule, semester_start, dry_run=False):
        ''' Save the provided observing schedule
        Return the number of submitted requests
        '''
        return self.network_schedule_interface.save(schedule, semester_start, self.configdb_interface, dry_run)
    
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
                                     self._running_request_groups_by_id().values(),
                                     {})


class CachedInputNetworkInterface(object):
    
    def __init__(self, input_file_name):
        self.input_file_name = input_file_name
        input_file = open(self.input_file_name, 'r')
        input_data = pickle.load(input_file)
        input_file.close()
        self.json_request_group_list = input_data['json_request_group_list']
        self.resource_usage_snapshot_data = input_data['resource_usage_snapshot']
        
    def schedulable_request_set_has_changed(self):
        '''True if set of schedulable requests has changed
        '''
        return True
    
    def clear_schedulable_request_set_changed_state(self):
        '''True if set of schedulable requests has changed
        '''
        return True
    
    def get_all_request_groups(self, start, end):
        '''Get all user requests waiting for scheduling between
        start and end date
        '''
        return self.json_request_group_list
                
    def set_requests_to_unschedulable(self, unschedulable_r_ids):
        '''Update the state of all the unschedulable Requests in the DB in one go.'''
        pass
    
    def set_request_groups_to_unschedulable(self, unschedulable_rg_ids):
        '''Update the state of all the unschedulable Request Groups in the DB in one go.'''
        pass
    
    def cancel(self, cancelation_date_list_by_resource, include_toos, include_normals):
        ''' Cancel the current scheduler between start and end
        Return the number of deleted requests
        '''
        return 0
    
    def abort(self, running_request):
        ''' Abort a running request"
        '''
        pass
            
    def save(self, schedule, semester_start, dry_run=False):
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
    
    # TODO: Remove rr_request_group_ids, the scheduler should be able to remember what is scheduled during last run
    def resource_usage_snapshot(self, resources, snapshot_start, snapshot_end, request_group_priorities, rr_request_group_ids):
        return self.resource_usage_snapshot_data
    

