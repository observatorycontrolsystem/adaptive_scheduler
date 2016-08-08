from adaptive_scheduler.utils import timeit, metric_timer, SendMetricMixin
from reqdb.client import SearchQuery, SchedulerClient, ConnectionError, RequestDBError
from adaptive_scheduler.printing         import pluralise as pl
from adaptive_scheduler.request_parser   import TreeCollapser
from adaptive_scheduler.tree_walker      import RequestMaxDepthFinder

from datetime import datetime
import json
import logging
import sys

log = logging.getLogger(__name__)


class RequestDBInterface(object, SendMetricMixin):
    
    def __init__(self, requestdb_client, debug=False):
        self.requestdb_client = requestdb_client
        self.debug = debug
        self.log = logging.getLogger(__name__)
    
    
    def _request_db_dirty_flag_is_invalid(self, dirty_response):
        try:
            dirty_response['dirty']
            return False
        except TypeError as e:
            self.log.critical("Request DB could not update internal state. Aborting current scheduling loop.")
            return True
    
    
    @timeit
    @metric_timer('requestdb.is_dirty')
    def is_dirty(self):
        dirty_response = dict(dirty=False)
        try:
            dirty_response = self.requestdb_client.get_dirty_flag()
        except ConnectionError as e:
            self.log.warn("Error retrieving dirty flag from DB: %s", e)
            self.log.warn("Skipping this scheduling cycle")
            self.send_metric('requestdb.connection_status', 1)
    
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
    
    
    def clear_dirty_flag(self):
        # Clear the dirty flag
        self.log.info("Clearing dirty flag")
        try:
            self.requestdb_client.clear_dirty_flag()
            return True
        except ConnectionError as e:
            self.log.critical("Error clearing dirty flag on DB: %s", e)
            self.log.critical("Aborting current scheduling loop.")
            self.send_metric('requestdb.connection_status', 1)
    
        return False
    
    
    def _get_requests(self, start, end):
        # Try and get the requests
        try:
            requests = get_requests_from_db(self.requestdb_client.url, 'dummy arg',
                                            start, end, self.debug)
            self.log.info("Got %d %s from Request DB", *pl(len(requests), 'User Request'))
            return requests
    
        except ConnectionError as e:
            self.log.warn("Error retrieving Requests from DB: %s", e)
            self.log.warn("Skipping this scheduling cycle")
            self.send_metric('requestdb.connection_status', 1)

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
    
    
    def get_all_user_requests(self, start, end):
        '''Get all user requests waiting for scheduling between
        start and end date
        '''
        json_user_requests = self._get_requests(start, end)
    
        # Collapse each request tree
        json_user_requests = self._collapse_requests(json_user_requests)
        
        return json_user_requests
                
                
    def set_requests_to_unschedulable(self, unschedulable_r_numbers):
        '''Update the state of all the unschedulable Requests in the DB in one go.'''
        try:
            self.requestdb_client.set_request_state('UNSCHEDULABLE', unschedulable_r_numbers)
        except ConnectionError as e:
            self.log.error("Problem setting Request states to UNSCHEDULABLE: %s" % str(e))
            self.send_metric('requestdb.connection_status', 1)
        except RequestDBError as e:
            msg = "Internal RequestDB error when setting UNSCHEDULABLE Request states: %s" % str(e)
            self.log.error(msg)
            self.send_metric('requestdb.connection_status', 2)

    
        return
    
    
    def set_user_requests_to_unschedulable(self, unschedulable_ur_numbers):
        '''Update the state of all the unschedulable User Requests in the DB in one go.'''
        try:
            self.requestdb_client.set_user_request_state('UNSCHEDULABLE', unschedulable_ur_numbers)
        except ConnectionError as e:
            self.log.error("Problem setting User Request states to UNSCHEDULABLE: %s" % str(e))
            self.send_metric('requestdb.connection_status', 1)
        except RequestDBError as e:
            msg = "Internal RequestDB error when setting UNSCHEDULABLE User Request states: %s" % str(e)
            self.log.error(msg)
            self.send_metric('requestdb.connection_status', 2)

        return

@timeit
@metric_timer('requestdb.get_requests', num_requests=lambda x: len(x))
def get_requests_from_db(url, telescope_class, sem_start, sem_end, debug=False):
    format = '%Y-%m-%d %H:%M:%S'

    search = SearchQuery()
    search.set_date(start=sem_start.strftime(format), end=sem_end.strftime(format))

    log.info("Asking DB (%s) for User Requests between %s and %s", url, sem_start, sem_end)
    sc = SchedulerClient(url)

    ur_list = sc.retrieve(search, debug=debug)

    return ur_list


def get_requests(url, telescope_class):

    rc = RetrievalClient(url)
    rc.set_location(telescope_class)

    json_req_str = rc.retrieve()
    requests     = json.loads(json_req_str)

    return requests