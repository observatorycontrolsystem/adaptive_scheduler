from adaptive_scheduler.utils import timeit, metric_timer, SendMetricMixin
from reqdb.client import SearchQuery, SchedulerClient, ConnectionError, RequestDBError
from adaptive_scheduler.printing         import pluralise as pl
from adaptive_scheduler.interfaces import ScheduleException

from datetime import datetime
import json
import logging
import sys
import requests
from requests.exceptions import RequestException

log = logging.getLogger(__name__)


class RequestDBInterface(object, SendMetricMixin):
    
    def __init__(self, requestdb_url, debug=False):
        self.requestdb_url = requestdb_url
        self.debug = debug
        self.log = logging.getLogger(__name__)

    @timeit
    @metric_timer('requestdb.is_dirty')
    def is_dirty(self):
        '''Trigger valhalla to update request states from recent pond blocks, and report back if any states were updated
        '''
        try:
            response = requests.get(self.requestdb_url + '/api/isDirty/')
            response.raise_for_status()
            is_dirty = response.json()['isDirty']
        except (RequestException) as e:
            raise RequestDBError("is_dirty check failed: {}".format(repr(e)))

        return is_dirty

    @timeit
    @metric_timer('requestdb.get_requests', num_requests=lambda x: len(x))
    def get_all_user_requests(self, start, end):
        '''Get all user requests waiting for scheduling between
        start and end date
        '''
        try:
            response = requests.get(self.requestdb_url + '/api/userrequests/schedulable_requests/?start=' + start.isoformat() + '&end=' + end.isoformat())
            response.raise_for_status()
            user_requests = response.json()
        except (RequestException) as e:
            raise RequestDBError("get_all_user_requests failed: {}".format(repr(e)))

        return user_requests
