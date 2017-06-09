from adaptive_scheduler.utils import timeit, metric_timer, SendMetricMixin

import logging
import requests
import os
from datetime import datetime
from requests.exceptions import RequestException


class ValhallaConnectionError(Exception):
    pass


class ValhallaInterface(object, SendMetricMixin):
    
    def __init__(self, valhalla_url, debug=False):
        self.valhalla_url = valhalla_url
        self.debug = debug
        self.log = logging.getLogger(__name__)
        self.headers = {'Authorization': 'Token ' + os.getenv("API_TOKEN", '')}
        self.current_semester_details = None

    def get_proposals(self):
        ''' Returns all active proposals using the bulk proposals API of valhalla
        '''
        try:
            response = requests.get(self.valhalla_url + '/api/proposals/?active=True', headers=self.headers)
            response.raise_for_status()
            return response.json()['results']
        except (RequestException, ValueError) as e:
            raise ValhallaConnectionError("failed to retrieve bulk proposals: {}".format(repr(e)))

    def get_proposal_by_id(self, proposal_id):
        ''' Returns the proposal details for the proposal_id given from the valhalla proposal API
        '''
        try:
            response = requests.get(self.valhalla_url + '/api/proposals/' + proposal_id + '/', headers=self.headers)
            response.raise_for_status()
            return response.json()
        except (RequestException, ValueError) as e:
            raise ValhallaConnectionError("failed to retrieve proposal {}: {}".format(proposal_id, repr(e)))

    def get_semester_details(self, date=datetime.utcnow()):
        ''' Return the previously cached semester details unless date specified is not within that semesters range.
            Gets the semester from the semesters api in valhalla.
        '''
        if (
                not self.current_semester_details
                or self.current_semester_details['start'] > date
                or self.current_semester_details['end'] < date):
            try:
                response = requests.get(self.valhalla_url + '/api/semesters/' +
                                        '?semester_contains={}'.format(date.isoformat()), headers=self.headers)
                response.raise_for_status()
                self.current_semester_details = response.json()['results'][0]
                self.current_semester_details['start'] = datetime.strptime(self.current_semester_details['start'],
                                                                           '%Y-%m-%dT%H:%M:%SZ')
                self.current_semester_details['end'] = datetime.strptime(self.current_semester_details['end'],
                                                                           '%Y-%m-%dT%H:%M:%SZ')
            except (RequestException, ValueError) as e:
                raise ValhallaConnectionError("failed to retrieve semester info for date {}: {}".format(date, repr(e)))
        return self.current_semester_details

    @timeit
    @metric_timer('requestdb.is_dirty')
    def is_dirty(self):
        ''' Triggers valhalla to update request states from recent pond blocks, and report back if any states were updated
        '''
        try:
            response = requests.get(self.valhalla_url + '/api/isDirty/', headers=self.headers)
            response.raise_for_status()
            is_dirty = response.json()['isDirty']
        except (RequestException, ValueError) as e:
            raise ValhallaConnectionError("is_dirty check failed: {}".format(repr(e)))

        self.log.info("isDirty returned {}".format(is_dirty))
        return is_dirty

    @timeit
    @metric_timer('requestdb.get_requests', num_requests=lambda x: len(x))
    def get_all_user_requests(self, start, end):
        ''' Get all user requests waiting for scheduling between
            start and end date
        '''
        try:
            response = requests.get(self.valhalla_url + '/api/userrequests/schedulable_requests/?start=' + start.isoformat() + '&end=' + end.isoformat(), headers=self.headers)
            response.raise_for_status()
            user_requests = response.json()
        except (RequestException, ValueError) as e:
            raise ValhallaConnectionError("get_all_user_requests failed: {}".format(repr(e)))

        return user_requests
