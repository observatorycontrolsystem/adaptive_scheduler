from adaptive_scheduler.utils import timeit, metric_timer, SendMetricMixin

import logging
import requests
import os
from datetime import datetime
from dateutil.parser import parse
from requests.exceptions import RequestException, Timeout


class ObservationPortalConnectionError(Exception):
    pass


class ObservationPortalInterface(SendMetricMixin):

    def __init__(self, obs_portal_url, debug=False):
        self.obs_portal_url = obs_portal_url
        self.debug = debug
        self.log = logging.getLogger(__name__)
        self.headers = {'Authorization': 'Token ' + os.getenv("OBSERVATION_PORTAL_API_TOKEN", '')}
        self.current_semester_details = None

    def get_proposals(self):
        ''' Returns all active proposals using the bulk proposals API of the observation portal
        '''
        try:
            response = requests.get(self.obs_portal_url + '/api/proposals/?active=True&limit=1000',
                                    headers=self.headers,
                                    timeout=120)
            response.raise_for_status()
            return response.json()['results']
        except (RequestException, ValueError, Timeout) as e:
            raise ObservationPortalConnectionError("failed to retrieve bulk proposals: {}".format(repr(e)))

    def get_proposal_by_id(self, proposal_id):
        ''' Returns the proposal details for the proposal_id given from the observation portal proposal API
        '''
        try:
            response = requests.get(self.obs_portal_url + '/api/proposals/' + proposal_id + '/', headers=self.headers,
                                    timeout=15)
            response.raise_for_status()
            return response.json()
        except (RequestException, ValueError, Timeout) as e:
            raise ObservationPortalConnectionError("failed to retrieve proposal {}: {}".format(proposal_id, repr(e)))

    def get_semester_details(self, date=datetime.utcnow()):
        ''' Return the previously cached semester details unless date specified is not within that semesters range.
            Gets the semester from the semesters api in the observation portal.
        '''
        if (
                not self.current_semester_details
                or self.current_semester_details['start'] > date
                or self.current_semester_details['end'] < date):
            try:
                response = requests.get(self.obs_portal_url + '/api/semesters/' +
                                        '?semester_contains={}'.format(date.isoformat()), headers=self.headers,
                                        timeout=15)
                response.raise_for_status()
                self.current_semester_details = response.json()['results'][0]
                self.current_semester_details['start'] = datetime.strptime(self.current_semester_details['start'],
                                                                           '%Y-%m-%dT%H:%M:%SZ')
                self.current_semester_details['end'] = datetime.strptime(self.current_semester_details['end'],
                                                                         '%Y-%m-%dT%H:%M:%SZ')
            except (RequestException, ValueError, Timeout) as e:
                raise ObservationPortalConnectionError(
                    "failed to retrieve semester info for date {}: {}".format(date, repr(e)))
        return self.current_semester_details

    @timeit
    @metric_timer('requestdb.get_last_changed')
    def get_last_changed(self):
        ''' Queries the observation portal for the time the last change was made to request or observation state
        :return: The datetime for the last change in observation portals models
        '''
        try:
            response = requests.get(self.obs_portal_url + '/api/last_changed/', headers=self.headers, timeout=180)
            response.raise_for_status()
            last_changed = parse(response.json()['last_change_time'])
            last_changed = last_changed.replace(tzinfo=None)
        except (RequestException, ValueError, Timeout) as e:
            raise ObservationPortalConnectionError("last_changed check failed: {}".format(repr(e)))

        self.log.info("last_changed time returned {}".format(last_changed.isoformat()))
        return last_changed

    @timeit
    @metric_timer('requestdb.get_requests', num_requests=lambda x: len(x))
    def get_all_request_groups(self, start, end):
        ''' Get all user requests waiting for scheduling between
            start and end date
        '''
        requests_url = self.obs_portal_url + '/api/requestgroups/schedulable_requests/?start=' + start.isoformat() + '&end=' + end.isoformat()
        self.log.info("Getting schedulable requests from: {}".format(requests_url))
        try:
            response = requests.get(requests_url, headers=self.headers, timeout=180)
            response.raise_for_status()
            request_groups = response.json()
        except (RequestException, ValueError, Timeout) as e:
            raise ObservationPortalConnectionError("get_all_request_groups failed: {}".format(repr(e)))

        return request_groups
