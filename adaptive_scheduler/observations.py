#!/usr/bin/env python
'''
observations.py - Facilitates getting, submitting, and cancelling observations from the Observation Portal.

This module provides the scheduler's interface for constructing Observations.

Author: Jon Nation
March 2019
'''
from __future__ import division
import os

from datetime import timedelta
from collections import defaultdict

from adaptive_scheduler.utils import (get_reservation_datetimes, timeit, split_location)

from adaptive_scheduler.printing import pluralise as pl
from adaptive_scheduler.log import RequestGroupLogger
from adaptive_scheduler.interfaces import RunningRequest, RunningRequestGroup, ScheduleException
from adaptive_scheduler.configdb_connections import ConfigDBError

# Set up and configure a module scope logger
import logging
from adaptive_scheduler.utils import metric_timer
from time_intervals.intervals import Intervals

import requests
from dateutil.parser import parse

log = logging.getLogger(__name__)

multi_rg_log = logging.getLogger('rg_logger')
rg_log = RequestGroupLogger(multi_rg_log)


class ObservationRunningRequest(RunningRequest):

    def __init__(self, telescope, request_id, observation_id, start, end):
        RunningRequest.__init__(self, telescope, request_id, start, end)
        self.observation_id = observation_id

    def __str__(self):
        return RunningRequest.__str__(self) + ", observation_id: {}".format(self.observation_id)


class ObservationScheduleInterface(object):

    def __init__(self, host=None):
        self.host = host
        self.headers = {'Authorization': 'Token ' + os.getenv("OBSERVATION_PORTAL_API_TOKEN", '')}
        self.running_observations_by_telescope = None
        self.running_intervals_by_telescope = None
        self.rr_intervals_by_telescope = None

        self.log = logging.getLogger(__name__)

    def fetch_data(self, telescopes, running_window_start, running_window_end):
        # Fetch the data
        self.running_observations_by_telescope = self._fetch_running_observations(telescopes, running_window_start,
                                                                                  running_window_end)
        self.running_intervals_by_telescope = get_network_running_intervals(self.running_observations_by_telescope)
        # TODO: Possible inefficency here.  Might be able to determine running RR intervals from running blocks wihtout another call
        self.rr_intervals_by_telescope = self._fetch_rr_intervals(telescopes, running_window_start, running_window_end)

    @metric_timer('observation_portal.get_running_observations', num_blocks=len)
    def _fetch_running_observations(self, telescopes, end_after, start_before):
        running_observations = self._get_network_running_observations(telescopes, end_after, start_before)
        # This is just logging held over from when this was in the scheduling loop
        all_running_observations = []
        for observations in running_observations.values():
            all_running_observations += observations
        for observation in all_running_observations:
            msg = "Request %d has a running observation (id=%d, finishing at %s)" % (
                observation['request']['id'],
                observation['id'],
                observation['end']
            )
            self.log.debug(msg)
        return running_observations

    @metric_timer('observation_portal.get_rr_intervals')
    def _fetch_rr_intervals(self, telescopes, end_after, start_before):
        rr_observations = self._get_rr_intervals_by_telescope(telescopes, end_after, start_before)

        return rr_observations

    def running_request_groups_by_id(self):
        running_rgs = {}
        for observations in self.running_observations_by_telescope.values():
            for observation in observations:
                request_group_id = int(observation['request_group_id'])
                running_rg = running_rgs.setdefault(request_group_id, RunningRequestGroup(request_group_id))
                telescope = observation['telescope'] + '.' + observation['enclosure'] + '.' + observation['site']
                request_id = observation['request']['id']
                running_request = ObservationRunningRequest(telescope, request_id, observation['id'],
                                                            observation['start'], observation['end'])
                if any([conf['state'] == 'FAILED' for conf in observation['request']['configurations']]):
                    running_request.add_error("Observation has failed configurations")
                running_rg.add_running_request(running_request)

        return running_rgs

    def rr_request_group_intervals_by_telescope(self):
        ''' Return the schedule RR intervals for the supplied telescope
        '''
        return self.rr_intervals_by_telescope

    @metric_timer('observation_portal.cancel_observations', num_requests=lambda x: x, rate=lambda x: x)
    def cancel(self, cancelation_date_list_by_resource, include_rr, include_normal):
        ''' Cancel the current scheduler between start and end
        '''
        n_deleted = 0
        if cancelation_date_list_by_resource:
            n_deleted += self._cancel_schedule(cancelation_date_list_by_resource, include_rr,
                                               include_normal)
        return n_deleted

    def abort(self, running_request):
        ''' Abort a running request
        '''
        observation_ids = [running_request.observation_id]
        return self._cancel_observations(observation_ids)

    @metric_timer('observation_portal.save_schedule', num_requests=lambda x: x, rate=lambda x: x)
    def save(self, schedule, semester_start, configdb_interface, dry_run=False):
        ''' Save the provided observing schedule
        '''
        # Convert the kernel schedule into observation_portal observations, and submit them
        n_submitted = self._send_schedule_to_observation_portal(schedule, semester_start,
                                                                configdb_interface, dry_run)
        return n_submitted

    # Already timed by the save method
    @timeit
    def _send_schedule_to_observation_portal(self, schedule, semester_start, configdb_interface, dry_run=False):
        ''' Convert a kernel schedule into Observation Portal observations and submit them '''

        # TODO: Update this code to send Observations and ConfigStatuses to observation portal
        observations_by_resource = defaultdict(list)
        for resource_name, reservations in schedule.items():
            for reservation in reservations:
                try:
                    observation = build_observation(reservation, semester_start, configdb_interface)
                except Exception:
                    log.exception('Unable to build observation from reservation for request number {}'.format(
                        reservation.request.id
                    ))
                    continue
                observations_by_resource[resource_name].append(observation)
            _, observation_str = pl(len(observations_by_resource[resource_name]), 'observation')
            msg = 'Will send {} {} to {}'.format(len(observations_by_resource[resource_name]), observation_str,
                                                 resource_name)
            log_info_dry_run(msg, dry_run)
        n_submitted_total = self._send_observations_to_observation_portal(observations_by_resource, dry_run)

        return n_submitted_total

    def _send_observations_to_observation_portal(self, observations_by_resource, dry_run=False):
        observations = [ob for obs in observations_by_resource.values() for ob in obs]
        num_created = len(observations)
        if not dry_run and num_created > 0:
            try:
                response = requests.post(self.host + '/api/observations/', json=observations, headers=self.headers,
                                         timeout=120)
                response.raise_for_status()
                num_created = response.json()['num_created']
                self._log_bad_observations(observations,
                                           response.json()['errors'] if 'errors' in response.json() else {})
            except Exception as e:
                log.error("_send_observations_to_observation_portal error: {}".format(repr(e)))

        return num_created

    def _log_bad_observations(self, observations, errors):
        for index, error in errors.items():
            bad_observation = observations[int(index)]
            log.warning('Failed to schedule observation for request {} due to reason {}'
                        .format(bad_observation['request'],
                                error))

    def _get_rr_observations_by_telescope(self, tels, ends_after, starts_before):
        telescope_observations = {}
        for full_tel_name in tels:
            tel_name, obs_name, site_name = full_tel_name.split('.')
            schedule = self._get_schedule(ends_after, starts_before, site_name, obs_name, tel_name, only_rr=True,
                                          states=['PENDING', 'IN_PROGRESS'])
            telescope_observations[full_tel_name] = schedule

        return telescope_observations

    @timeit
    def _get_rr_intervals_by_telescope(self, tels, ends_after, starts_before):
        '''
            Return a map of telescope name to intervals for all currently
            scheduled Rapid Response observations.
        '''
        telescope_interval = {}
        rr_observations_by_telescope = self._get_rr_observations_by_telescope(tels, ends_after, starts_before)

        for full_tel_name in tels:
            observations = rr_observations_by_telescope.get(full_tel_name, [])

            intervals = get_intervals(observations)
            if not intervals.is_empty():
                telescope_interval[full_tel_name] = intervals

        return telescope_interval

    @metric_timer('observation_portal.get_schedule')
    def _get_schedule(self, start, end, site, enc, tel, only_rr=False, states=None):
        if states is None:
            states = []
        # Only retrieve observations which are currently active
        params = dict(end_after=start, start_before=end, start_after=start - timedelta(days=1), site=site,
                      enclosure=enc, telescope=tel, limit=1000,
                      observation_type=['NORMAL', 'RAPID_RESPONSE', 'TIME_CRITICAL'],
                      state=states, offset=0)
        if only_rr:
            params['observation_type'] = 'RAPID_RESPONSE'

        base_url = self.host + '/api/schedule/'

        initial_results = self._get_schedule_helper(base_url, params)
        observations = initial_results['results']
        count = initial_results['count']
        total = len(observations)
        while total < count:
            params['offset'] += params['limit']
            results = self._get_schedule_helper(base_url, params)
            count = results['count']
            total += len(results['results'])
            observations.extend(results['results'])

        for block in observations:
            block['start'] = parse(block['start'], ignoretz=True)
            block['end'] = parse(block['end'], ignoretz=True)

        return observations

    def _get_schedule_helper(self, base_url, params):
        try:
            results = requests.get(base_url, params=params, headers=self.headers, timeout=120)
            results.raise_for_status()
            return results.json()
        except Exception as e:
            raise ScheduleException(e, "Unable to retrieve Schedule from Observation Portal: {}".format(repr(e)))

    @timeit
    def _cancel_schedule(self, cancelation_date_list_by_resource, include_rr, include_normal):
        total_num_canceled = 0
        for full_tel_name, cancel_dates in cancelation_date_list_by_resource.items():
            for (start, end) in cancel_dates:
                tel, enc, site = full_tel_name.split('.')
                log.info("Cancelling schedule at %s, from %s to %s", full_tel_name,
                         start, end)

                data = {
                    'start': start.isoformat(),
                    'end': end.isoformat(),
                    'site': site,
                    'enclosure': enc,
                    'telescope': tel,
                    'include_rr': include_rr,
                    'include_normal': include_normal
                }

                try:
                    results = requests.post(self.host + '/api/observations/cancel/', json=data, headers=self.headers,
                                            timeout=120)
                    results.raise_for_status()
                    num_canceled = int(results.json()['canceled'])
                    total_num_canceled += num_canceled
                    msg = 'Cancelled {} observations at {}'.format(num_canceled, full_tel_name)
                    log.info(msg)
                except Exception as e:
                    raise ScheduleException("Failed to cancel observations in Observation Portal: {}".format(repr(e)))

        return total_num_canceled

    def _cancel_observations(self, observation_ids):
        try:
            data = {'ids': observation_ids}
            results = requests.post(self.host + '/api/observations/cancel/', json=data, headers=self.headers,
                                    timeout=120)
            results.raise_for_status()
            num_canceled = results.json()['canceled']
        except Exception as e:
            raise ScheduleException("Failed to abort observations in Observation Portal: {}".format(repr(e)))

        return num_canceled

    def _get_network_running_observations(self, tels, ends_after, starts_before):
        n_running_total = 0
        running_at_tel = {}
        for full_tel_name in tels:
            tel_name, obs_name, site_name = full_tel_name.split('.')
            log.debug("Acquiring running observations and first availability at %s",
                      full_tel_name)

            running = self._get_running_observations(ends_after, starts_before,
                                                     site_name, obs_name, tel_name)

            running_at_tel[full_tel_name] = running

            n_running = len(running)
            _, observation_str = pl(n_running, 'observation')
            log.debug("Found %d running %s at %s", n_running, observation_str, full_tel_name)
            n_running_total += n_running

        _, observation_str = pl(n_running_total, 'observation')
        log.info("Network-wide, found %d running %s", n_running_total, observation_str)

        return running_at_tel

    def _get_running_observations(self, ends_after, starts_before, site, obs, tel):
        schedule = self._get_schedule(ends_after, starts_before, site, obs, tel,
                                      states=['PENDING', 'IN_PROGRESS', 'COMPLETED'])

        cutoff_dt = starts_before
        for observation in schedule:
            if observation['start'] < starts_before < observation['end']:
                if observation['end'] > cutoff_dt and observation['state'] in ['PENDING', 'IN_PROGRESS']:
                    cutoff_dt = observation['end']

        running = [b for b in schedule if b['start'] < cutoff_dt]

        return running


def log_info_dry_run(msg, dry_run):
    if dry_run:
        msg = "DRY-RUN: " + msg
    log.info(msg)


def resolve_instrument(instrument_type, site, obs, tel, configdb_interface):
    """Determine the specific camera name for a given site.

    If a non-generic name is provided, we just pass it through and assume it's ok.
    """
    try:
        specific_camera = configdb_interface.get_specific_instrument(instrument_type, site, obs, tel)
    except ConfigDBError:
        msg = "Couldn't find any instrument for '{}' at {}.{}.{}".format(instrument_type, tel, obs, site)
        raise InstrumentResolutionError(msg)

    return specific_camera


def resolve_autoguider(self_guide, instrument_name, site, enc, tel, configdb_interface):
    """Determine the specific autoguider for a given site.

    If a specific name is provided, pass through and return it.
    If a generic name (SCICAM-*) is provided, resolve for self-guiding.
    If nothing is specified, resolve to the preferred autoguider.
    """

    try:
        ag_match = configdb_interface.get_autoguider_for_instrument(instrument_name, self_guide)
    except ConfigDBError:
        msg = "Couldn't find any autoguider for '{}' at {}.{}.{}".format(instrument_name, tel, enc, site)
        raise InstrumentResolutionError(msg)

    return ag_match


def build_observation(reservation, semester_start, configdb_interface):
    request = reservation.request
    res_start, res_end = get_reservation_datetimes(reservation, semester_start)
    telescope, enclosure, site = split_location(reservation.scheduled_resource)

    configuration_statuses = []
    for configuration in request.configurations:
        specific_camera = resolve_instrument(configuration.instrument_type, site,
                                             enclosure, telescope, configdb_interface)
        configuration_status = {
            'instrument_name': specific_camera,
            'configuration': configuration.id
        }

        if configuration.guiding_config.get('mode', 'OFF') != 'OFF':
            self_guide = getattr(configuration.extra_params, 'self_guide', False)
            specific_ag = resolve_autoguider(self_guide, specific_camera,
                                             site, enclosure, telescope,
                                             configdb_interface)
            configuration_status['guide_camera_name'] = specific_ag
            msg = "Autoguider resolved as {}".format(specific_ag)
            log.debug(msg)
            rg_log.debug(msg, reservation.request_group.id)
        configuration_statuses.append(configuration_status)

    observation = {
        'site': site,
        'enclosure': enclosure,
        'telescope': telescope,
        'start': res_start.isoformat(),
        'end': res_end.isoformat(),
        'request': request.id,
        'configuration_statuses': configuration_statuses
    }

    return observation


@metric_timer('observation_portal.get_network_running_intervals')
def get_network_running_intervals(running_observations_by_telescope):
    running_at_tel = {}

    for telescope, observations in running_observations_by_telescope.items():
        running_at_tel[telescope] = get_intervals(observations)

    return running_at_tel


def get_intervals(observations):
    ''' Create Intervals from given observations '''
    intervals = []
    for observation in observations:
        intervals.append((observation['start'], observation['end']))

    return Intervals(intervals)


class InstrumentResolutionError(Exception):
    '''Raised when no instrument can be determined for a given resource.'''
    pass
