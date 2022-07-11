"""
Metric calculation functions for the scheduler simulator.
"""
import datetime as dt
from datetime import datetime
from collections import defaultdict

import requests
from requests.exceptions import RequestException, Timeout
from rise_set.astrometry import calculate_airmass_at_times
from adaptive_scheduler.observation_portal_connections import ObservationPortalConnectionError
from adaptive_scheduler.utils import time_in_capped_intervals, normalised_epoch_to_datetime, datetime_to_epoch
from adaptive_scheduler.models import DataContainer


DEFAULT_EFFECTIVE_HORIZON_DAYS = 5


def percent_of(x, y):
    """Returns x/y as a percentage (float)."""
    return x / y * 100.


def percent_diff(x, y):
    """Returns the percent difference between x and y as a float."""
    if x == y == 0:
        return 0
    mean = (abs(x) + abs(y)) / 2
    return abs(x - y) / mean * 100.


class MetricCalculator():
    """A class encapsulating the metric calculating functions for the scheduler simulator.

    Args:
        normal_scheduler_result (SchedulerResult): The normal schedule output of the scheduler.
            The attribute of interest is SchedulerResult.schedule, which is a dictionary formatted
            as follows:
                {scheduled_resource, [reservations]}
        rr_scheduler_result (SchedulerResult): The rapid-response schedule output of the scheduler.
        scheduler (LCOGTNetworkScheduler): The instance of the scheduler used by the simulator.
        scheduler_runner (SchedulerRunner): The instance of the scheduler runner used by the simulator.
    """
    def __init__(self, normal_scheduler_result, rr_scheduler_result, scheduler, scheduler_runner):
        self.scheduler = scheduler
        self.scheduler_runner = scheduler_runner
        self.observation_portal_interface = self.scheduler_runner.network_interface.observation_portal_interface
        if self.scheduler_runner.sched_params.metric_effective_horizon:
            self.effective_horizon = self.scheduler_runner.sched_params.metric_effective_horizon
        else:
            self.effective_horizon = DEFAULT_EFFECTIVE_HORIZON_DAYS

        self.normal_scheduler_result = normal_scheduler_result
        self.normal_schedule = self.normal_scheduler_result.schedule
        if rr_scheduler_result:
            self.rr_scheduler_result = rr_scheduler_result
            self.rr_schedule = self.rr_scheduler_result.schedule
            self._combine_normal_rr_schedules()
            self._combine_resources_scheduled()
        else:
            self.combined_schedule = self.normal_schedule
            self.combined_resources_scheduled = self.normal_scheduler_result.resources_scheduled()

    def _combine_resources_scheduled(self):
        normal_resources = self.normal_scheduler_result.resources_scheduled()
        rr_resources = self.rr_scheduler_result.resources_scheduled()
        self.combined_resources_scheduled = list(set(normal_resources + rr_resources))

    def _combine_normal_rr_schedules(self):
        self.combined_schedule = defaultdict(list)
        for resource, reservations in self.rr_schedule.items():
            for reservation in reservations:
                self.combined_schedule[resource].append(reservation)
        for resource, reservations in self.normal_schedule.items():
            for reservation in reservations:
                if reservation not in self.combined_schedule[resource]:
                    self.combined_schedule[resource].append(reservation)

    def count_scheduled(self, schedule=None):
        schedule = self.combined_schedule if schedule is None else schedule
        counter = 0
        for reservations in schedule.values():
            for reservation in reservations:
                if reservation.scheduled:
                    counter += 1
        return counter

    def count_unscheduled(self, schedule=None):
        schedule = self.combined_schedule if schedule is None else schedule
        counter = 0
        for reservations in schedule.values():
            for reservation in reservations:
                if not reservation.scheduled:
                    counter += 1
        return counter

    def percent_reservations_scheduled(self, schedule=None):
        schedule = self.combined_schedule if schedule is None else schedule
        scheduled = self.count_scheduled(schedule)
        total = scheduled + self.count_unscheduled(schedule)
        return percent_of(scheduled, total)

    def total_scheduled_seconds(self, schedule=None):
        schedule = self.combined_schedule if schedule is None else schedule
        total_scheduled_seconds = 0
        for reservations in schedule.values():
            for reservation in reservations:
                total_scheduled_seconds += reservation.duration
        return total_scheduled_seconds

    def total_available_seconds(self, resources_scheduled=None, horizon_days=None):
        """Aggregates the total available time, calculated from dark intervals.

        Args:
            scheduled_resources (list): The list of sites scheduled, if nothing is passed then use the
                list generated when MetricCalculators is initialized.
            horizon_days (float): The number of days to cap, basically an effective horizon. If nothing
                is passed then use the value in sched_params.

        Returns:
            total_available_time (float): The dark intervals capped by the horizon.
        """
        resources_scheduled = self.combined_resources_scheduled if resources_scheduled is None else resources_scheduled
        horizon_days = self.effective_horizon if horizon_days is None else horizon_days
        total_available_time = 0
        start_time = self.scheduler.estimated_scheduler_end
        end_time = start_time + dt.timedelta(days=horizon_days)
        for resource in resources_scheduled:
            if resource in self.scheduler.visibility_cache:
                dark_intervals = self.scheduler.visibility_cache[resource].dark_intervals
                available_time = time_in_capped_intervals(dark_intervals, start_time, end_time)
                total_available_time += available_time
        return total_available_time

    def percent_time_utilization(self, schedule=None, resources_scheduled=None, horizon_days=None):
        schedule = self.combined_schedule if schedule is None else schedule
        resources_scheduled = self.combined_resources_scheduled if resources_scheduled is None else resources_scheduled
        horizon_days = self.effective_horizon if horizon_days is None else horizon_days
        return percent_of(self.total_scheduled_seconds(schedule),
                          self.total_available_seconds(resources_scheduled, horizon_days))
        
    def _get_airmass_data_from_observation_portal(self, request_id):
        """Pulls airmass data from the Observation Portal.

        Args:
            observation_portal_interface (ObservationPortalInterface): Instance of the Observation Portal
                used by the scheduler.
            request_id (str): The request id.

        Returns:
            airmass_data (dict): The airmass data returned from the API.
        """
        airmass_url = f'{self.observation_portal_interface.obs_portal_url}/api/requests/{request_id}/airmass/'
        try:
            response = requests.get(airmass_url, headers=self.observation_portal_interface.headers, timeout=180)
            response.raise_for_status()
            airmass_data = response.json()
        except (RequestException, ValueError, Timeout) as e:
            raise ObservationPortalConnectionError("get_airmass_data failed: {}".format(repr(e)))

        return airmass_data
    
    def _get_ideal_airmass_for_request(self, request_id):
        """Finds the minimum airmass across all sites for the request."""
        ideal_airmass = 1000
        airmass_data = self._get_airmass_data_from_observation_portal(request_id)
        for site in airmass_data['airmass_data'].values():
            ideal_for_site = min(site['airmasses'])
            ideal_airmass = min(ideal_airmass, ideal_for_site)
        return ideal_airmass

    def avg_ideal_airmass(self, schedule=None):
        """Calculates the average ideal airmass for scheduled observations."""
        schedule = self.combined_schedule if schedule is None else schedule
        sum_ideal_airmass = 0
        count = 0
        for reservations in schedule.values():
            for reservation in reservations:
                if reservation.scheduled:
                    request_id = reservation.request.id
                    sum_ideal_airmass += self._get_ideal_airmass_for_request(request_id)
                    count += 1
        return sum_ideal_airmass / count

    def _get_midpoint_airmasses_from_request(self, request_id, start_time, end_time):
        midpoint_airmasses = {}
        midpoint_time = start_time + (end_time - start_time) / 2
        airmass_data = self._get_airmass_data_from_observation_portal(request_id)['airmass_data']
        for site, details in airmass_data.items():
            times, airmasses = list(details.values())[0], list(details.values())[1]
            index = 0
            time_diff = abs((midpoint_time - datetime.strptime(times[0], '%Y-%m-%dT%H:%M')).total_seconds())

            for i,_ in enumerate(times):
                temp_time_diff = abs((midpoint_time - datetime.strptime(times[i], '%Y-%m-%dT%H:%M')).total_seconds())
                if temp_time_diff < time_diff:
                    time_diff = temp_time_diff
                    index = i
            midpoint_airmass = airmasses[index]
            midpoint_airmasses[site] = midpoint_airmass
        return midpoint_airmasses

    def avg_midpoint_airmass(self, schedule=None):
        schedule = self.combined_schedule if schedule is None else schedule
        semester_start = self.scheduler_runner.semester_details['start']
        midpoint_airmass_for_each_reservation = []
        sum_midpoint_airmass = 0
        count = 0
        for reservations in schedule.values():
            for reservation in reservations:
                if reservation.scheduled:
                    request = reservation.request
                    request_id = request.id
                    start_time = normalised_epoch_to_datetime(reservation.scheduled_start,
                                                            datetime_to_epoch(semester_start))
                    end_time = start_time + dt.timedelta(seconds=reservation.duration)
                    midpoint_airmasses = self._get_midpoint_airmasses_from_request(request_id, start_time, end_time)
                    site = reservation.scheduled_resource[-3:]
                    midpoint_airmass = midpoint_airmasses[site]
                    midpoint_airmass_for_each_reservation.append(midpoint_airmass)
                    sum_midpoint_airmass += midpoint_airmass
                    count += 1
        return sum_midpoint_airmass / count


def reservation_data_populator(reservation):
    """Creates a new data container containing parameters useful in calculating metrics.

    Args:
        reservation (Reservation_v3): A Reservation object (obtained from the values of Scheduler.schedule).

    Returns:
        data (DataContainer): An object with data values of interest as attributes.
    """
    request_group = reservation.request_group
    proposal = request_group.proposal

    data = DataContainer(
        request_group_id=reservation.request_group.id,
        request_id=reservation.request.id,
        duration=reservation.duration,
        scheduled_resource=reservation.scheduled_resource,
        scheduled=reservation.scheduled,
        scheduled_start=reservation.scheduled_start,
        ipp_value=reservation.request_group.ipp_value,
        tac_priority=proposal.tac_priority,
    )
    return data


def fill_bin_with_reservation_data(data_dict, bin_name, reservation):
    """Populates bins in a dictionary with the reservation data container. The original
    dictionary is modified, instead of creating and returning a copy.

    Args:
        data_dict (dict): Binned data dictionary. Each bin contains a list of DataContainer's.
        bin_name (str): The name of the bin to create or populate.
        reservation (Reservation_v3): A Reservation object.
    """
    if bin_name not in data_dict:
        data_dict[bin_name] = []
    reservation_data = reservobservation_portal_interface, _reservation_data(scheduled_requests_by_eff_priority,
                                               eff_priority,
                                               reservation)
    return scheduled_requests_by_eff_priority


def bin_scheduler_result_by_tac_priority(schedule):
    scheduled_requests_by_tac_priority = {}
    for reservations in schedule.values():
        for reservation in reservations:
            if reservation.scheduled:
                proposal = reservation.request_group.proposal
                tac_priority = str(proposal.tac_priority)
                fill_bin_with_reservation_data(scheduled_requests_by_tac_priority,
                                               tac_priority,
                                               reservation)
    return scheduled_requests_by_tac_priority
