"""
Metric calculation functions for the scheduler simulator.
"""
import logging
import datetime as dt
from datetime import datetime

import requests
import numpy as np
from requests.exceptions import RequestException, Timeout

from adaptive_scheduler.observation_portal_connections import ObservationPortalConnectionError
from adaptive_scheduler.utils import time_in_capped_intervals, normalised_epoch_to_datetime, datetime_to_epoch, merge_dicts
from adaptive_scheduler.models import DataContainer
from rise_set.astrometry import calculate_airmass_at_times


def percent_of(x, y):
    """Returns x/y as a percentage (float)."""
    return x/y*100.

def percent_diff(x, y):
    """Returns the percent difference between x and y as a float."""
    if x == y == 0:
        return 0
    mean = (abs(x)+abs(y))/2
    return abs(x-y)/mean*100.


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
        self.normal_scheduler_result = normal_scheduler_result
        # THERE IS NOT ALWAYS A RR RESULT - ADD CHECKS FOR THIS
        self.rr_scheduler_result = rr_scheduler_result
        self.scheduler = scheduler
        self.scheduler_runner = scheduler_runner

        self.normal_schedule = self.normal_scheduler_result.schedule
        self.rr_schedule = self.rr_scheduler_result.schedule
        self.combined_schedule = self._combine_normal_rr_schedules()

    def _combine_normal_rr_schedules(self):
        self.combined_schedule = self.normal_schedule.copy()
        for resource, reservations in self.rr_schedule.items():
            for reservation in reservations:
                self.combined_schedule[resource].append(reservation)

                
def count_scheduled(schedule):
    counter = 0
    for reservations in schedule.values():
        for reservation in reservations:
            if reservation.scheduled:
                counter += 1
    return counter

def count_unscheduled(schedule):
    counter = 0
    for reservations in schedule.values():
        for reservation in reservations:
            if not reservation.scheduled:
                counter += 1
    return counter

def percent_reservations_scheduled(schedule):
    total = count_scheduled(schedule) + count_unscheduled(schedule)
    return percent_of(count_scheduled(schedule), total)


def total_scheduled_seconds(schedule):
    total_scheduled_seconds = 0
    for reservations in schedule.values():
        for reservation in reservations:
            total_scheduled_seconds += reservation.duration
    return total_scheduled_seconds        
        
def total_available_seconds(normal_scheduler_result, rr_scheduler_result, scheduler, horizon_days):
    """Aggregates the total available time, calculated from dark intervals.

    Args:
        normal_scheduler_result (SchedulerResult): The normal scheduler result.
        rr_scheduler_result (SchedulerResult): The rapid response scheduler result.
        scheduler (LCOGTNetworkScheduler): The scheduler object used by the scheduler runner.
        horizon_days (float): The length of the horizon in days to calculate the metric.

    Returns:
        total_available_time (float): The dark intervals capped by the horizon.
    """
    total_available_time = 0
    normal_resources = normal_scheduler_result.resources_scheduled()
    rr_resources = rr_scheduler_result.resources_scheduled()
    scheduled_resources = list(set(normal_resources + rr_resources))
    start_time = scheduler.estimated_scheduler_end
    end_time = start_time + dt.timedelta(days=horizon_days)
    for resource in scheduled_resources:
        if resource in scheduler.visibility_cache:
            dark_intervals = scheduler.visibility_cache[resource].dark_intervals
            available_time = time_in_capped_intervals(dark_intervals, start_time, end_time)
        total_available_time += available_time
    return total_available_time


def reservation_data_populator(reservation):
    """Creates a new data container containing parameters useful in calculating metrics.

    Args:
        reservation (Reservation_v3): A Reservation object (obtained from the values of Scheduler.schedule).

    Returns:
        data (DataContainer): An object with data values of interest as attributes.
    """
    request_group = reservation.request_group
    proposal = request_group.proposal
    requests = request_group.requests
        
    data = DataContainer(
        request_group_id=reservation.request_group.id,
        duration=reservation.duration,
        scheduled_resource=reservation.scheduled_resource,
        scheduled=reservation.scheduled,
        scheduled_start=reservation.scheduled_start,
        ipp_value=reservation.request_group.ipp_value,
        tac_priority=proposal.tac_priority,
        requests=reservation.request_group.requests,
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
    if not bin_name in data_dict:
        data_dict[bin_name] = []
    reservation_data = reservation_data_populator(reservation)
    data_dict[bin_name].append(reservation_data)


def bin_scheduler_result_by_eff_priority(schedule):
    scheduled_requests_by_eff_priority = {}
    for reservations in schedule.values():
        for reservation in reservations:
            if reservation.scheduled:
                eff_priority = str(reservation.priority)
                fill_bin_with_reservation_data(scheduled_requests_by_eff_priority,
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
                

def get_airmass_data_from_observation_portal(observation_portal_interface, request_id):
    """Pulls airmass data from the Observation Portal.

    Args:
        observation_portal_interface (ObservationPortalInterface): Instance of the Observation Portal
            used by the scheduler.
        request_id (str): The request id.

    Returns:
        airmass_data (dict): The airmass data returned from the API.
    """
    airmass_url = f'{observation_portal_interface.obs_portal_url}/api/requests/{request_id}/airmass'
    try:
        response = requests.get(airmass_url, headers=observation_portal_interface.headers, timeout=180)
        response.raise_for_status()
        airmass_data = response.json()
    except (RequestException, ValueError, Timeout) as e:
        raise ObservationPortalConnectionError("get_airmass_data failed: {}".format(repr(e)))

    return airmass_data


def get_ideal_airmass_for_request(observation_portal_interface, request_id):
    """Finds the minimum airmass across all sites for the request."""
    ideal_airmass = 1000
    airmass_data = get_airmass_data_from_observation_portal(
        observation_portal_interface, request_id)
    for site in airmass_data['airmass_data'].values():
        ideal_for_site = min(site['airmasses'])
        ideal_airmass = min(ideal_airmass, ideal_for_site)
    return ideal_airmass
    

def avg_ideal_airmass(observation_portal_interface, schedule):
    """Calculates the average ideal airmass for scheduled observations."""
    sum_ideal_airmass = 0
    count = 0
    for reservations in schedule.values():
        for reservation in reservations:
            if reservation.scheduled:
                for request in reservation.request_group.requests:
                    request_id = request.id
                    sum_ideal_airmass += get_ideal_airmass_for_request(
                        observation_portal_interface, request_id)
                    count += 1
    return sum_ideal_airmass / count


def calculate_midpoint_airmass(scheduled_requests_by_rg_id):
    # midpoint_airmass = 1.5
    midpoint_airmass_each_request = {}
    for request_group in scheduled_requests_by_rg_id.values():
        for request in request_group.values():
            if request.scheduled:
                start_time = request.start()
                end_time = request.end()
                midpoint_time = [start_time + (end_time - start_time)/2]
                target = request.get_target()
                observation_sites = request.get_site()
                midpoint_airmass_each_request[request] = {}
                for site in observation_sites:
                    obs_latitude =  site['latitdue']
                    obs_longitude =  site['longitude']
                    obs_height = site['elevation']
                    midpoint_airmass = calculate_airmass_at_times(midpoint_time, target, obs_latitude, obs_longitude, obs_height)
                    midpoint_airmass_each_request[request][site] = midpoint_airmass
    return midpoint_airmass_each_request
    
    
def get_midpoint_airmasses_from_request(observation_portal_interface, request_id, start_time, end_time):
    midpoint_airmasses = {}
    midpoint_time = start_time + (end_time - start_time)/2
    airmass_data = get_airmass_data_from_observation_portal(
        observation_portal_interface, request_id)['airmass_data']
    for site, details in airmass_data.items():
        times, airmasses = list(details.values())[0], list(details.values())[1]
        index = 0
        time_diff = midpoint_time -datetime.strptime(times[0],'%Y-%m-%dT%H:%M')
        for i in range(len(times)):
            temp_time_diff = midpoint_time - datetime.strptime(times[i],'%Y-%m-%dT%H:%M')
            if temp_time_diff < time_diff:
                time_diff = temp_time_diff
                index = i 
        midpoint_airmass = airmasses[index]
        midpoint_airmasses[site] = midpoint_airmass
    return midpoint_airmasses


def get_midpoint_airmass_for_each_reservation(observation_portal_interface, schedule, semester_start):
    midpoint_airmass_for_each_reservation = []
    for reservations in schedule.values():
        for reservation in reservations:
            if reservation.scheduled:
                for request in reservation.request_group.requests:
                    request_id = request.id
                    start_time = normalised_epoch_to_datetime(reservation.scheduled_start, datetime_to_epoch(semester_start))
                    end_time = start_time + dt.timedelta(seconds = reservation.duration)
                    midpoint_airmasses = get_midpoint_airmasses_from_request(
                                        observation_portal_interface, request_id,
                                        start_time, end_time)
                    site = reservation.scheduled_resource[-3:]
                    midpoint_airmass = midpoint_airmasses[site]
                midpoint_airmass_for_each_reservation.append(midpoint_airmass)
    return midpoint_airmass_for_each_reservation


def midpoint_airmass_vs_priority(observation_portal_interface, schedule, semester_start):
    midpoint_airmass_vs_priority={}
    midpoint_airmass_for_each_reservation = get_midpoint_airmass_for_each_reservation(observation_portal_interface, schedule, semester_start)
    eff_priorities = []
    for reservations in schedule.values():
        for reservation in reservations:
            if reservation.scheduled:
                eff_priority = reservation.priority
                eff_priorities.append(eff_priority)
    midpoint_airmass_vs_priority['midpoint_airmass']= midpoint_airmass_for_each_reservation
    midpoint_airmass_vs_priority['eff_priorities'] = eff_priorities
    return midpoint_airmass_vs_priority

    
    
