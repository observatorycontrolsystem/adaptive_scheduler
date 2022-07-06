"""
Metric calculation functions for the scheduler simulator.
"""
import logging
from turtle import st
import numpy as np
import datetime as dt
from datetime import datetime

import requests
from adaptive_scheduler.utils import time_in_capped_intervals
from adaptive_scheduler.models import DataContainer
from rise_set.astrometry import calculate_airmass_at_times

log = logging.getLogger('adaptive_scheduler')


def combine_normal_and_rr_requests_by_rg_id(normal_scheduled_requests_by_rg_id,
                                            rr_scheduled_requests_by_rg_id):
    """Combines normal and scheduled request results for aggregation.

    Args:
        normal_scheduled_requests_by_rg_id (dict): This is the output of
            SchedulerResult.get_scheduled_requests_by_request_group_id()
            which is a dictionary formatted as follows:
            {rg_id1: {request1: request1_data, request2: request2_data},
             rg_id2: ...}
        rr_scheduled_requests_by_rg_id (dict): The same format of results but for
            rapid response scheduler results.

    Returns:
        combined_scheduled_requests_by_rg_id (dict): Merged dictionaries with duplicate
            keys being excluded (OR).
    """
    return normal_scheduled_requests_by_rg_id | rr_scheduled_requests_by_rg_id


def total_scheduled_time(scheduled_requests_by_rg_id):
    """Aggregates the total scheduled time.

    Args:
        scheduled_requests_by_rg_id (dict): SchedulerResult.get_scheduled_requests_by_request_group_id() format.

    Returns:
        total_scheduled_time (int): The total scheduled time in seconds.
    """
    total_scheduled_time = 0
    for request_group in scheduled_requests_by_rg_id.values():
        for request in request_group.values():
            if request.scheduled:
                total_scheduled_time += request.duration
    return total_scheduled_time


def total_scheduled_count(scheduled_requests_by_rg_id):
    """Counts the number of scheduled requests."""
    counter = 0
    for request_group in scheduled_requests_by_rg_id.values():
        for request in request_group.values():
            if request.scheduled:
                counter += 1
    return counter
   

def total_unscheduled_count(scheduled_requests_by_rg_id):
    """Counts the number of unscheduled requests."""
    counter = 0
    for request_group in scheduled_requests_by_rg_id.values():
        for request in request_group.values():
            if not request.scheduled:
                counter += 1
    return counter


def percent_of_requests_scheduled(combined_scheduled_requests_by_rg_id):
    """Simple percentage scheduled calculation."""
    scheduled_count = total_scheduled_count(combined_scheduled_requests_by_rg_id)
    unscheduled_count = total_unscheduled_count(combined_scheduled_requests_by_rg_id)
    return scheduled_count/(scheduled_count + unscheduled_count) * 100


def total_available_time(normal_scheduler_result, rr_scheduler_result, scheduler, horizon_days):
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
    # it may be helpful to directly set max_airmass as an attribute of a request itself
    request_id_configurations = {request.id: request.configurations
                                 for request in requests}
        
    data = DataContainer(
        request_group_id=reservation.request_group.id,
        duration=reservation.duration,
        scheduled_resource=reservation.scheduled_resource,
        scheduled=reservation.scheduled,
        scheduled_start=reservation.scheduled_start,
        ipp_value=reservation.request_group.ipp_value,
        tac_priority=proposal.tac_priority,
        requests=reservation.request_group.requests,
        configurations_by_request_id=request_id_configurations,
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
            eff_priority = str(reservation.priority)
            fill_bin_with_reservation_data(scheduled_requests_by_eff_priority,
                                           eff_priority,
                                           reservation)
    return scheduled_requests_by_eff_priority


def bin_scheduler_result_by_tac_priority(schedule):
    scheduled_requests_by_tac_priority = {}
    for reservations in schedule.values():
        for reservation in reservations:
            proposal = reservation.request_group.proposal
            tac_priority = str(proposal.tac_priority)
            fill_bin_with_reservation_data(scheduled_requests_by_tac_priority,
                                           tac_priority,
                                           reservation)
    return scheduled_requests_by_tac_priority
                


def bin_scheduler_result_by_airmass_constr(schedule):
    # TODO
    # the airmasses are in a list which is kind of annoying
    scheduled_requests_by_airmass_constr = {}
    for reservations in schedule.values():
        for reservation in reservations:
            pass


# def calculate_best_airmass_vs_scheduled(normal_scheduler_result, rr_scheduler_result):
#     """Calculate the percent difference between the best possible airmass vs the average airmass 
#     for each scheduled reservation.
#     """
#     normal_resources = normal_scheduler_result.resources_scheduled()
#     rr_resources = rr_scheduler_result.resources_scheduled()
#     scheduled_resources = list(set(normal_resources + rr_resources))
#     best_airmass_vs_scheduled = []
#     best_case = 1
#     for reservation in scheduled_resources.values():
#         airmasses = np.mean(reservation_data_populator(reservation)["airmasses"])
#         best_airmass_vs_scheduled.append((best_case - airmasses)/best_case *100)

#     return best_airmass_vs_scheduled


# def calculate_max_contraints_vs_scheduled(normal_scheduler_result, rr_scheduler_result):
#     """Calculate the percent difference between the airmass max constraints vs the average airmass 
#     for each scheduled reservation.
#     """
#     normal_resources = normal_scheduler_result.resources_scheduled()
#     rr_resources = rr_scheduler_result.resources_scheduled()
#     scheduled_resources = list(set(normal_resources + rr_resources))
#     airmass_constraints_vs_scheduled = []
#     best_case = 1
#     for reservation in scheduled_resources.values():
#         airmasses = np.mean(reservation_data_populator(reservation)["max_airmass_by_request"])
#         airmass_constraints_vs_scheduled.append((best_case - airmasses)/best_case *100)

#     return airmass_constraints_vs_scheduled


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
    midpoint_time = [start_time + (end_time - start_time)/2]
    airmass_data = get_airmass_data_from_observation_portal(
        observation_portal_interface, request_id)['airmass_data']
    for site in airmass_data:
        for times, airmasses in site.items():
            target_time = times[0]
            index = 0
            time_diff = dt.timedelta(midpoint_time -times[0])
            for i in range(len(times)):
                temp_time_diff = dt.timedelta(midpoint_time - times[i])
                if temp_time_diff < time_diff:
                    time_diff = temp_time_diff
                    index = i 
            midpoint_airmass = airmasses[index]
        midpoint_airmasses[site.key()] = midpoint_airmass
    return midpoint_airmasses


def get_midpoint_airmass_for_scheduler(observvation_portal_interface, scheduler):
    