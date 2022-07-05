"""
Metric calculation functions for the scheduler simulator.
"""
import logging
import numpy as np
import datetime as dt
from datetime import datetime
from adaptive_scheduler.utils import time_in_capped_intervals
from adaptive_scheduler.models import DataContainer

log = logging.getLogger('adaptive_scheduler')


def combine_normal_and_rr_requests_by_rg_id(normal_scheduled_requests_by_rg_id,
                                            rr_scheduled_requests_by_rg_id):
    # this assumes that a request unique to either normal or rr and cannot be in both
    # otherwise, write a check that excludes duplicates
    return normal_scheduled_requests_by_rg_id | rr_scheduled_requests_by_rg_id


def total_scheduled_time(combined_scheduled_requests_by_rg_id):
    # Sums the duration of all scheduled requests
    # note, not sure if this is gonna be a timedelta or float object
    # looking at the existing code, it seems to be an integer for the duration in seconds
    total_scheduled_time = 0
    for request_group in combined_scheduled_requests_by_rg_id.values():
        for request in request_group.values():
            if request.scheduled:
                total_scheduled_time += request.duration
    return total_scheduled_time


def total_scheduled_count(combined_scheduled_requests_by_rg_id):
    counter = 0
    for request_group in combined_scheduled_requests_by_rg_id.values():
        for request in request_group.values():
            if request.scheduled:
                counter += 1
    return counter


def total_available_time(normal_scheduler_result, rr_scheduler_result, scheduler, horizon_days):
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
          

def total_unscheduled_count(combined_scheduled_requests_by_rg_id):
    counter = 0
    for request_group in combined_scheduled_requests_by_rg_id.values():
        for request in request_group.values():
            if request.scheduled:
                counter += 1
    return counter


def percent_of_requests_scheduled(combined_scheduled_requests_by_rg_id):
    scheduled_count = total_scheduled_count(combined_scheduled_requests_by_rg_id)
    unscheduled_count = total_unscheduled_count(combined_scheduled_requests_by_rg_id)
    return scheduled_count/(scheduled_count + unscheduled_count) * 100



def request_group_data_populator(reservation):
    # assumes the proposal/requestgroup is in the format from the observation portal API
    request_group = reservation.request_group
    proposal = request_group.proposal
    requests = request_group.requests
    # it may be helpful to directly set max_airmass as an attribute of a request itself
    max_airmass_by_request_id = {}
    for request in requests:
        request_id = request.id
        # assumes the airmass is the same for all configurations in a request
        # if not we can maybe aggregate with min/max or avg
        # again this assumes that configurations is a list of dicts matching the API
        configuration = request.configurations[0]
        max_airmass = configuration.constraints['max_airmass']
        max_airmass_by_request_id[request_id] = max_airmass
        
    data = DataContainer(
        request_group_id=reservation.request_group.id,
        duration=reservation.duration,
        scheduled_resource=reservation.scheduled_resource,
        scheduled=reservation.scheduled,
        scheduled_start=reservation.scheduled_start,
        ipp_value=reservation.request_group.ipp_value,
        tac_priority=proposal.tac_priority,
        requests=reservation.request_group.requests,
        max_airmass_by_request=max_airmass_by_request_id,
    )
    return data

# is this function name too long? or is the specificity necessary?
def populate_binned_data_dict_with_rg_data(data_dict, key, reservation):
    request_group_id = reservation.request_group.id
    if not key in data_dict:
        data_dict[key] = {}
    request_group_data = request_group_data_populator(reservation)
    data_dict[key][request_group_id] = request_group_data


def bin_scheduler_result_by_effective_priority(schedule):
    # this is somewhat structured differently to normal_scheduled_requests_by_rg_id
    # but we can change it to make it consistent if necessary
    scheduled_requests_by_priority = {}
    for reservations in schedule.values():
        for reservation in reservations:
            priority = str(reservation.priority)
            populate_binned_data_dict_with_rg_data(scheduled_requests_by_priority,
                                                   priority,
                                                   reservation)
    return scheduled_requests_by_priority


def bin_scheduler_result_by_tac_priority(schedule):
    scheduled_requests_by_tac_priority = {}
    for reservations in schedule.values():
        for reservation in reservations:
            proposal = reservation.request_group.proposal
            tac_priority = str(proposal.tac_priority)
            populate_binned_data_dict_with_rg_data(scheduled_requests_by_tac_priority,
                                                   tac_priority,
                                                   reservation)
    return scheduled_requests_by_tac_priority
                


def bin_scheduler_result_by_airmass(scheduler_result):
    # TODO
    # the airmasses are in a list which is kind of annoying
    scheduled_requests_by_airmass = {}


def cap_scheduler_results_by_effective_horizon(scheduler_result, horizon_length):
    # need to confirm the time format for scheduled_start before doing anything
    # but basically this function truncates the scheduler results to only include things
    # scheduled within a certain period of time and modifies the schedule accordingly
    for reservations in scheduler_result.values():
        for reservation in reservations:
            if reservation.scheduled_start: # is after the horizon
                reservations.remove(reservation)
    return scheduler_result


def calculate_best_airmass_vs_scheduled(scheduler_result):
    """Calculate the percent difference between the best possible airmass vs the average airmass 
    for each scheduled reservation.
    """
    best_airmass_vs_scheduled = []
    best_case = 1
    for reservation in scheduler_result.values():
        airmasses = np.mean(request_group_data_populator(reservation)["airmasses"])
        best_airmass_vs_scheduled.append((best_case - airmasses)/best_case *100)

    return best_airmass_vs_scheduled


def calculate_max_contraints_vs_scheduled(scheduler_result):
    """Calculate the percent difference between the airmass max constraints vs the average airmass 
    for each scheduled reservation.
    """
    airmass_constraints_vs_scheduled = []
    best_case = 1
    for reservation in scheduler_result.values():
        airmasses = np.mean(request_group_data_populator(reservation)["max_airmass_by_request"])
        airmass_constraints_vs_scheduled.append((best_case - airmasses)/best_case *100)

    return airmass_constraints_vs_scheduled
