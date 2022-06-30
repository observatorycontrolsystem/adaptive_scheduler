"""
Metric calculation functions for the scheduler simulator.
"""

from adaptive_scheduler.models import DataContainer


def combine_normal_and_rr_requests_by_rg_id(normal_scheduled_requests_by_rg_id,
                                            rr_scheduled_requests_by_rg_id):
    # this assumes that a request unique to either normal or rr and cannot be in both
    # otherwise, write a check that excludes duplicates
    return normal_scheduled_requests_by_rg_id.update(rr_scheduled_requests_by_rg_id)


def total_scheduled_time(combined_scheduled_requests_by_rg_id):
    # Sums the duration of all scheduled requests
    # note, not sure if this is gonna be a timedelta or float object
    # looking at the existing code, it seems to be an integer for the duration in seconds
    total_scheduled_time = 0
    for request_group in combined_scheduled_requests_by_rg_id.values():
        for request in request_group.values():
            if request.scheduled:
                total_time += request.duration
    return total_scheduled_time


def total_scheduled_count(combined_scheduled_requests_by_rg_id):
    total_scheduled_count = 0
    for request_group in combined_scheduled_requests_by_rg_id.values():
        for request in request_group.values():
            if request.scheduled:
                total_scheduled_count += 1
    return total_scheduled_count


def total_unscheduled_count(combined_scheduled_requests_by_rg_id):
    total_unscheduled_count = 0
    for request_group in combined_scheduled_requests_by_rg_id.values():
        for request in request_group.values():
            if request.scheduled:
                total_unscheduled_count += 1
    return total_scheduled_count


def percent_of_requests_scheduled(combined_scheduled_requests_by_rg_id):
    scheduled = total_scheduled_count(combined_scheduled_requests_by_rg_id)
    unscheduled = total_unscheduled_count(combined_scheduled_requests_by_rg_id)
    return scheduled/(scheduled + unscheduled) * 100


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
        # again this assumes that configurations is a list of dicts matching the API
        # if not we can maybe aggregate with min/max or avg
        configuration = request.configurations[0]
        max_airmass = configuration['constraints']['max_airmass']
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


def bin_scheduler_result_by_effective_priority(scheduler_result):
    # this is somewhat structured differently to normal_scheduled_requests_by_rg_id
    # but we can change it to make it consistent if necessary
    scheduled_requests_by_priority = {}
    for reservations in scheduler_result.values():
        for reservation in reservations:
            priority = str(reservation.priority)
            populate_binned_data_dict_with_rg_data(scheduled_requests_by_priority,
                                                   priority,
                                                   reservation)
    return scheduled_requests_by_priority


def bin_scheduler_result_by_airmass(scheduler_result):
    # TODO
    # the airmasses are in a list which is kind of annoying
    scheduled_requests_by_airmass = {}

