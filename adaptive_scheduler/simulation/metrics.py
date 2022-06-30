"""
Metric calculation functions for the scheduler simulator.
"""


def combine_normal_and_rr_requests_by_rg_id(normal_scheduled_requests_by_rg_id,
                                            rr_scheduled_requests_by_rg_id):
    # this assumes that a request unique to either normal or rr and cannot be in both
    # otherwise, write a check that excludes duplicates
    return normal_scheduled_requests_by_rg_id.update(rr_scheduled_requests_by_rg_id)


def total_scheduled_time(combined_scheduled_requests_by_rg_id):
    # Sums the duration of all scheduled requests
    # note, not sure if this is gonna be a timedelta or float object
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
