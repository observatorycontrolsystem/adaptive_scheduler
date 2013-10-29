#!/usr/bin/env python
from __future__ import division
'''
request_filters.py - Filtering of Requests for schedulability.

A) Window filters
-----------------
     Semester start      Now               Semester end (or expiry)
           |    _____     |                      |
1)         |   |     |    |                      |
           |   |_____|    |                      |
           |           ___|__                    |
2)         |          |   |  |                   |
           |          |___|__|                   |
           |              |       _____          |
3)         |              |      |     |         |
           |              |      |_____|         |
           |              |                   ___|__
4)         |              |                  |   |  |
           |              |                  |___|__|
           |              |                      |     _____
5)         |              |                      |    |     |
           |              |                      |    |_____|
           |              |                      |

6)  <--------->
      _____
     |     |
     |_____|

Window filters 1,2,4,5 and 6 are implemented. Filters 2 and 4 truncate the window
at the scheduling boundaries. Running the first four filters results in a set of
URs whose remaining windows are guaranteed to fall entirely within the scheduling
horizon.

Filter 6 throws away any windows too small to fit the requested observations,
after accounting for overheads. Order matters here; this filter should be called
*after* the truncation filters (2 and 4).

B) User Request filters
-----------------------
The remaining filters operate on URs themselves:
    * 7) expired URs are filtered out
    * 8) URs which cannot be completed (no child Requests with appropriate Windows
         remain) are filtered out

The convenience method run_all_filters() executes all the filters in the correct
order.

Authors: Eric Saunders, Martin Norbury
February 2013
'''

from datetime import datetime, timedelta
from adaptive_scheduler.printing import pluralise as pl
from adaptive_scheduler.log      import UserRequestLogger
from reqdb.client                import ConnectionError, RequestDBError


import logging
log = logging.getLogger(__name__)

multi_ur_log = logging.getLogger('ur_logger')
ur_log = UserRequestLogger(multi_ur_log)

# Comparator for all filters
now = datetime.utcnow()


def log_windows(fn):
    def wrap(ur_list, *args, **kwargs):

        n_windows_before = sum([ur.n_windows() for ur in ur_list])
        ur_list          = fn(ur_list, *args, **kwargs)
        n_windows_after  = sum([ur.n_windows() for ur in ur_list])

        log.info("%s: windows in (%d); windows out (%d)", fn.__name__, n_windows_before,
                                                                        n_windows_after)
        return ur_list

    return wrap


def log_urs(fn):
    def wrap(ur_list, *args, **kwargs):
        in_size  = len(ur_list)
        ur_list  = fn(ur_list, *args, **kwargs)
        out_size = len(ur_list)

        log.info("%s: URs in (%d); URs out (%d)", fn.__name__, in_size, out_size)

        return ur_list

    return wrap


def set_rs_to_unschedulable(client, unschedulable_r_numbers):
    '''Update the state of all the unschedulable Requests in the DB in one go.'''
    try:
        client.set_request_state('UNSCHEDULABLE', unschedulable_r_numbers)
    except ConnectionError as e:
        log.error("Problem setting Request states to UNSCHEDULABLE: %s" % str(e))
    except RequestDBError as e:
        msg = "Internal RequestDB error when setting UNSCHEDULABLE Request states: %s" % str(e)
        log.error(msg)

    return


def set_urs_to_unschedulable(client, unschedulable_ur_numbers):
    '''Update the state of all the unschedulable User Requests in the DB in one go.'''
    try:
        client.set_user_request_state('UNSCHEDULABLE', unschedulable_ur_numbers)
    except ConnectionError as e:
        log.error("Problem setting User Request states to UNSCHEDULABLE: %s" % str(e))
    except RequestDBError as e:
        msg = "Internal RequestDB error when setting UNSCHEDULABLE User Request states: %s" % str(e)
        log.error(msg)

    return


def filter_urs(ur_list):
    initial_urs       = set(ur_list)
    schedulable_urs   = set(run_all_filters(ur_list))
    unschedulable_urs = initial_urs - schedulable_urs

    return schedulable_urs, unschedulable_urs


def find_unschedulable_ur_numbers(unschedulable_urs):
    unschedulable_ur_numbers = [ur.tracking_number for ur in unschedulable_urs]

    return unschedulable_ur_numbers


def filter_and_set_unschedulable_urs(client, ur_list, user_now, dry_run=False):
    global now
    now = user_now

    schedulable_urs, unschedulable_urs = filter_urs(ur_list)

    log.info("Found %d unschedulable %s after filtering", *pl(len(unschedulable_urs), 'UR'))

    # Find the child Request numbers of ok URs which need to be marked UNSCHEDULABLE
    # For example, a MANY where the first child has no window any more
    dropped_r_numbers = drop_empty_requests(schedulable_urs)

    if dry_run:
        log.info("Dry-run: Not updating any Request DB states")
    else:
        log.info("Updating Request DB states")

    # Find the tracking numbers of the URs which need to be marked UNSCHEDULABLE
    unschedulable_ur_numbers = find_unschedulable_ur_numbers(unschedulable_urs)

    # Set the states of the Requests and User Requests
    set_rs_to_unschedulable(client, dropped_r_numbers)
    set_urs_to_unschedulable(client, unschedulable_ur_numbers)

    return schedulable_urs


@log_urs
def run_all_filters(ur_list):
    '''Execute all the filters, in the correct order. Windows may be discarded or
       truncated during this process. Unschedulable User Requests are discarded.'''
    ur_list = filter_on_pending(ur_list)
    ur_list = filter_on_expiry(ur_list)
    ur_list = filter_out_past_windows(ur_list)
    ur_list = truncate_lower_crossing_windows(ur_list)
    ur_list = truncate_upper_crossing_windows(ur_list)
    ur_list = filter_out_future_windows(ur_list)
    ur_list = filter_on_duration(ur_list)
    ur_list = filter_on_type(ur_list)

    return ur_list


# A) Window Filters
#------------------
@log_windows
def filter_out_past_windows(ur_list):
    '''Case 1: The window exists entirely in the past.'''
    filter_test = lambda w, ur, r: w.end > now

    return _for_all_ur_windows(ur_list, filter_test)


@log_windows
def truncate_lower_crossing_windows(ur_list):
    '''Case 2: The window starts in the past, but finishes at a
       schedulable time. Remove the unschedulable portion of the window.'''

    def truncate_lower_crossing(w, ur, r):
        if w.start < now < w.end:
            w.start = now

        return True

    filter_test = truncate_lower_crossing

    return _for_all_ur_windows(ur_list, filter_test)


@log_windows
def truncate_upper_crossing_windows(ur_list, horizon=None):
    '''Case 4: The window starts at a schedulable time, but finishes beyond the
       scheduling horizon (provided, or semester end, or expiry date). Remove the
       unschedulable portion of the window.'''

    global now

    def truncate_upper_crossing(w, ur, r):
        effective_horizon = ur.scheduling_horizon(now)
        if horizon:
            if horizon < effective_horizon:
                effective_horizon = horizon
        if w.start < effective_horizon < w.end:
            w.end = effective_horizon

        return True

    filter_test = truncate_upper_crossing

    return _for_all_ur_windows(ur_list, filter_test)


@log_windows
def filter_out_future_windows(ur_list, horizon=None):
    '''Case 5: The window lies beyond the scheduling horizon.'''

    global now

    def filter_on_future(w, ur, r):
        effective_horizon = ur.scheduling_horizon(now)
        if horizon:
            if horizon < effective_horizon:
                effective_horizon = horizon

        return w.start < effective_horizon

    filter_test = filter_on_future

    return  _for_all_ur_windows(ur_list, filter_test)


@log_windows
def filter_on_duration(ur_list):
    '''Case 6: Return only windows which are larger than the UR's child R durations.'''
    def filter_on_duration(w, ur, r):
        # Transparently handle either float (in seconds) or datetime durations
        try:
            duration = timedelta(seconds=r.duration)
        except TypeError as e:
            duration = r.duration

        return w.end - w.start > duration

    filter_test = filter_on_duration

    return _for_all_ur_windows(ur_list, filter_test)


def _for_all_ur_windows(ur_list, filter_test):
    '''Loop over all Requests of each UserRequest provided, and execute the supplied
       filter condition on each one.'''
    for ur in ur_list:
            ur.filter_requests(filter_test)

    return ur_list


# Request Filters
#---------------------
def drop_empty_requests(ur_list):
    '''Delete child Requests which have no windows remaining.'''

    dropped_request_numbers = []
    for ur in ur_list:
        dropped = ur.drop_empty_children()
        for removed_r in dropped:
            ur_log.info("Dropped Request %s: no windows remaining" % removed_r.request_number,
                        ur.tracking_number)
            dropped_request_numbers.append(removed_r.request_number)

    return dropped_request_numbers


def filter_on_pending(ur_list):
    '''Case 7: Delete child Requests which are not in a PENDING state.'''
    for ur in ur_list:
        dropped = ur.drop_non_pending()
        req_str = pl(dropped, 'Request')
        ur_log.info("Dropped %d %s: not PENDING" % (len(dropped), req_str),
                                                    ur.tracking_number)

    return ur_list


# User Request Filters
#---------------------
@log_urs
def filter_on_expiry(ur_list):
    '''Case 8: Return only URs which haven't expired.'''

    return [ ur for ur in ur_list if ur.expires > now ]


@log_urs
def filter_on_type(ur_list):
    '''Case 9: Only return URs which can still be completed (have enough child
       Requests with Windows remaining).'''
    new_ur_list = []
    for ur in ur_list:
        if ur.is_schedulable():
            new_ur_list.append(ur)

    return new_ur_list


