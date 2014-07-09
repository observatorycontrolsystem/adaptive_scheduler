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


def _for_all_ur_windows(ur_list, filter_test):
    '''Loop over all Requests of each UserRequest provided, and execute the supplied
       filter condition on each one.'''
    for ur in ur_list:
            ur.filter_requests(filter_test)

    return ur_list





def filter_urs(ur_list, running_request_numbers):

    # Don't use sets here, unless you like non-deterministic orderings
    # The solve may be sensitive to order, so don't mess with it
    schedulable_urs = run_all_filters(ur_list, running_request_numbers)
    unschedulable_urs = []
    for ur in ur_list:
        if ur not in schedulable_urs:
            unschedulable_urs.append(ur)

    return schedulable_urs, unschedulable_urs


def find_unschedulable_ur_numbers(unschedulable_urs):
    unschedulable_ur_numbers = [ur.tracking_number for ur in unschedulable_urs]

    return unschedulable_ur_numbers


def set_now(user_now):
    global now
    now = user_now


@log_urs
def run_all_filters(ur_list, running_request_numbers):
    '''Execute all the filters, in the correct order. Windows may be discarded or
       truncated during this process. Unschedulable User Requests are discarded.'''
    ur_list = filter_out_windows_for_running_requests(ur_list, running_request_numbers)
    ur_list = filter_on_pending(ur_list)
    ur_list = filter_on_expiry(ur_list)
    ur_list = filter_out_past_windows(ur_list)
    ur_list = truncate_lower_crossing_windows(ur_list)
    ur_list = truncate_upper_crossing_windows(ur_list)
    ur_list = filter_out_future_windows(ur_list)
    ur_list = filter_on_duration(ur_list)
    ur_list = filter_on_type(ur_list, running_request_numbers)

    return ur_list


@log_windows
def filter_out_windows_for_running_requests(ur_list, running_request_numbers):
    '''Case 1: Remove windows for requests that are already running'''
    def filter_test(w, ur, r):
        if r.request_number in running_request_numbers:
            tag = 'RequestIsRunning'
            msg = 'Request %s Window (at %s) %s -> %s removed because request is currently running' % (r.request_number, w.get_resource_name(),
                                                               w.start, w.end)
            ur.emit_user_feedback(msg, tag)
            return False
        else:
            return True

    return _for_all_ur_windows(ur_list, filter_test)


# A) Window Filters
#------------------
@log_windows
def filter_out_past_windows(ur_list):
    '''Case 2: The window exists entirely in the past.'''
    def filter_test(w, ur, r):
        if w.end > now:
            return True
        else:
            tag = 'WindowInPast'
            msg = 'Request %s Window (at %s) %s -> %s falls before %s' % (r.request_number, w.get_resource_name(),
                                                               w.start, w.end, now)
            ur.emit_user_feedback(msg, tag)
            return False

    return _for_all_ur_windows(ur_list, filter_test)


@log_windows
def truncate_lower_crossing_windows(ur_list):
    '''Case 3: The window starts in the past, but finishes at a
       schedulable time. Remove the unschedulable portion of the window.'''

    def truncate_lower_crossing(w, ur, r):
        if w.start < now < w.end:
            tag = 'WindowTruncatedLower'
            msg = 'Request %s Window (at %s) %s -> %s truncated to %s' % (r.request_number, w.get_resource_name(),
                                                               w.start, w.end, now)
            ur.emit_user_feedback(msg, tag)
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
            tag = 'WindowTruncatedUpper'
            msg = 'Request %s Window (at %s) %s -> %s truncated to %s' % (r.request_number, w.get_resource_name(), 
                                                               w.start, w.end, effective_horizon)
            ur.emit_user_feedback(msg, tag)
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

        if w.start < effective_horizon:
            return True
        else:
            tag = 'WindowBeyondHorizon'
            msg = 'Request %s Window (at %s) %s -> %s starts after the scheduling horizon (%s)' % (
                                                                                r.request_number,
                                                                                w.get_resource_name(),
                                                                                w.start,
                                                                                w.end,
                                                                                effective_horizon)
            ur.emit_user_feedback(msg, tag)
            return False

    filter_test = filter_on_future

    return  _for_all_ur_windows(ur_list, filter_test)


@log_windows
def filter_on_duration(ur_list, filter_executor=_for_all_ur_windows):
    '''Case 6: Return only windows which are larger than the UR's child R durations.'''
    def filter_on_duration(w, ur, r):
        # Transparently handle either float (in seconds) or datetime durations
        try:
            duration = timedelta(seconds=r.duration)
        except TypeError as e:
            duration = r.duration

        if w.end - w.start > duration:
            return True
        else:
            tag = 'WindowTooSmall'
            msg = "Request %s Window (at %s) %s -> %s too small for duration '%s'" % (r.request_number,
                                                                           w.get_resource_name(),
                                                                           w.start, w.end, duration)
            ur.emit_user_feedback(msg, tag)
            return False

    filter_test = filter_on_duration

    return filter_executor(ur_list, filter_test)



# Request Filters
#---------------------
def drop_empty_requests(ur_list):
    '''Delete child Requests which have no windows remaining.'''

    dropped_request_numbers = []
    for ur in ur_list:
        dropped = ur.drop_empty_children()
        for removed_r in dropped:
            tag = 'NoWindowsRemaining'
            msg = "Dropped Request %s: no windows remaining" % removed_r.request_number
            ur.emit_user_feedback(msg, tag)
            ur_log.info(msg, ur.tracking_number)
            dropped_request_numbers.append(removed_r.request_number)

    return dropped_request_numbers


def filter_on_pending(ur_list):
    '''Case 7: Delete child Requests which are not in a PENDING state.'''
    for ur in ur_list:
        dropped = ur.drop_non_pending()
        if dropped:
            ur_log.info("Dropped %d Requests: not PENDING" % len(dropped),
                                                        ur.tracking_number)
            log.info("Dropped %d Requests (UR %s): not PENDING" % (len(dropped), ur.tracking_number))

    return ur_list


# User Request Filters
#---------------------
@log_urs
def filter_on_expiry(ur_list):
    '''Case 8: Return only URs which haven't expired.'''

    not_expired = []
    for ur in ur_list:
        if ur.expires > now:
            not_expired.append(ur)
        else:
            tag = 'UserRequestExpired'
            msg = 'User Request %s expired on %s (and now = %s)' % (ur.tracking_number,
                                                                    ur.expires, now)
            ur.emit_user_feedback(msg, tag)

    return not_expired

@log_urs
def filter_on_type(ur_list, running_request_numbers):
    '''Case 9: Only return URs which can still be completed (have enough child
       Requests with Windows remaining or running requests).'''
    new_ur_list = []
    for ur in ur_list:
        if ur.is_schedulable(running_request_numbers):
            new_ur_list.append(ur)
        else:
            tag = 'UserRequestImpossible'
            msg = 'Dropped UserRequest %s: not enough Requests remaining' % ur.tracking_number
            ur.emit_user_feedback(msg, tag)

    return new_ur_list


