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
RGs whose remaining windows are guaranteed to fall entirely within the scheduling
horizon.

Filter 6 throws away any windows too small to fit the requested observations,
after accounting for overheads. Order matters here; this filter should be called
*after* the truncation filters (2 and 4).

B) Request Group filters
-----------------------
The remaining filters operate on RGs themselves:
    * 7) expired RGs are filtered out
    * 8) RGs which cannot be completed (no child Requests with appropriate Windows
         remain) are filtered out

The convenience method run_all_filters() executes all the filters in the correct
order.

Authors: Eric Saunders, Martin Norbury
February 2013
'''

from datetime import datetime, timedelta
from adaptive_scheduler.log import RequestGroupLogger

import logging

log = logging.getLogger(__name__)

multi_rg_log = logging.getLogger('rg_logger')
rg_log = RequestGroupLogger(multi_rg_log)

# Comparator for all filters
now = datetime.utcnow()


def log_windows(fn):
    def wrap(rg_list, *args, **kwargs):
        n_windows_before = sum([rg.n_windows() for rg in rg_list])
        rg_list = fn(rg_list, *args, **kwargs)
        n_windows_after = sum([rg.n_windows() for rg in rg_list])

        log.info("%s: windows in (%d); windows out (%d)", fn.__name__, n_windows_before,
                 n_windows_after)
        return rg_list

    return wrap


def log_rgs(fn):
    def wrap(rg_list, *args, **kwargs):
        in_size = len(rg_list)
        rg_list = fn(rg_list, *args, **kwargs)
        out_size = len(rg_list)

        log.info("%s: RGs in (%d); RGs out (%d)", fn.__name__, in_size, out_size)

        return rg_list

    return wrap


def _for_all_rg_windows(rg_list, filter_test):
    '''Loop over all Requests of each RequestGroup provided, and execute the supplied
       filter condition on each one.'''
    for rg in rg_list:
        rg.filter_requests(filter_test)

    return rg_list


def filter_rgs(rg_list, running_request_ids):
    # Don't use sets here, unless you like non-deterministic orderings
    # The solve may be sensitive to order, so don't mess with it
    schedulable_rgs = run_all_filters(rg_list, running_request_ids)
    unschedulable_rgs = []
    for rg in rg_list:
        if rg not in schedulable_rgs:
            unschedulable_rgs.append(rg)

    return schedulable_rgs, unschedulable_rgs


def find_unschedulable_rg_ids(unschedulable_rgs):
    unschedulable_rg_ids = [rg.id for rg in unschedulable_rgs]

    return unschedulable_rg_ids


def set_now(user_now):
    global now
    now = user_now


@log_rgs
def run_all_filters(rg_list, running_request_ids):
    '''Execute all the filters, in the correct order. Windows may be discarded or
       truncated during this process. Unschedulable Request Groups are discarded.'''
    rg_list = filter_out_windows_for_running_requests(rg_list, running_request_ids)
    rg_list = filter_on_pending(rg_list)
    rg_list = filter_on_expiry(rg_list)
    rg_list = filter_out_past_windows(rg_list)
    rg_list = truncate_lower_crossing_windows(rg_list)
    rg_list = truncate_upper_crossing_windows(rg_list)
    rg_list = filter_out_future_windows(rg_list)
    rg_list = filter_on_duration(rg_list)
    rg_list = filter_on_type(rg_list, running_request_ids)

    return rg_list


@log_windows
def filter_out_windows_for_running_requests(rg_list, running_request_ids):
    '''Case 1: Remove windows for requests that are already running'''

    def filter_test(w, rg, r):
        if r.id in running_request_ids:
            tag = 'RequestIsRunning'
            msg = 'Request %d Window (at %s) %s -> %s removed because request is currently running' % (
            r.id, w.get_resource_name(),
            w.start, w.end)
            rg.emit_rg_feedback(msg, tag)
            return False
        else:
            return True

    return _for_all_rg_windows(rg_list, filter_test)


# A) Window Filters
# ------------------
@log_windows
def filter_out_past_windows(rg_list):
    '''Case 2: The window exists entirely in the past.'''

    def filter_test(w, rg, r):
        if w.end > now:
            return True
        else:
            tag = 'WindowInPast'
            msg = 'Request %d Window (at %s) %s -> %s falls before %s' % (r.id, w.get_resource_name(),
                                                                          w.start, w.end, now)
            rg.emit_rg_feedback(msg, tag)
            return False

    return _for_all_rg_windows(rg_list, filter_test)


@log_windows
def truncate_lower_crossing_windows(rg_list):
    '''Case 3: The window starts in the past, but finishes at a
       schedulable time. Remove the unschedulable portion of the window.'''

    def truncate_lower_crossing(w, rg, r):
        if w.start < now < w.end:
            tag = 'WindowTruncatedLower'
            msg = 'Request %d Window (at %s) %s -> %s truncated to %s' % (r.id, w.get_resource_name(),
                                                                          w.start, w.end, now)
            rg.emit_rg_feedback(msg, tag)
            w.start = now

        return True

    filter_test = truncate_lower_crossing

    return _for_all_rg_windows(rg_list, filter_test)


@log_windows
def truncate_upper_crossing_windows(rg_list, horizon=None):
    '''Case 4: The window starts at a schedulable time, but finishes beyond the
       scheduling horizon (provided, or semester end, or expiry date). Remove the
       unschedulable portion of the window.'''

    global now

    def truncate_upper_crossing(w, rg, r):
        effective_horizon = rg.expires
        if horizon:
            if horizon < effective_horizon:
                effective_horizon = horizon
        if w.start < effective_horizon < w.end:
            tag = 'WindowTruncatedUpper'
            msg = 'Request %d Window (at %s) %s -> %s truncated to %s' % (r.id, w.get_resource_name(),
                                                                          w.start, w.end, effective_horizon)
            rg.emit_rg_feedback(msg, tag)
            w.end = effective_horizon

        return True

    filter_test = truncate_upper_crossing

    return _for_all_rg_windows(rg_list, filter_test)


@log_windows
def filter_out_future_windows(rg_list, horizon=None):
    '''Case 5: The window lies beyond the scheduling horizon.'''

    global now

    def filter_on_future(w, rg, r):
        effective_horizon = rg.expires
        if horizon:
            if horizon < effective_horizon:
                effective_horizon = horizon

        if w.start < effective_horizon:
            return True
        else:
            tag = 'WindowBeyondHorizon'
            msg = 'Request %d Window (at %s) %s -> %s starts after the scheduling horizon (%s)' % (
                r.id,
                w.get_resource_name(),
                w.start,
                w.end,
                effective_horizon)
            rg.emit_rg_feedback(msg, tag)
            return False

    filter_test = filter_on_future

    return _for_all_rg_windows(rg_list, filter_test)


@log_windows
def filter_on_duration(rg_list, filter_executor=_for_all_rg_windows):
    '''Case 6: Return only windows which are larger than the RG's child R durations.'''

    def filter_on_duration(w, rg, r):
        # Transparently handle either float (in seconds) or datetime durations
        try:
            duration = timedelta(seconds=r.duration)
        except TypeError:
            duration = r.duration

        if w.end - w.start > duration:
            return True
        else:
            tag = 'WindowTooSmall'
            msg = "Request %d Window (at %s) %s -> %s too small for duration '%s'" % (r.id,
                                                                                      w.get_resource_name(),
                                                                                      w.start, w.end, duration)
            rg.emit_rg_feedback(msg, tag)
            return False

    filter_test = filter_on_duration

    return filter_executor(rg_list, filter_test)


# Request Filters
# ---------------------
def drop_empty_requests(rg_list):
    '''Delete child Requests which have no windows remaining.'''

    dropped_request_ids = []
    for rg in rg_list:
        dropped = rg.drop_empty_children()
        for removed_r in dropped:
            tag = 'NoWindowsRemaining'
            msg = "Dropped Request %d: no windows remaining" % removed_r.id
            rg.emit_rg_feedback(msg, tag)
            rg_log.info(msg, rg.id)
            dropped_request_ids.append(removed_r.id)

    return dropped_request_ids


def filter_on_pending(rg_list):
    '''Case 7: Delete child Requests which are not in a PENDING state.'''
    total_dropped = 0
    for rg in rg_list:
        dropped = rg.drop_non_pending()
        total_dropped += len(dropped)
        if dropped:
            rg_log.info("Dropped %d Requests: not PENDING" % len(dropped),
                        rg.id)
    log.info("Dropped %d Requests in Total: not PENDING" % (total_dropped))

    return rg_list


# Request Group Filters
# ---------------------
@log_rgs
def filter_on_expiry(rg_list):
    '''Case 8: Return only RGs which haven't expired.'''

    not_expired = []
    for rg in rg_list:
        if rg.expires > now:
            not_expired.append(rg)
        else:
            tag = 'RequestGroupExpired'
            msg = 'RequestGroup %d expired on %s (and now = %s)' % (rg.id,
                                                                    rg.expires, now)
            rg.emit_rg_feedback(msg, tag)

    return not_expired


@log_rgs
def filter_on_type(rg_list, running_request_ids=()):
    '''Case 9: Only return RGs which can still be completed (have enough child
       Requests with Windows remaining or running requests).'''
    new_rg_list = []
    for rg in rg_list:
        if rg.is_schedulable(running_request_ids):
            new_rg_list.append(rg)
        else:
            tag = 'RequestGroupImpossible'
            msg = 'Dropped RequestGroup %d: not enough Requests remaining' % rg.id
            rg.emit_rg_feedback(msg, tag)

    return new_rg_list
