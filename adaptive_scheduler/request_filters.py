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
import logging
log = logging.getLogger(__name__)

# Comparator for all filters
now = datetime.utcnow()

def log_urs(fn):
    def wrap(ur_list):
        in_size  = len(ur_list)
        ur_list  = fn(ur_list)
        out_size = len(ur_list)

        log.debug("%s: URs in (%d); URs out (%d)", fn.__name__, in_size, out_size)

        return ur_list

    return wrap


def filter_and_set_unschedulable_urs(client, ur_list, user_now, dry_run=False):
    global now
    now = user_now

    initial_urs     = set(ur_list)
    schedulable_urs = set(run_all_filters(ur_list))

    unschedulable_urs = initial_urs - schedulable_urs

    log.info("Found %d unschedulable %s after filtering", *pl(len(unschedulable_urs), 'UR'))

    unschedulable_r_numbers = []
    for ur in unschedulable_urs:
        for r in ur.requests:
            # Only blacklist child Requests with no windows
            # (the UR could be unschedulable due to type, but that is a parent
            # issue, not the child's)
            if not r.has_windows():
                # TODO: Contemplate errors
                msg =  "Request %s (UR %s) is UNSCHEDULABLE" % (ur.tracking_number,
                                                                r.request_number)
                log.info(msg)
                unschedulable_r_numbers.append(r.request_number)


    if dry_run:
        log.info("Dry-run: Not updating any Request DB states")
    else:
        log.info("Updating Request DB states")
        # Update the state of all the unschedulable Requests in the DB in one go
        client.set_request_state('UNSCHEDULABLE', unschedulable_r_numbers)

        # Update the state of all the unschedulable User Requests in the DB in one go
        unschedulable_ur_numbers = [ur.tracking_number for ur in unschedulable_urs]
        client.set_user_request_state('UNSCHEDULABLE', unschedulable_ur_numbers)

    return schedulable_urs


@log_urs
def run_all_filters(ur_list):
    '''Execute all the filters, in the correct order. Windows may be discarded or
       truncated during this process. Unschedulable User Requests are discarded.'''
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
@log_urs
def filter_out_past_windows(ur_list):
    '''Case 1: The window exists entirely in the past.'''
    filter_test = lambda w, ur: w.end > now

    return _for_all_ur_windows(ur_list, filter_test)


@log_urs
def truncate_lower_crossing_windows(ur_list):
    '''Case 2: The window starts in the past, but finishes at a
       schedulable time. Remove the unschedulable portion of the window.'''

    def truncate_lower_crossing(w, ur):
        if w.start < now < w.end:
            w.start = now

        return True

    filter_test = truncate_lower_crossing

    return _for_all_ur_windows(ur_list, filter_test)


@log_urs
def truncate_upper_crossing_windows(ur_list):
    '''Case 4: The window starts at a schedulable time, but finishes beyond the
       scheduling horizon (semester end or expiry date). Remove the unschedulable
       portion of the window.'''

    def truncate_upper_crossing(w, ur):
        horizon = ur.scheduling_horizon()
        if w.start < horizon < w.end:
            w.end = horizon

        return True

    filter_test = truncate_upper_crossing

    return _for_all_ur_windows(ur_list, filter_test)


@log_urs
def filter_out_future_windows(ur_list):
    '''Case 5: The window lies beyond the scheduling horizon.'''
    filter_test = lambda w, ur: w.start < ur.scheduling_horizon()

    return _for_all_ur_windows(ur_list, filter_test)


@log_urs
def filter_on_duration(ur_list):
    '''Case 6: Return only windows which are larger than the UR's duration.'''
    def filter_on_duration(w, ur):
        # Transparently handle either float (in seconds) or datetime durations
        try:
            duration = timedelta(seconds=ur.duration)
        except TypeError as e:
            duration = ur.duration

        return w.end - w.start > duration

    filter_test = filter_on_duration

    return _for_all_ur_windows(ur_list, filter_test)


def _for_all_ur_windows(ur_list, filter_test):
    '''Loop over all Requests of each UserRequest provided, and execute the supplied
       filter condition on each one.'''
    for ur in ur_list:
            ur.filter_requests(filter_test)

    return ur_list


# User Request Filters
#---------------------
@log_urs
def filter_on_expiry(ur_list):
    '''Case 7: Return only URs which haven't expired.'''

    return [ ur for ur in ur_list if ur.expires > now ]


@log_urs
def filter_on_type(ur_list):
    '''Case 8: Only return URs which can still be completed (have enough child
       Requests with Windows remaining).'''
    new_ur_list = []
    for ur in ur_list:
        if ur.is_schedulable():
            new_ur_list.append(ur)

    return new_ur_list


