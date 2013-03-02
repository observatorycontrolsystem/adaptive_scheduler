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

from datetime import datetime


def filter_and_set_unschedulable_urs(client, ur_list):
    initial_urs     = set(ur_list)
    schedulable_urs = set(run_all_filters(ur_list))

    unschedulable_urs = schedulable_urs - initial_urs

    for ur in unschedulable_urs:
        for r in ur.requests:
            # Only blacklist child Requests with no windows
            # (the UR could be unschedulable due to type, but that is a parent
            # issue, not the child's)
            # TODO: Set the state of the parent (not implemented at Req DB yet)
            if not r.has_windows():
                # TODO: Contemplate errors
                client.set_request_state('UNSCHEDULABLE', r.request_number)

    return schedulable_urs


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
def filter_out_past_windows(ur_list):
    '''Case 1: The window exists entirely in the past.'''
    now = datetime.utcnow()
    filter_test = lambda w, ur: w.start > now and w.end > now

    return _for_all_ur_windows(ur_list, filter_test)


def truncate_lower_crossing_windows(ur_list):
    '''Case 2: The window starts in the past, but finishes at a
       schedulable time. Remove the unschedulable portion of the window.'''
    now = datetime.utcnow()

    def truncate_lower_crossing(w, ur):
        if w.start < now < w.end:
            w.start = now

        return True

    filter_test = truncate_lower_crossing

    return _for_all_ur_windows(ur_list, filter_test)


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

def filter_out_future_windows(ur_list):
    '''Case 5: The window lies beyond the scheduling horizon.'''
    filter_test = lambda w, ur: w.start < ur.scheduling_horizon() and \
                                w.end < ur.scheduling_horizon()

    return _for_all_ur_windows(ur_list, filter_test)


def filter_on_duration(ur_list):
    '''Case 6: Return only windows which are larger than the UR's duration.'''
    filter_test = lambda w, ur: w.end - w.start > ur.duration

    return _for_all_ur_windows(ur_list, filter_test)


def _for_all_ur_windows(ur_list, filter_test):
    '''Loop over all Requests of each UserRequest provided, and execute the supplied
       filter condition on each one.'''
    for ur in ur_list:
        for r in ur.requests:
            ur.filter_requests(filter_test)

    return ur_list


# User Request Filters
#---------------------
def filter_on_expiry(ur_list):
    '''Case 7: Return only URs which haven't expired.'''
    now = datetime.utcnow()

    return [ ur for ur in ur_list if ur.expires > now ]


def filter_on_type(ur_list):
    '''Case 8: Only return URs which can still be completed (have enough child
       Requests with Windows remaining).'''
    new_ur_list = []
    for ur in ur_list:
        if ur.is_schedulable():
            new_ur_list.append(ur)

    return new_ur_list


