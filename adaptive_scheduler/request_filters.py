#!/usr/bin/env python
from __future__ import division

'''
request_filters.py - Filtering of Requests for schedulability

description

     Semester start      Now               Semester end
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

Authors: Eric Saunders, Martin Norbury
February 2013
'''

from datetime import datetime
from adaptive_scheduler import semester_service


def filter_on_expiry(ur_list):
    now = datetime.utcnow()

    return [ ur for ur in ur_list if ur.expires > now ]


def filter_out_past_windows(ur_list):
    now = datetime.utcnow()
    filter_test = lambda w, ur: w.start > now and w.end > now

    return for_all_ur_windows(ur_list, filter_test)


def filter_out_future_windows(ur_list):
    sem_end = semester_service.get_semester_end()
    filter_test = lambda w, ur: w.start < sem_end and w.end < sem_end

    return for_all_ur_windows(ur_list, filter_test)


def truncate_lower_crossing_windows(ur_list):
    now = datetime.utcnow()

    def truncate_lower_crossing(w, ur):
        if w.start < now < w.end:
            w.start = now

        return True

    filter_test = truncate_lower_crossing

    return for_all_ur_windows(ur_list, filter_test)


def truncate_upper_crossing_windows(ur_list):
    sem_end = semester_service.get_semester_end()

    def truncate_upper_crossing(w, ur):
        if w.start < sem_end < w.end:
            w.end = sem_end

        return True

    filter_test = truncate_upper_crossing

    return for_all_ur_windows(ur_list, filter_test)


def filter_on_duration(ur_list):
    '''Return only windows which are larger than the UR's duration.'''

    filter_test = lambda w, ur: w.end - w.start > ur.duration

    return for_all_ur_windows(ur_list, filter_test)


def for_all_ur_windows(ur_list, filter_test):
    for ur in ur_list:
        for r in ur.requests:
            ur.filter_requests(filter_test)


    return ur_list



def filter_on_type(ur_list):
    new_ur_list = []
    for ur in ur_list:
        ok_to_add = True
        if ur.operator == 'and':
            for r in ur.requests:
                print "Doing an r"
                all_windows = []
                for resource_name, windows in r.windows.windows_for_resource.iteritems():
                    print "Windows:", windows
                    all_windows += windows

                if len(all_windows) == 0:
                    ok_to_add = False

        if ok_to_add:
            new_ur_list.append(ur)

    return new_ur_list


