#!/usr/bin/env python
from __future__ import division

'''
request_filters.py - Filtering of Requests for schedulability

description

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
    filter_test = lambda w: w.start > now and w.end > now

    return for_all_ur_windows(ur_list, filter_test)


def filter_out_future_windows(ur_list):
    # TODO: Implement this service!
    sem_end = semester_service.get_semester_end()
    filter_test = lambda w: w.start < sem_end and w.end < sem_end

    return for_all_ur_windows(ur_list, filter_test)


def truncate_lower_crossing_windows(ur_list):
    now = datetime.utcnow()

    def truncate_lower_crossing(w):
        if w.start < now and w.end > now:
            w.start = now

        return True

    filter_test = truncate_lower_crossing

    return for_all_ur_windows(ur_list, filter_test)


def for_all_ur_windows(ur_list, filter_test):
    for ur in ur_list:
        for r in ur.requests:
            for resource_name, windows in r.windows.windows_for_resource.iteritems():
                r.windows.windows_for_resource[resource_name] = [w for w in windows if filter_test(w)]

    return ur_list


#TODO: Expiry, compound filter
