#!/usr/bin/env python

'''
unschedule_requests.py - Safely make a set of requests ignored by the scheduler

description

Author: Eric Saunders
May 2015
'''

from reqdb.client                 import SchedulerClient
from adaptive_scheduler.requestdb import RequestDBInterface

from datetime    import datetime
from collections import defaultdict

def get_user_requests(requestdb_url, start, end):
    requestdb_client = SchedulerClient(requestdb_url)
    reqdb = RequestDBInterface(requestdb_client)
    urs = reqdb.get_all_user_requests(start, end)

    return urs

def filter_urs_by_proposal_id(urs, target_proposal_id):
    ur_counter = defaultdict(int)

    filtered_urs = []
    for ur in urs:
        proposal_id = ur['proposal']['proposal_id']
        ur_counter[proposal_id] += 1

        if proposal_id == target_proposal_id:
            filtered_urs.append(ur)

    return filtered_urs, ur_counter


if __name__ == '__main__':

    requestdb_url      = 'http://scheduler1/requestdb/'
    target_proposal_id = 'KEY2014A-003'

    start = datetime(2015, 5, 10, 15)
    end   = datetime(2015, 5, 21, 15)
#    start = '2015-05-18 20:00:00'
#    end   = '2015-05-20 20:00:00'

    urs = get_user_requests(requestdb_url, start, end)
    target_urs, ur_counter = filter_urs_by_proposal_id(urs, target_proposal_id)

    unschedulable_ur_numbers = []
    unschedulable_r_numbers  = []
    for ur in target_urs:
        unschedulable_ur_numbers.append(ur['tracking_number'])
        for r in ur['requests']:
            unschedulable_r_numbers.append(r['request_number'])

    print "Marking %s URs UNSCHEDULABLE" % len(unschedulable_ur_numbers)
    print "Marking %s Rs UNSCHEDULABLE" % len(unschedulable_r_numbers)

    # Uncomment these to really do it
    #reqdb.set_requests_to_unschedulable(unschedulable_r_numbers)
    #reqdb.set_user_requests_to_unschedulable(unschedulable_ur_numbers)


