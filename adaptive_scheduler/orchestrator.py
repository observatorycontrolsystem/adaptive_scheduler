#!/usr/bin/env python
'''
orchestrator.py - Top-level entry-point to the scheduler

This module provides main(), the top-level running function of the scheduler. It may
be called standalone for single-thread testing purposes, or embedded in a threaded
environoment for persistent execution.

Author: Eric Saunders
August 2012
'''
from __future__ import division

import sys
import json
import ast
from datetime import datetime

from adaptive_scheduler.request_parser  import TreeCollapser
from adaptive_scheduler.tree_walker     import RequestMaxDepthFinder
from adaptive_scheduler.model2          import ModelBuilder
from adaptive_scheduler.kernel_mappings import ( construct_visibilities,
                                                 construct_resource_windows,
                                                 make_compound_reservations,
                                                 prefilter_for_kernel        )
from adaptive_scheduler.input    import ( get_telescope_network, dump_scheduler_input )
from adaptive_scheduler.printing import ( print_schedule, print_compound_reservations,
                                          pluralise )
from adaptive_scheduler.pond     import send_schedule_to_pond, cancel_schedule
from adaptive_scheduler.semester_service import get_semester_block, get_semester_code

from adaptive_scheduler.kernel.fullscheduler_v5 import FullScheduler_v5 as FullScheduler
from adaptive_scheduler.request_filters import filter_and_set_unschedulable_urs
from adaptive_scheduler.utils import timeit

from reqdb.client import SearchQuery, SchedulerClient
from reqdb        import request_factory


#TODO: Refactor - move all these functions to better locations
def get_requests(url, telescope_class):

    rc  = RetrievalClient(url)
    rc.set_location(telescope_class)

    json_req_str = rc.retrieve()
    requests     = json.loads(json_req_str)

    return requests


def get_requests_from_file(req_filename, telescope_class):

    with open(req_filename, 'r') as req_fh:
        req_data = req_fh.read()
        return ast.literal_eval(req_data)

def get_requests_from_json(req_filename, telescope_class):

    with open(req_filename, 'r') as req_fh:
        req_data = req_fh.read()
        return json.loads(req_data)

@timeit
def get_requests_from_db(url, telescope_class):
    sem_start, sem_end = get_semester_block()
    format = '%Y-%m-%d %H:%M:%S'

    search = SearchQuery()
    search.set_date(start=sem_start.strftime(format), end=sem_end.strftime(format))

    print "Asking DB (%s) for User Requests between %s and %s" % (url, sem_start, sem_end)
    sc           = SchedulerClient(url)
    json_ur_list = sc.retrieve(search, debug=True)
    ur_list      = json.loads(json_ur_list)

    write_requests_to_file_as_json(ur_list, 'retrieved_urs.dat')

    return ur_list


def write_requests_to_file_as_json(ur_list, filename):
    out_fh = open(filename, 'w')
    json.dump(ur_list, out_fh)
    out_fh.close()


def write_requests_to_file(requests, filename):
    out_fh = open(filename, 'w')
    out_fh.write(str(requests))
    out_fh.close()


def collapse_requests(requests):
    collapsed_reqs = []
    for i, req_dict in enumerate(requests):

        tc = TreeCollapser(req_dict)
        tc.collapse_tree()

        if tc.is_collapsible:
            print "Request %d was successfully collapsed!" % i

            depth_finder = RequestMaxDepthFinder(tc.collapsed_tree)
            depth_finder.walk()

            # The scheduling kernel can't handle more than one level of nesting
            if depth_finder.max_depth > 1:
                print "Request %d is still too deep (%d levels) - skipping." % ( i,
                                                                  depth_finder.max_depth )

            else:
                print "Request %d has depth %d - continuing." % ( i,
                                                                  depth_finder.max_depth )
                collapsed_reqs.append(tc.collapsed_tree)

        else:
            print "Request %d could not be collapsed - skipping." % i


    return collapsed_reqs




# TODO: Add configuration options, refactor into smaller chunks
# TODO: Remove hard-coded options
@timeit
def main(requests, sched_client):
    semester_start, semester_end = get_semester_block()
    date_fmt = '%Y-%m-%d'

    print "Scheduling for semester %s (%s to %s)" % (get_semester_code(),
                                                     semester_start.strftime(date_fmt),
                                                     semester_end.strftime(date_fmt))

    ur_string = pluralise(len(requests), 'User Request')
    print "Received %d %s from Request DB" % ( len(requests), ur_string)

    # Collapse each request tree
    collapsed_reqs = collapse_requests(requests)

    # Configuration files
    tel_file            = 'telescopes.dat'
    scheduler_dump_file = 'to_schedule.pickle'

    mb = ModelBuilder(tel_file)

#    user_reqs = []
#    for serialised_ur in collapsed_reqs:
#        proposal_data = serialised_ur['proposal']
#        del(serialised_ur['proposal'])
#        ur = request_factory.parse(serialised_ur, proposal_data)
#        user_reqs.append(ur)

    user_reqs = []
    for serialised_req in collapsed_reqs:
        user_req = mb.build_user_request(serialised_req)
        user_reqs.append(user_req)

    # TODO: Swap to tels2
    tels = mb.tel_network.telescopes
#    print "Available telescopes:"
#    for t, v in tels.iteritems():
#        print t, v


    # Filter by window, and set UNSCHEDULABLE on the Request DB as necessary
    user_reqs = filter_and_set_unschedulable_urs(sched_client, user_reqs)

    # Construct visibility objects for each telescope
    visibility_from = construct_visibilities(tels, semester_start, semester_end)

    # Do another check on duration and operator soundness, after dark/rise checking
    user_reqs = prefilter_for_kernel(user_reqs, visibility_from)

    # Convert CompoundRequests -> CompoundReservations
    to_schedule = make_compound_reservations(user_reqs, visibility_from,
                                             semester_start)

    # Translate when telescopes are available into kernel speak
    resource_windows = construct_resource_windows(visibility_from, semester_start)

    # Dump the variables to be scheduled, for offline analysis if necessary
    contractual_obligations = []
    dump_scheduler_input(scheduler_dump_file, to_schedule, resource_windows,
                         contractual_obligations)

    print_compound_reservations(to_schedule)


    if not to_schedule:
        print "Nothing to schedule! Skipping kernel call..."
        return

    # Filter a second time to remove (now) unschedulable Requests

    # Instantiate and run the scheduler
    # TODO: Set alignment to start of first night on each resource
    # TODO: Move this to a config file
    time_slicing_dict = {
                            '0m4a.aqwa.bpl' : [0, 600],
                            '0m4b.aqwa.bpl' : [0, 600],
                            '1m0a.doma.elp' : [0, 600],
                            '1m0a.domb.lsc' : [0, 600],
                        }
    kernel   = FullScheduler(to_schedule, resource_windows, contractual_obligations,
                             time_slicing_dict)
    schedule = kernel.schedule_all()

    # Summarise the schedule in normalised epoch (kernel) units of time
    print_schedule(schedule, semester_start, semester_end)

    # Clean out all existing scheduled blocks
    n_deleted = cancel_schedule(tels, semester_start, semester_end)

    # Convert the kernel schedule into POND blocks, and send them to the POND
    n_submitted = send_schedule_to_pond(schedule, semester_start)

    print "\nScheduling Summary"
    print "------------------"
    print "Received %d %s from Request DB" % ( len(requests), ur_string)
    print "In total, deleted %d previously scheduled %s" % (n_deleted,
                                                  pluralise(n_deleted, 'block'))
    print "Submitted %d new %s to the POND" % (n_submitted,
                                               pluralise(n_submitted, 'block'))
    print "Scheduling complete."

