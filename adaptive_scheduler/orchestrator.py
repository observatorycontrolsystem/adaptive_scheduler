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
                                                 make_compound_reservations )
from adaptive_scheduler.input    import ( get_telescope_network, dump_scheduler_input )
from adaptive_scheduler.printing import print_schedule, print_compound_reservations
from adaptive_scheduler.pond     import send_schedule_to_pond

#from adaptive_scheduler.kernel.fullscheduler_v3 import FullScheduler_v3 as FullScheduler
#from adaptive_scheduler.kernel.fullscheduler_v2 import FullScheduler_v2 as FullScheduler
from adaptive_scheduler.kernel.fullscheduler_v5 import FullScheduler_v5 as FullScheduler

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

def get_requests_from_db(url, telescope_class):

    search = SearchQuery()
    search.set_location(telescope_class=telescope_class)
    sc = SchedulerClient(url)
    json_ur_list = sc.retrieve(search, debug=True)
    ur_list = json.loads(json_ur_list)

    return ur_list


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
def main(requests):
    # TODO: Replace with config file (from laptop)
    semester_start = datetime(2013, 3, 28, 0, 0, 0)
    semester_end   = datetime(2013, 4, 28, 0, 0, 0)

    flat_url         = 'http://mbecker-linux2.lco.gtn:8001/get/requests/'
    hierarchical_url = 'http://mbecker-linux2.lco.gtn:8001/get/'

    url  = hierarchical_url


    # Collapse each request tree
    collapsed_reqs = collapse_requests(requests)

    # Configuration files
    tel_file = 'telescopes.dat'
    scheduler_dump_file = 'to_schedule.pickle'


    mb = ModelBuilder(tel_file)

#    user_reqs = []
#    for serialised_ur in collapsed_reqs:
#        proposal_data = serialised_ur['proposal']
#        del(serialised_ur['proposal'])
#        ur = request_factory.parse(serialised_ur, proposal_data)
#        user_reqs.append(ur)

    user_reqs = []
    i = 0
    for serialised_req in collapsed_reqs:
        print "Trying i", i
        user_req = mb.build_user_request(serialised_req)
        user_reqs.append(user_req)
        i += 1

    tels = get_telescope_network(tel_file)


    # Construct visibility objects for each telescope
    visibility_from = construct_visibilities(tels, semester_start, semester_end)

    # Translate when telescopes are available into kernel speak
    resource_windows = construct_resource_windows(visibility_from, semester_start)

    # Convert CompoundRequests -> CompoundReservations
    to_schedule = make_compound_reservations(user_reqs, visibility_from,
                                             semester_start)

    # Dump the variables to be scheduled, for offline analysis if necessary
    contractual_obligations = []
    dump_scheduler_input(scheduler_dump_file, to_schedule, resource_windows,
                         contractual_obligations)

    print_compound_reservations(to_schedule)

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
#    delete_scheduled_blocks()

    # Convert the kernel schedule into POND blocks, and send them to the POND
    send_schedule_to_pond(schedule, semester_start)

    #print "cr.reservation_list:", cr.reservation_list
