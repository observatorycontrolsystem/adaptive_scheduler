#!/usr/bin/env python

'''
as2.py - summary line

description

Author: Eric Saunders
July 2012
'''
from __future__ import division
import sys
import json
import ast
from datetime import datetime

# TODO: Remove hard-coded paths
#sys.path.append('/home/esaunderslocal/projects/schedulerWebService')
#sys.path.append('${HOME}/programming/python/rise_set/')


#from client.retrieval_client import RetrievalClient
from adaptive_scheduler.request_parser import TreeCollapser
from adaptive_scheduler.tree_walker import RequestMaxDepthFinder
from adaptive_scheduler.model2 import ModelBuilder
from adaptive_scheduler.kernel_mappings import ( construct_visibilities,
                                                 construct_resource_windows,
                                                 make_compound_reservations )
from adaptive_scheduler.input import ( get_telescope_network, dump_scheduler_input )
from adaptive_scheduler.printing import print_schedule

from adaptive_scheduler.kernel.fullscheduler_v3 import FullScheduler_v3 as FullScheduler


def get_requests(url, telescope_class):

    rc  = RetrievalClient(url)
    rc.set_location(telescope_class)

    json_req_str = rc.retrieve()
    requests     = json.loads(json_req_str)

    return requests


def get_requests_from_file(req_filename, telescope_class):

    req_fh = open(req_filename, 'r')
    req_data = req_fh.read()

    return ast.literal_eval(req_data)


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




# TODO: Replace with config file (from laptop)
semester_start = datetime(2011, 11, 1, 0, 0, 0)
semester_end   = datetime(2011, 11, 8, 0, 0, 0)

flat_url         = 'http://mbecker-linux2.lco.gtn:8001/get/requests/'
hierarchical_url = 'http://mbecker-linux2.lco.gtn:8001/get/'

url  = hierarchical_url


# TODO: Replace with loop over all classes, schedule each separately
telescope_class = '1m0'

# Acquire and collapse the requests
#requests       = get_requests(url, telescope_class)
requests       = get_requests_from_file('requests.dat', 'dummy arg')
collapsed_reqs = collapse_requests(requests)

# Configuration files
tel_file = 'telescopes.dat'
scheduler_dump_file = 'to_schedule.pickle'


mb = ModelBuilder(tel_file)

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

# Instantiate and run the scheduler
kernel   = FullScheduler(to_schedule, resource_windows, contractual_obligations)
schedule = kernel.schedule_all()

# Summarise the schedule in normalised epoch (kernel) units of time
print_schedule(schedule, semester_start, semester_end)

# Convert the kernel schedule into POND blocks, and send them to the POND
#send_schedule_to_pond(schedule, semester_start)





# TODO: Temporary debug code
v = visibility_from['1m0a.doma.bpl']
rw = resource_windows['1m0a.doma.bpl']
cr = to_schedule[0]
#print "cr.reservation_list:", cr.reservation_list
