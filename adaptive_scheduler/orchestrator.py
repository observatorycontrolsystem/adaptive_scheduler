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
from datetime import datetime, timedelta

from adaptive_scheduler.model2          import (filter_out_compounds,
                                                 differentiate_by_type, n_requests,
                                                  )
from adaptive_scheduler.kernel_mappings import (construct_visibilities,
                                                 construct_resource_windows,
                                                 make_compound_reservations,
                                                 make_many_type_compound_reservations,
                                                 filter_for_kernel,
                                                 construct_global_availability)
from adaptive_scheduler.printing import (print_schedule, print_compound_reservations,
                                          summarise_urs, log_full_ur, log_windows)
from adaptive_scheduler.printing import plural_str as pl
from schedutils.semester_service import get_semester_code

#from adaptive_scheduler.kernel.fullscheduler_v6 import FullScheduler_v6 as FullScheduler
# from adaptive_scheduler.kernel.fullscheduler_gurobi import FullScheduler_gurobi as FullScheduler
from adaptive_scheduler.request_filters import filter_urs, drop_empty_requests, find_unschedulable_ur_numbers, set_now
from adaptive_scheduler.eventbus        import get_eventbus
from adaptive_scheduler.feedback        import TimingLogger
from adaptive_scheduler.utils import timeit
from adaptive_scheduler.log   import UserRequestLogger
from adaptive_scheduler.event_utils import report_scheduling_outcome

from collections import defaultdict
from adaptive_scheduler.kernel.intervals import Intervals
from timeit import itertools
from reqdb.requests import Request

# Set up and configure a module scope logger, and a UR-specific logger
import logging
log          = logging.getLogger(__name__)
multi_ur_log = logging.getLogger('ur_logger')

ur_log = UserRequestLogger(multi_ur_log)

event_bus = get_eventbus()

# TODO: Refactor - move all these functions to better locations



def get_requests_from_file(req_filename, telescope_class):

    with open(req_filename, 'r') as req_fh:
        req_data = req_fh.read()
        return ast.literal_eval(req_data)


def get_requests_from_json(req_filename, telescope_class):

    with open(req_filename, 'r') as req_fh:
        req_data = req_fh.read()
        return json.loads(req_data)


def write_requests_to_file_as_json(ur_list, filename):
    out_fh = open(filename, 'w')
    json.dump(ur_list, out_fh)
    out_fh.close()


def write_requests_to_file(requests, filename):
    out_fh = open(filename, 'w')
    out_fh.write(str(requests))
    out_fh.close()


def dump_kernel_input(to_schedule, resource_windows, contractual_obligations,
                      time_slicing_dict):
    json_dump = {
                  'to_schedule' : to_schedule,
                  'resource_windows' : resource_windows,
                  'contractual_obligations' : contractual_obligations,
                  'time_slicing_dict' : time_slicing_dict
                }


    kernel_dump_file = 'kernel.dump'
    kernel_dump_fh = open(kernel_dump_file, 'w')
#    kernel_dump_fh.write(jsonpickle.encode(json_dump))
    kernel_dump_fh.close()
    log.info("Wrote kernel input dump to %s", kernel_dump_file)

    return


def dump_kernel_input2(to_schedule, global_windows, contractual_obligations, time_slicing_dict):
    args_filename = 'input_args.%s.tmp' % datetime.utcnow().strftime(format='%Y-%m-%d_%H_%M_%S')

    args_fh = open(args_filename, 'w')
    print "Dumping kernel args to %s" % args_filename

    to_schedule_serial = [x.serialise() for x in to_schedule]
    global_windows_serial = dict([(k, v.serialise()) for k, v in global_windows.items()])

    args_fh.write(json.dumps({
                                     'to_schedule' : to_schedule_serial,
                                     'global_windows' : global_windows_serial,
                                     'contractual_obligations' : contractual_obligations,
                                     'time_slicing_dict' : time_slicing_dict
                                     }))
    args_fh.close()

    return