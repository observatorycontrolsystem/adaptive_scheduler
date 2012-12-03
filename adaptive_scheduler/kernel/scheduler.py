#!/usr/bin/env python

'''
Scheduler is the base class for all schedulers. 

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
Dec 2012
'''

from reservation_v3 import *
#from contracts_v2 import *
import copy
from uncontendedscheduler import *

class Scheduler(object):
    
    def __init__(self, compound_reservation_list, 
                 globally_possible_windows_dict, 
                 contractual_obligation_list):
        self.compound_reservation_list   = compound_reservation_list
        self.contractual_obligation_list = contractual_obligation_list
        # globally_possible_windows_dict is a dictionary mapping:
        # resource -> globally possible windows (Intervals) on that resource. 
        self.globally_possible_windows_dict   = globally_possible_windows_dict
        # these dictionaries hold:
        # scheduled reservations
        self.schedule_dict      = {}
        # busy intervals
        self.schedule_dict_busy = {}
        # free intervals
        self.schedule_dict_free = {}

        # resource_list holds the schedulable resources.
        # possible windows specified by reservations may include
        # resources not on this list, but we cannot schedule them because
        # we do not know their globally possible windows.
        self.resource_list = globally_possible_windows_dict.keys()

        for resource in self.resource_list:
            # reservation list
            self.schedule_dict[resource]      = []
            # busy intervals
            self.schedule_dict_busy[resource] = Intervals([], 'busy')
        # free intervals
        self.schedule_dict_free = copy.copy(globally_possible_windows_dict)
        
        self.and_constraints   = []        
        self.oneof_constraints = []
        self.reservation_list  = self.convert_compound_to_simple()
        self.unscheduled_reservation_list = copy.copy(self.reservation_list)


    def order_equals(self, x):
        if (x.order == self.current_order):
            return True
        else:
            return False


    def resources_include(self, x):
        if (self.current_resource in x.free_windows_dict[resource].keys()):
            return True
        else:
            return False


    def get_reservation_by_ID(self, ID):
        for r in self.reservation_list:
            if r.get_ID() == ID:
                return r
        return None


    def check_window_consistency(self):
        # TODO: 
        # checks that all windows requested by reservations are within
        # the global windows of opportunity for their respective resource
        return


    def convert_compound_to_simple(self):
        reservation_list = []
        for cr in self.compound_reservation_list:
            if cr.issingle():
                reservation_list.append(cr.reservation_list[0])
            elif cr.isoneof():
                reservation_list.extend(cr.reservation_list)
                self.oneof_constraints.append(cr.reservation_list)
            elif cr.isand():
                reservation_list.extend(cr.reservation_list)
                # add the constraint to the list of constraints
                self.and_constraints.append(cr.reservation_list)
        return reservation_list


    def commit_reservation_to_schedule(self, r):
        if not r.scheduled:
            print "error: trying to commit unscheduled reservation"
            return
        else:
            interval = Intervals(r.scheduled_timepoints)
            self.schedule_dict[r.scheduled_resource].append(r)

            # add interval & remove free time
            self.schedule_dict_busy[r.scheduled_resource].add(r.scheduled_timepoints)
            self.schedule_dict_free[r.scheduled_resource] = self.schedule_dict_free[r.scheduled_resource].subtract(interval)
            # remove from list of unscheduled reservations
            self.unscheduled_reservation_list.remove(r)
            # TODO? remove scheduled time from free windows of other reservations?


    def uncommit_reservation_from_schedule(self, r):
        resource = r.scheduled_resource
        self.schedule_dict[resource].remove(r)
        # remove interval & add back free time
        self.schedule_dict_free[resource].add(r.scheduled_timepoints)
        self.schedule_dict_busy[resource].subtract(Intervals(r.scheduled_timepoints, 'free'))
        self.unscheduled_reservation_list.append(r)
        r.unschedule()
        # TODO?: add back the window to those reservations that originally
        # included it in their possible_windows list.
        # Not bothering with this now since there is no pass following 
        # this that could benefit from this information. 
