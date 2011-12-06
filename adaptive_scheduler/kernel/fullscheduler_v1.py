#!/usr/bin/env python

'''
FullScheduler_v1 class for co-scheduling reservations & contractual obligations
across multiple resources using bipartite matching. 

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
November 2011
'''

from reservation_v2 import *
from bipartitescheduler import *
from uncontendedscheduler import *
#from contracts_v2 import *
from clustering import *
import copy

class FullScheduler_v1(object):
    
    def __init__(self, compound_reservation_list, 
                 globally_possible_windows_dict, 
                 contractual_obligation_list):
        self.compound_reservation_list   = compound_reservation_list
        self.contractual_obligation_list = contractual_obligation_list
        # globally_possible_windows_dict is a dictionary mapping:
        # resource -> globally possible windows on that resource. 
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
            self.schedule_dict_free[resource] = Intervals(globally_possible_windows_dict[resource], 'free')
        self.constraints      = []        
        self.reservation_list = self.convert_compound_to_simple()
        self.unscheduled_reservation_list = copy.copy(self.reservation_list)
        self.current_order    = 1
        self.current_resource = None


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
            elif cr.isnof():
                for i in range(0,cr.repeats):
                    reservation_list.append(cr.reservation_list[0])
            elif cr.isand():
                reservation_list.extend(cr.reservation_list)
                # add the constraint to the list of constraints
                self.constraints.append(cr.reservation_list)
        return reservation_list


    def cluster_and_order_reservations(self, n=2):
        c = Clustering(self.unscheduled_reservation_list)
        max_order = c.cluster_and_order(n)
        # returns the max order
        return max_order 


    def schedule_uncontended_reservations(self):
        for resource in self.resource_list:
            self.current_resource = resource
            reservation_list = filter(self.resources_include, self.unscheduled_reservation_list)
            us = UncontendedScheduler(reservation_list, resource)
            scheduled_reservations = us.schedule()
            for r in scheduled_reservations:
                self.commit_reservation_to_schedule(r)


    def schedule_contended_reservations(self):
        # first check that the unscheduled reservation list is not empty
        if self.unscheduled_reservation_list:
            # then check that there are enough unscheduled reservations
            # to make it worth it to cluster them. This threshold is 
            # arbitrary and is currently set to: 10
            if len(self.unscheduled_reservation_list) > 10:
                # break reservations into classes according to some criterion
                # find the max order and iterate until max_order
                max_order = self.cluster_and_order_reservations()
            else:
                max_order = 1
            for current_order in range(1, max_order+1):
                # foreach class do:
                self.schedule_contended_reservations_pass(current_order)
        return


    def schedule_contended_reservations_pass(self, current_order):
        self.current_order = current_order
        reservation_list   = filter(self.order_equals, self.unscheduled_reservation_list)
        if len(reservation_list) > 1:
            bs = BipartiteScheduler(reservation_list, self.resource_list)
            scheduled_reservations = bs.schedule()
        else:
            reservation_list[0].schedule_anywhere()
            scheduled_reservations = reservation_list
        for r in scheduled_reservations:
            self.commit_reservation_to_schedule(r)


    def schedule_contractual_obligations(self):
        # TODO
        return


    def order_equals(self, x):
        if (x.order == self.current_order):
            return True
        else:
            return False


    def resources_include(self, x):
        if (self.current_resource in x.possible_windows_dict.keys()):
            return True
        else:
            return False

    
    def commit_reservation_to_schedule(self, r):
        if r.scheduled:
            start    = r.scheduled_start
            resource = r.scheduled_resource
            quantum  = r.scheduled_quantum
            interval = Intervals(r.scheduled_timepoints, 'busy')
            if resource not in self.resource_list:
                print "error: trying to commit reservation on a resource not in the resource list\n"
                return
            self.schedule_dict[resource].append(r)
            # add interval & remove free time
            self.schedule_dict_busy[resource].add(r.scheduled_timepoints)
            self.schedule_dict_free[resource] = self.schedule_dict_free[resource].subtract(interval)
            # remove from list of unscheduled reservations
            self.unscheduled_reservation_list.remove(r)
            # remove scheduled time from free windows of other reservations
            self.current_resource = resource
            reservation_list = filter(self.resources_include, self.reservation_list)
            for reservation in reservation_list:
                if r == reservation:
                    continue
                else:
                    reservation.remove_from_free_windows(resource, interval)
        else:
            print "error: trying to commit unscheduled reservation"


    def uncommit_reservation_from_schedule(self, r):
        resource = r.scheduled_resource
        self.schedule_dict[resource].remove(r)
        # remove interval & add back free time
        self.schedule_dict_free[resource].add(r.scheduled_timepoints)
        self.schedule_dict_busy[resource].subtract(Intervals(r.scheduled_timepoints, 'free'))
        self.unscheduled_reservation_list.append(r)
        r.unschedule()
        # TODO: add back the window to those reservations that originally
        # included it in their possible_windows list.
        # Not bothering with this now since there is no pass following 
        # this that could benefit from this information. 
        

    def enforce_all_constraints(self):
        for c in self.constraints:
            counter = 0
            size    = len(c)
            for r in c:
                if r.scheduled:
                    counter += 1
            if counter > 0 and counter != size:
                # we can either just return False, or fix it
                # by removing the scheduled res's, and then
                # return. Here we fix it first
                for r in c:
                    if r.scheduled:
                        self.uncommit_reservation_from_schedule(r)
              

    def schedule_all(self):
        self.schedule_uncontended_reservations()
        self.schedule_contended_reservations()
        self.enforce_all_constraints()
        self.schedule_contractual_obligations()
        
        return self.schedule_dict
