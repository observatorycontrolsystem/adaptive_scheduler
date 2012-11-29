#!/usr/bin/env python

'''
FullScheduler_v2 class for co-scheduling reservations & contractual obligations
across multiple resources using bipartite matching. 

It handles 'and' compound reservations using constraints, and 'oneof' compound reservations transparently.

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
November 2011
'''

from reservation_v2 import *
from bipartitescheduler import *
from uncontendedscheduler import *
#from contracts_v2 import *
from clustering import *
import copy

class FullScheduler_v2(object):
    
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
        self.current_order     = 1
        self.current_resource  = None


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


    def cluster_and_order_reservations(self, n=1):
        c = Clustering(self.unscheduled_reservation_list)
        max_order = c.cluster_and_order(n)
        # returns the max order
        return max_order 


    def schedule_uncontended_reservations(self):
        for resource in self.resource_list:
            self.current_resource = resource
            reservation_list = filter(self.resource_equals, self.unscheduled_reservation_list)
            us = UncontendedScheduler(reservation_list, resource)
            scheduled_reservations = us.schedule()
            # check for intersection w/ oneof constraints
            constraints_to_remove = []
            for constraint in self.oneof_constraints:
                intersection = list(set(constraint) & set(scheduled_reservations))
                if len(intersection) > 1:
                    # oops! we scheduled too many
                    # since they were uncontended, we can arbitrarily drop some
                    for r in intersection[1:]:
                        scheduled_reservations.remove(r)
                        r.unschedule()
                    # also remove the constraint, since it's satisfied
                    constraints_to_remove.append(constraint)
                elif len(intersection) == 1:
                    # we only scheduled one, so remove the constraint.
                    # also remove the remaining res.'s on this constraint
                    # from the unschedule_reservations list
                    for r in constraint:
                        if r == intersection[0]:
                            pass
                        else:
                            self.unscheduled_reservation_list.remove(r)
                    constraints_to_remove.append(constraint)
            for constraint in constraints_to_remove:
                try:
                    self.oneof_constraints.remove(constraint)
                except ValueError:
                    pass
            # commit the reservations to the schedule
            for r in scheduled_reservations:
                self.commit_reservation_to_schedule(r)


    def schedule_contended_reservations(self):
        # first check that the unscheduled reservation list is not empty
        if self.unscheduled_reservation_list:
            # then check that there are enough unscheduled reservations
            # to make it worth it to cluster them. This threshold is 
            # arbitrary and is currently set to: 10
            if len(self.unscheduled_reservation_list) > 2:
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
        reservation_list.sort(reverse=True)
        scheduled_reservations = []
        if len(reservation_list) > 1:
            bs = BipartiteScheduler(reservation_list, self.resource_list)
            # handle oneofs
            for constraint in self.oneof_constraints:
                intersection = list(set(constraint).intersection(reservation_list))
                if len(intersection) > 1:
                    residtokeep = intersection[0].get_ID()
                    for restomerge in intersection[1:]:
                        bs.merge_constraints(residtokeep, restomerge.get_ID())
            scheduled_reservations = bs.schedule()
        elif len(reservation_list) == 1:
            if reservation_list[0].schedule_anywhere():
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


    def resource_equals(self, x):
        if (self.current_resource == x.resource):
            return True
        else:
            return False

    
    def commit_reservation_to_schedule(self, r):
        if r.scheduled:
            start    = r.scheduled_start
            quantum  = r.scheduled_quantum
            resource = r.scheduled_resource
            interval = Intervals(r.scheduled_timepoints, 'busy')
            self.schedule_dict[resource].append(r)
            # add interval & remove free time
            self.schedule_dict_busy[resource].add(r.scheduled_timepoints)
            self.schedule_dict_free[resource] = self.schedule_dict_free[resource].subtract(interval)
            # remove from list of unscheduled reservations
            self.unscheduled_reservation_list.remove(r)
            # remove scheduled time from free windows of other reservations
            self.current_resource = resource
            reservation_list = filter(self.resource_equals, self.reservation_list)
            for reservation in reservation_list:
                if r == reservation:
                    continue
                else:
                    reservation.remove_from_free_windows(interval, resource)
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
        

    def enforce_oneof_constraints(self):
        for c in self.oneof_constraints:
            counter = 0
            size    = len(c)
            for r in c:
                if r.scheduled:
                    counter += 1
                if counter > 1:
                    self.uncommit_reservation_from_schedule(r)


    def enforce_and_constraints(self):
        for c in self.and_constraints:
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
        self.enforce_oneof_constraints()
        self.enforce_and_constraints()
        self.schedule_contractual_obligations()
        
        return self.schedule_dict
