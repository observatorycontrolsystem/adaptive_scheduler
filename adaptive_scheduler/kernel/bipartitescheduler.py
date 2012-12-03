#!/usr/bin/env python

'''
BipartiteScheduler is the parent class for the unweighted (Hopcroft-Karp)
and weighted (Hungarian) bipartite matching-based schedulers. 

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
Dec 2012
'''

from reservation_v3 import *
from uncontendedscheduler import *
#from contracts_v2 import *
from clustering import *
import copy
from scheduler import *

class BipartiteScheduler(Scheduler):
    def __init__(self, compound_reservation_list, 
                 globally_possible_windows_dict, 
                 contractual_obligation_list):
        Scheduler.__init__(self, compound_reservation_list, 
                           globally_possible_windows_dict, 
                           contractual_obligation_list)
        self.current_order     = 1
        self.current_resource  = None


    def cluster_and_order_reservations(self, n=2):
        c = Clustering(self.unscheduled_reservation_list)
        max_order = c.cluster_and_order(n)
        # returns the max order
        return max_order 


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


    def schedule_contractual_obligations(self):
        # TODO
        return


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


    def schedule_uncontended_reservations(self):
        for resource in self.resource_list:
            us = UncontendedScheduler(self.reservation_list, resource)
            scheduled_reservations = us.schedule()
            for r in scheduled_reservations:
                self.commit_reservation_to_schedule(r)
