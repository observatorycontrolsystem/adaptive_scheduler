#!/usr/bin/env python

'''
FullScheduler_v1 class for co-scheduling reservations & contractual obligations
across multiple resources using bipartite matching. 

It handles 'and' and 'oneof' compound reservations using constraints.

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
November 2011
'''

from reservation_v3 import *
from bipartitescheduler import *
from uncontendedscheduler import *
#from contracts_v2 import *
from clustering import *
import copy
from scheduler import *

class FullScheduler_v1(Scheduler):
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


    def schedule_contended_reservations_pass(self, current_order):
        self.current_order = current_order
        reservation_list   = filter(self.order_equals, self.unscheduled_reservation_list)
        if len(reservation_list) > 1:
            bs = BipartiteScheduler(reservation_list, self.resource_list)
            scheduled_reservations = bs.schedule()
        else:
            if reservation_list[0].schedule_anywhere():
                scheduled_reservations = reservation_list
        for r in scheduled_reservations:
            self.commit_reservation_to_schedule(r)


    def schedule_contractual_obligations(self):
        # TODO
        return


    def schedule_all(self):
        self.schedule_uncontended_reservations()
        self.schedule_contended_reservations()
        self.enforce_and_constraints()
        self.enforce_oneof_constraints()
        self.schedule_contractual_obligations()
        
        return self.schedule_dict
