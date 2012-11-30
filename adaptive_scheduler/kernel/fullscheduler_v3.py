#!/usr/bin/env python

'''
FullScheduler_v3 class for co-scheduling reservations & contractual obligations
across multiple resources using the Hungarian algorithm.

It handles 'and' compound reservations using constraints, and 'oneof' compound reservations transparently.

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
June 2012
'''

from reservation_v3 import *
from hungarianscheduler import *
from uncontendedscheduler import *
#from contracts_v2 import *
from clustering import *
import copy
from scheduler import *

class FullScheduler_v3(Scheduler):
    def __init__(self, compound_reservation_list, 
                 globally_possible_windows_dict, 
                 contractual_obligation_list):
        Scheduler.__init__(self, compound_reservation_list, 
                           globally_possible_windows_dict, 
                           contractual_obligation_list)
        self.current_order     = 1
        self.current_resource  = None


    def cluster_and_order_reservations(self, n=1):
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
            bs = HungarianScheduler(reservation_list, self.resource_list)
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


    def schedule_all(self):
        self.schedule_uncontended_reservations()
        self.schedule_contended_reservations()
        self.enforce_oneof_constraints()
        self.enforce_and_constraints()
        self.schedule_contractual_obligations()
        
        return self.schedule_dict
