#!/usr/bin/env python

'''
FullScheduler_v3 class for co-scheduling reservations & contractual obligations
across multiple resources using the Hungarian algorithm.

It handles 'and' compound reservations using constraints, and 'oneof' compound reservations transparently.

Author: Sotiria Lampoudi (slampoud@gmail.com)
June 2012
'''

from reservation_v3 import *
#from contracts_v2 import *
from clustering import *
import copy
from bipartitescheduler import *
from hungarianscheduler import *

class FullScheduler_v3(BipartiteScheduler):


    def schedule_contended_reservations_pass(self, current_order):
        self.current_order = current_order
        reservation_list   = filter(self.order_equals, self.unscheduled_reservation_list)
        reservation_list.sort(reverse=True)
        self.make_free_windows_consistent(reservation_list)
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
#        self.enforce_oneof_constraints()
        self.enforce_and_constraints()
        self.schedule_contractual_obligations()
        
        return self.schedule_dict
