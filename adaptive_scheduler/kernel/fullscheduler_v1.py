#!/usr/bin/env python

'''
FullScheduler_v1 class for co-scheduling reservations & contractual obligations
across multiple resources using bipartite matching. 

It handles 'and' and 'oneof' compound reservations using constraints.

Author: Sotiria Lampoudi (slampoud@gmail.com)
November 2011
'''

from reservation_v3 import *
#from contracts_v2 import *
from clustering import *
import copy
from bipartitescheduler import *
from hopcroftkarpscheduler import *

class FullScheduler_v1(BipartiteScheduler):
    def schedule_contended_reservations_pass(self, current_order):
        self.current_order = current_order
        reservation_list   = filter(self.order_equals, self.unscheduled_reservation_list)
        # make sure reservations know about previous passes
        self.make_free_windows_consistent(reservation_list)
        if len(reservation_list) > 1:
            bs = HopcroftKarpScheduler(reservation_list, self.resource_list)
            scheduled_reservations = bs.schedule()
        else:
            if reservation_list[0].schedule_anywhere():
                scheduled_reservations = reservation_list
        if scheduled_reservations:
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
