#!/usr/bin/env python

'''
FullScheduler_v4 class for co-scheduling reservations 
across multiple resources using a discretized integer program.

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
August 2012
'''

from reservation_v2 import *
#from contracts_v2 import *
import copy
import numpy
from openopt import LP

class FullScheduler_v4(object):
    
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
        self.Yik = [] # maps idx -> [resID, window idx]
        self.aikt = {} # maps slice -> Yik idxs


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

    
    def hash_slice(self, start, resource, slice_length):
        return "resource_"+resource+"_start_"+repr(start)+"_length_"+repr(slice_length)
        

    def unhash_slice(self, mystr):
        l = mystr.split("_")
        return [l[1], int(l[3]), int(l[5])]


    def schedule_all(self):
        #TODO: where do slice_alignment and length (per resource) come from?
        slice_alignment = 0
        slice_length = 1
        for r in self.reservation_list:
            r.slices = r.free_windows.get_slices(slice_alignment, slice_length, r.duration)
            r.Yik_entries = []
            w_idx = 0
            for w in r.slices:
                Yik_idx = len(self.Yik)
                r.Yik_entries.append(Yik_idx)
                self.Yik.append([r.resID, w_idx, r.priority])
                w_idx += 1
                for s in w:
                    # build aikt
                    # working with slice: (s,r.resource)
                    key = self.hash_slice(s, r.resource, slice_length)
                    if key in self.aikt.keys():
                        self.aikt[key].append(Yik_idx)
                    else:
                        self.aikt[key] = [Yik_idx]
        # allocate A & b
        # find the row size of A:
        A_rows = len(self.reservation_list) + len(self.aikt) + len(self.oneof_constraints)
        A = numpy.zeros((A_rows, len(self.Yik)), dtype=numpy.int)
        b = numpy.zeros(A_rows, dtype=numpy.int)
        # build A & b
        # constraint 2: each res should have one start:
        row = 0
        for r in self.reservation_list:
            for entry in r.Yik_entries:
                A[row,entry] = 1
            b[row] = 1
            row += 1

        # constraint 3: each slice should only have one sched. reservation:
        for s in self.aikt.keys():
            for entry in self.aikt[s]:
                A[row,entry] = 1
            b[row] = 1
            row += 1

        # constraint 5: oneof
        for c in self.oneof_constraints:
            for r in c:
                for entry in r.Yik_entries:
                    A[row,entry] = 1
            b[row] = 1
            row += 1    
        
        # constraint 6: and       
#        Aeq = None
#        beq = None
        # figure out size of constraint matrix
        Aeq_rows = 0
        for c in self.and_constraints:
            Aeq_rows += len(c)-1
        # allocate Aeq and beq
        Aeq = numpy.zeros((Aeq_rows, len(self.Yik)), dtype=numpy.int)
        beq = numpy.zeros(Aeq_rows, dtype=numpy.int)
        row = 0
        for c in self.and_constraints:
            constraint_size = len(c)
            left_idx = 0
            right_idx = 1
            while right_idx < constraint_size:
                left_r = c[left_idx]
                right_r = c[right_idx]
                for entry in left_r.Yik_entries:
                    Aeq[row, entry] = 1
                for entry in right_r.Yik_entries:
                    Aeq[row, entry] = -1
                left_idx += 1
                right_idx += 1
                row += 1

        # bounds:
        lb = numpy.zeros(len(self.Yik), dtype=numpy.int)
        ub = numpy.ones(len(self.Yik), dtype=numpy.int)

        # objective function:
        f = numpy.zeros(len(self.Yik))
        row = 0
        for entry in self.Yik:
            f[row] = - entry[2] #priority
            row += 1
        p = LP(f=f, A=A, Aeq=Aeq, b=b, beq=beq, lb=lb, ub=ub)
        r = p.minimize('pclp') # or 'glpk' or 'lpsolve'
        print r.xf


    # def commit_reservation_to_schedule(self, r):
    #     if r.scheduled:
    #         start    = r.scheduled_start
    #         quantum  = r.scheduled_quantum
    #         resource = r.scheduled_resource
    #         interval = Intervals(r.scheduled_timepoints, 'busy')
    #         self.schedule_dict[resource].append(r)
    #         # add interval & remove free time
    #         self.schedule_dict_busy[resource].add(r.scheduled_timepoints)
    #         self.schedule_dict_free[resource] = self.schedule_dict_free[resource].subtract(interval)
    #         # remove from list of unscheduled reservations
    #         self.unscheduled_reservation_list.remove(r)
    #         # remove scheduled time from free windows of other reservations
    #         self.current_resource = resource
    #         reservation_list = filter(self.resource_equals, self.reservation_list)
    #         for reservation in reservation_list:
    #             if r == reservation:
    #                 continue
    #             else:
    #                 reservation.remove_from_free_windows(interval)
    #     else:
    #         print "error: trying to commit unscheduled reservation"


    # def uncommit_reservation_from_schedule(self, r):
    #     resource = r.scheduled_resource
    #     self.schedule_dict[resource].remove(r)
    #     # remove interval & add back free time
    #     self.schedule_dict_free[resource].add(r.scheduled_timepoints)
    #     self.schedule_dict_busy[resource].subtract(Intervals(r.scheduled_timepoints, 'free'))
    #     self.unscheduled_reservation_list.append(r)
    #     r.unschedule()
    #     # TODO: add back the window to those reservations that originally
    #     # included it in their possible_windows list.
    #     # Not bothering with this now since there is no pass following 
    #     # this that could benefit from this information. 
        

    # def enforce_oneof_constraints(self):
    #     for c in self.oneof_constraints:
    #         counter = 0
    #         size    = len(c)
    #         for r in c:
    #             if r.scheduled:
    #                 counter += 1
    #             if counter > 1:
    #                 self.uncommit_reservation_from_schedule(r)


    # def enforce_and_constraints(self):
    #     for c in self.and_constraints:
    #         counter = 0
    #         size    = len(c)
    #         for r in c:
    #             if r.scheduled:
    #                 counter += 1
    #         if counter > 0 and counter != size:
    #             # we can either just return False, or fix it
    #             # by removing the scheduled res's, and then
    #             # return. Here we fix it first
    #             for r in c:
    #                 if r.scheduled:
    #                     self.uncommit_reservation_from_schedule(r)
              

    # def schedule_all(self):
    #     self.schedule_uncontended_reservations()
    #     self.schedule_contended_reservations()
    #     self.enforce_oneof_constraints()
    #     self.enforce_and_constraints()
    #     self.schedule_contractual_obligations()
        
    #     return self.schedule_dict
