#!/usr/bin/env python

'''
FullScheduler_v4 class for co-scheduling reservations 
across multiple resources using time-slicing and an integer program.

Because time is discretized into time slices, this scheduler requires
information about how to generate the slices, so its signature has one
more argument than usual. 

This implementation uses a dense matrix representation. 

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
August 2012
Dec 2012: changed to work with Reservation_v3
'''

from reservation_v3 import *
#from contracts_v2 import *
import copy
import numpy
from openopt import LP

class FullScheduler_v4(object):
    
    def __init__(self, compound_reservation_list, 
                 globally_possible_windows_dict, 
                 contractual_obligation_list, 
                 time_slicing_dict):
        self.compound_reservation_list   = compound_reservation_list
        self.contractual_obligation_list = contractual_obligation_list
        # globally_possible_windows_dict is a dictionary mapping:
        # resource -> globally possible windows (Intervals) on that resource. 
        self.globally_possible_windows_dict   = globally_possible_windows_dict
        # time_slicing_dict is a dictionary that maps: 
        # resource-> [slice_alignment, slice_length]
        self.time_slicing_dict = time_slicing_dict

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
#        self.unscheduled_reservation_list = copy.copy(self.reservation_list)

        # these are the structures we need for the linear programming solver
        self.Yik = [] # maps idx -> [resID, window idx, priority, resource]
        self.aikt = {} # maps slice -> Yik idxs


    def get_reservation_by_ID(self, ID):
        for r in self.reservation_list:
            if r.get_ID() == ID:
                return r
        return None


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
        # first we need to build up the list of discretized slices that each
        # reservation can begin in. These are represented as attributes
        # that get attached to the reservation object.
        # The new attributes are:
        # slices_dict
        # internal_starts_dict
        # and the dicts are keyed by resource.
        # the description of slices and internal starts is in intervals.py
        for r in self.reservation_list:
            r.Yik_entries = []
            r.slices_dict = {}
            r.internal_starts_dict = {}
            for resource in r.free_windows_dict.keys():
                slice_alignment = self.time_slicing_dict[resource][0]
                slice_length = self.time_slicing_dict[resource][1]
                r.slices_dict[resource], r.internal_starts_dict[resource] = r.free_windows_dict[resource].get_slices(slice_alignment, slice_length, r.duration)
                w_idx = 0
                for w in r.slices_dict[resource]:
                    Yik_idx = len(self.Yik)
                    r.Yik_entries.append(Yik_idx)
                    self.Yik.append([r.resID, w_idx, r.priority, resource])
                    w_idx += 1
                    for s in w:
                        # build aikt
                        # working with slice: (s,resource)
                        key = self.hash_slice(s, resource, slice_length)
                        if key in self.aikt.keys():
                            self.aikt[key].append(Yik_idx)
                        else:
                            self.aikt[key] = [Yik_idx]
        # allocate A & b
        # find the row size of A:
        # first find the number of reservations participating in oneofs
        oneof_reservation_num = 0
        for c in self.oneof_constraints:
            oneof_reservation_num += len(c)
        A_rows = len(self.reservation_list) + len(self.aikt) + len(self.oneof_constraints) - oneof_reservation_num
        try:
            A = numpy.zeros((A_rows, len(self.Yik)), dtype=numpy.int)
        except ValueError:
            print "Number of A rows: ", A_rows
        b = numpy.zeros(A_rows, dtype=numpy.int)
        # build A & b
        row = 0

        # constraint 5: oneof
        for c in self.oneof_constraints:
            for r in c:
                for entry in r.Yik_entries:
                    A[row,entry] = 1
                r.skip_constraint2 = True
            b[row] = 1
            row += 1    

        # constraint 2: each res should have one start:
        # optimization: 
        # if the reservation participates in a oneof, then this is 
        # redundant with the oneof constraint added above, so don't add it.
        for r in self.reservation_list:
            if hasattr(r, 'skip_constraint2'):
                continue
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
        
        # constraint 6: and       
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
#        r = p.minimize('pclp') 
        r = p.minimize('glpk')
#        r = p.minimize('lpsolve')

#        print r.xf
        idx = 0
        for value in r.xf:
            if value == 1:
                resID = self.Yik[idx][0]
                slice_idx = self.Yik[idx][1]
                resource = self.Yik[idx][3]
                reservation = self.get_reservation_by_ID(resID)
                # use the internal_start for the start
                start = reservation.internal_starts_dict[resource][slice_idx]
                # the quantum is the length of all the slices we've occupied
                quantum = reservation.slices_dict[resource][slice_idx][-1] + self.time_slicing_dict[resource][1] - reservation.slices_dict[resource][slice_idx][0]
                reservation.scheduled = True
                self.commit_reservation_to_schedule(reservation, start, quantum, resource)
            idx += 1
        return self.schedule_dict


    def commit_reservation_to_schedule(self, r, start, quantum, resource):
        if r.scheduled:
            r.schedule(start, quantum, resource, 
                       [Timepoint(start, 'start'),
                        Timepoint(start + r.duration, 'end')], 
                       'slicedIPdense')
        else:
            print "error: trying to commit unscheduled reservation"
        self.schedule_dict[resource].append(r)
        # remove from list of unscheduled reservations
#        self.unscheduled_reservation_list.remove(r)

        # interval = Intervals(r.scheduled_timepoints, 'busy')
        # # add interval & remove free time
        # self.schedule_dict_busy[r.resource].add(r.scheduled_timepoints)
        # self.schedule_dict_free[r.resource] = self.schedule_dict_free[r.resource].subtract(interval)
        # # remove scheduled time from free windows of other reservations
        # self.current_resource = resource
        # reservation_list = filter(self.resource_equals, self.reservation_list)
        # for reservation in reservation_list:
        #     if r == reservation:
        #         continue
        #     else:
        #         reservation.remove_from_free_windows(interval)


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
        

