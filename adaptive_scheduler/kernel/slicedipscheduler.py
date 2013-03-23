#!/usr/bin/env python

'''
SlicedIPScheduler class for co-scheduling reservations 
across multiple resources using time-slicing and an integer program.

Because time is discretized into time slices, this scheduler requires
information about how to generate the slices, so its signature has one
more argument than usual. 

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
Sept 2012
Dec 2012: changed to work with Reservation_v3
'''

from reservation_v3 import *
#from contracts_v2 import *
import copy
import numpy
from scheduler import *

class SlicedIPScheduler(Scheduler):
    
    def __init__(self, compound_reservation_list, 
                 globally_possible_windows_dict, 
                 contractual_obligation_list, 
                 time_slicing_dict):
        Scheduler.__init__(self, compound_reservation_list, 
                 globally_possible_windows_dict, 
                 contractual_obligation_list)
        # time_slicing_dict is a dictionary that maps: 
        # resource-> [slice_alignment, slice_length]
        self.time_slicing_dict = time_slicing_dict
        # these are the structures we need for the linear programming solver
        self.Yik = [] # maps idx -> [resID, window idx, priority, resource]
        self.aikt = {} # maps slice -> Yik idxs
	self.schedulerIDstring = 'slicedIPscheduler'


    def hash_slice(self, start, resource, slice_length):
        return "resource_"+resource+"_start_"+repr(start)+"_length_"+repr(slice_length)
        

    def unhash_slice(self, mystr):
        l = mystr.split("_")
        return [l[1], int(l[3]), int(l[5])]


    def build_data_structures(self):
        # first we need to build up the list of discretized slices that each
        # reservation can begin in. These are represented as attributes
        # that get attached to the reservation object. 
        # The new attributes are: 
        # slices_dict
        # internal_starts_dict
        # and the dicts are keyed by resource. 
        # the description of slices and internal starts is in intervals.py
        for r in self.reservation_list:
            # there is no longer a one-to-one mapping between reservations 
            # and resources, so we need to iterate over each resource
            r.Yik_entries = []
            r.slices_dict = {}
            r.internal_starts_dict = {}
            for resource in r.free_windows_dict.keys():
                slice_alignment = self.time_slicing_dict[resource][0]
                slice_length = self.time_slicing_dict[resource][1]
                r.slices_dict[resource], r.internal_starts_dict[resource] = self.get_slices( r.free_windows_dict[resource], slice_alignment, slice_length, r.duration)
#                print r.resID, resource, r.slices_dict[resource], r.internal_starts_dict[resource]
                w_idx = 0
                for w in r.slices_dict[resource]:
                    Yik_idx = len(self.Yik)
                    r.Yik_entries.append(Yik_idx)
                    self.Yik.append([r.resID, w_idx, r.priority, resource])
                    w_idx += 1
                    for s in w:
                        # build aikt
                        # working with slice: (s,r.resource)
                        key = self.hash_slice(s, resource, slice_length)
                        if key in self.aikt.keys():
                            self.aikt[key].append(Yik_idx)
                        else:
                            self.aikt[key] = [Yik_idx]


    def unpack_result(self, r):
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
                reservation.schedule(start, quantum, resource, self.schedulerIDstring)
                self.commit_reservation_to_schedule(reservation)
            idx += 1
        return self.schedule_dict
        

    def get_slices(self, intervals, slice_alignment, slice_length, duration):
        ''' Returns two things: 
        * slices: list of lists. Each inner list is a window. The first 
        element is the initial slice, and each subsequent slice is also
        occupied. All slices are aligned with slice_alignment, and are 
        slice_length long.
        * internal_starts: list of values, one per inner list of slices. 
        Each internal_start can be either equal to the corresponding 
        slices[0], in which case it's not internal, or > than it, being 
        internal.'''
        slices = []
        internal_starts = []
        intervals.timepoints.sort()
        for t in intervals.timepoints:
            if t.type == 'start':
                if t.time <= slice_alignment:
                    start = slice_alignment
                    internal_start = slice_alignment
                else:
                    # figure out start so it aligns with slice_alignment 
                    start = int(slice_alignment + math.floor(float(t.time - slice_alignment)/float(slice_length))*slice_length)
                    # use the actual start as an internal start (may or may not align w/ slice_alignment)
                    internal_start = t.time
                end_time = internal_start + duration
            elif t.type == 'end': 
                if t.time < slice_alignment:
                    continue
                while t.time - start >= duration:
                    tmp = range(start, internal_start+duration, slice_length)
                    slices.append(tmp)
                    internal_starts.append(internal_start)
                    start += slice_length
                    internal_start = start
        return slices, internal_starts
