#!/usr/bin/env python

'''
SlicedIPScheduler class for co-scheduling reservations 
across multiple resources using time-slicing and an integer program.

Because time is discretized into time slices, this scheduler requires
information about how to generate the slices, so its signature has one
more argument than usual. 

Author: Sotiria Lampoudi (slampoud@gmail.com)
Sept 2012
Dec 2012: changed to work with Reservation_v3
'''

from reservation_v3 import *
#from contracts_v2 import *
import copy
import numpy
from scheduler import *

class PossibleStart(object):
    def __init__(self, resource, slice_starts, internal_start):
        self.resource = resource
        self.first_slice_start = slice_starts[0]
        self.all_slice_starts = slice_starts
        self.internal_start = internal_start

    def __lt__(self, other):
        return self.first_slice_start < other.first_slice_start

    def __eq__(self, other):
        return self.first_slice_start == self.first_slice_start

    def __gt__(self, other):
        return self.first_slice_start > self.first_slice_start


class SlicedIPScheduler_v2(Scheduler):
    
    def __init__(self, compound_reservation_list, 
                 globally_possible_windows_dict, 
                 contractual_obligation_list,
                 slice_size_seconds):
        Scheduler.__init__(self, compound_reservation_list, 
                 globally_possible_windows_dict, 
                 contractual_obligation_list)
        # time_slicing_dict is a dictionary that maps: 
        # resource-> [slice_alignment, slice_length]
#         self.resource_list = resource_list
        self.slice_size_seconds = slice_size_seconds
        self.time_slicing_dict = {}
        # these are the structures we need for the linear programming solver
        self.Yik = [] # maps idx -> [resID, window idx, priority, resource]
        self.aikt = {} # maps slice -> Yik idxs
        self.schedulerIDstring = 'slicedIPscheduler'
        self.hashes = set()
        
        for r in self.resource_list:
            self.time_slicing_dict[r] = [0, self.slice_size_seconds]


    def hash_slice(self, start, resource, slice_length):
        string = "resource_"+resource+"_start_"+repr(start)+"_length_"+repr(slice_length)
        exists = string in self.hashes
        self.hashes.add(string)
        return string, exists
        

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
            # get the previous reservation for this reservations request if it exists
            previous_res = r.request.scheduled_reservation
            r.Yik_entries = []
            r.possible_starts = []
            for resource in r.free_windows_dict.keys():
                r.possible_starts.extend(self.get_slices( r.free_windows_dict[resource], resource, r.duration))
            # reorder PossibleStarts
            r.possible_starts.sort()
            # build aikt
            w_idx = 0
            for ps in r.possible_starts:
                Yik_idx = len(self.Yik)
                r.Yik_entries.append(Yik_idx)
                # set the initial warm start solution
                scheduled = 0
                if previous_res and previous_res.scheduled_start == ps.internal_start and previous_res.scheduled_resource == ps.resource:
                    scheduled = 1
                # now w_idx is the index into r.possible_starts, which have
                # been reordered by time.
                self.Yik.append([r.resID, w_idx, r.priority, ps.resource, scheduled])
                w_idx += 1
                # build aikt
                for s in ps.all_slice_starts:
                    key, exists = self.hash_slice(s, ps.resource, self.time_slicing_dict[ps.resource][1])
    #                        if key in self.aikt:
                    if exists:
                        self.aikt[key].append(Yik_idx)
                    else:
                        self.aikt[key] = [Yik_idx]


    def unpack_result(self, r):
        #        print r.xf
        idx = 0
        for value in r.xf:
            if value == 1:
                resID = self.Yik[idx][0]
                start_idx = self.Yik[idx][1]
                resource = self.Yik[idx][3]
                reservation = self.get_reservation_by_ID(resID)
                # use the internal_start for the start  
                start = reservation.possible_starts[start_idx].internal_start
                # the quantum is the length of all the slices we've occupied
                quantum = reservation.possible_starts[start_idx].all_slice_starts[-1] + self.time_slicing_dict[resource][1] - reservation.possible_starts[start_idx].first_slice_start
                reservation.schedule(start, quantum, resource, self.schedulerIDstring)
                self.commit_reservation_to_schedule(reservation)
            idx += 1
        return self.schedule_dict
        

    def get_slices(self, intervals, resource, duration):
        ''' Creates two things: 
        * slices: list of lists. Each inner list is a window. The first 
        element is the initial slice, and each subsequent slice is also
        occupied. All slices are aligned with slice_alignment, and are 
        slice_length long.
        * internal_starts: list of values, one per inner list of slices. 
        Each internal_start can be either equal to the corresponding 
        slices[0], in which case it's not internal, or > than it, being 
        internal.
        Returns: a list of PossibleStart objects'''

        ps_list = []
        # Make sure the resource is available.  If it is not in the time slicing dict, it's not available
        if self.time_slicing_dict.has_key(resource):
            slice_alignment = self.time_slicing_dict[resource][0]
            slice_length = self.time_slicing_dict[resource][1]
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
            
            # return slices, internal_starts
            ps_list = []
            idx = 0
            for w in slices:
                ps_list.append(PossibleStart(resource, w, internal_starts[idx]))
                idx += 1
            
        return ps_list
