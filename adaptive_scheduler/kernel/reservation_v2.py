#!/usr/bin/env python

'''
Reservation_v2 and CompoundReservation_v2 classes for scheduling.

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
November 2011
'''

from timepoint import *
from intervals import *
import copy

class Reservation_v2(object):

    resID = 0

    def __init__(self, priority, duration, tp_list):
        self.priority = priority
        self.duration = duration
        # possible_windows_dict is a dictionary mapping
        # resource -> intervals
        self.possible_windows_dict = self.populate_possible_windows_dict(tp_list)
        # free_windows_dict keeps track of which of the possible_windows 
        # are free.
        self.free_windows_dict    = copy.copy(self.possible_windows_dict)
        # clean up free windows by removing ones that are too small:
#        for resource in self.free_windows_dict.keys():
#            self.clean_up_free_windows(resource)
        Reservation_v2.resID     += 1
        self.resID                = Reservation_v2.resID
        # these fields are defined when the reservation is ultimately scheduled
        self.scheduled_start      = None
        self.scheduled_resource   = None
        self.scheduled_quantum    = None
        self.scheduled            = False
        self.scheduled_timepoints = None
        # order is the parameter used for grouping & ordering in scheduling
        self.order                = 1


    def schedule_anywhere(self):
        # find the first available spot & stick it there
        for resource in self.free_windows_dict.keys():
            start = self.free_windows_dict[resource].find_interval_of_length(self.duration)
            if start >= 0:
                break
        self.schedule(start, resource, self.duration)

    
    def schedule(self, start, resource, quantum):
        self.scheduled_start    = start
        self.scheduled_resource = resource
        self.scheduled_quantum  = quantum
        self.scheduled          = True
        self.scheduled_timepoints = [Timepoint(start, 'start'), 
        Timepoint(start+quantum, 'end')]


    def unschedule(self):
        self.scheduled_start    = None
        self.scheduled_resource = None
        self.scheduled_quantum  = None
        self.scheduled          = False
        self.scheduled_timepoints = None


    def __lt__(self, other):
        return self.priority < other.priority

    
    def get_ID(self):
        return self.resID


    def populate_possible_windows_dict(self, tp_list):
        tmpdict = {}
        for tp in tp_list:
            if tp.resource in tmpdict:
                tmpdict[tp.resource].append(tp)
            else: 
                tmpdict[tp.resource] = [tp]
        for key in tmpdict.keys():
            tmpdict[key] = Intervals(tmpdict[key])
        return tmpdict


    def remove_from_free_windows(self, resource, interval):
        self.free_windows_dict[resource] = self.free_windows_dict[resource].subtract(interval)
        self.clean_up_free_windows(resource)

        
    def clean_up_free_windows(self, resource):
        self.free_windows_dict[resource].remove_intervals_smaller_than(self.duration)
        if self.free_windows_dict[resource].is_empty():
            del self.free_windows_dict[resource]


class CompoundReservation_v2(object):

    def __init__(self, reservation_list, type='single', repeats=1):
        self.reservation_list = reservation_list
        self.type = type
        # allowed types are:
        # single
        # nof
        # and
        self.size       = len(reservation_list)
        self.repeats    = repeats
        if type == 'single' and self.size > 1:
            msg = ( "Initializing a CompoundReservation as 'single' but with %d "
                    "individual reservations. Ignoring all but the first."
                    % self.size )
            print msg
            self.size = 1
            self.reservation_list = [reservation_list.pop(0)]
        if type == 'and' and self.repeats > 1:
            msg = ("Initializing a CompoundReservation as 'and' but with repeats=%d"
                    ". Resetting repeats to 1."
                    % self.repeats )
            print msg
            self.repeats = 1


    def issingle(self):
        if self.type == "single":
            return True
        else:
            return False


    def isnof(self):
        if self.type == "nof":
            return True
        else:
            return False


    def isand(self):
        if self.type == "and":
            return True
        else:
            return False

