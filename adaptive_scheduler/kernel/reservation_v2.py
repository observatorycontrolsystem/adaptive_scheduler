#!/usr/bin/env python

'''
Reservation_v2 and CompoundReservation_v2 classes for scheduling.

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
November 2011
'''

from timepoint import *
from intervals import *

class Slot(object):

    def __init__(self, start, length, resource):
        self.start      = start
        self.end        = start + length
        self.length     = length
        self.resource   = resource
        self.timepoints = [Timepoint(start, 'start'), Timepoint(start+length, 'end')]


class Reservation_v2(object):

    resID = 0

    def __init__(self, priority, duration, slots_arg):
        self.priority = priority
        self.duration = duration
        self.slots    = []
        # check that at least one slot is >= duration
        # & drop slots < duration
        for slot in slots_arg:
            if slot.length >= duration:
                self.slots.append(slot)
        if len(self.slots) == 0:
            print "error: reservation with no feasible slots\n"
        # possible_windows_dict is a dictionary mapping
        # resource -> intervals
        self.possible_windows_dict     = self.convert_slots_to_intervals()
        # free_windows_dict keeps track of which of the possible_windows 
        # are free
        self.free_windows_dict         = self.possible_windows_dict
        Reservation_v2.resID          += 1
        self.resID                     = Reservation_v2.resID
        # these fields are defined when the reservation is ultimately scheduled
        self.scheduled_start      = None
        self.scheduled_resource   = None
        self.scheduled_quantum    = None
        self.scheduled            = False
        self.scheduled_timepoints = None
        # order is the parameter used for grouping & ordering in scheduling
        self.order                = 1

    
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
        return self.duration < other.duration

    
    def get_ID(self):
        return self.resID


    def convert_slots_to_intervals(self):
        tmpdict = {}
        for slot in self.slots:
            if slot.resource in tmpdict:
                tmpdict[slot.resource].extend(slot.timepoints)
            else: 
                tmpdict[slot.resource] = slot.timepoints
        for key in tmpdict.keys():
            tmpdict[key] = Intervals(tmpdict[key])
        return tmpdict


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

