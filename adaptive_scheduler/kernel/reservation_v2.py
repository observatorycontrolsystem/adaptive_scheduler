#!/usr/bin/env python

'''
Reservation_v2 and CompoundReservation_v2 classes for scheduling.

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
November 2011
edited Dec 2011: added resource to res. constructor, 
removed by-resource dicts, added oneof to comp. res.
'''

from timepoint import *
from intervals import *
import copy

class Reservation_v2(object):

    resID = 0

    def __init__(self, priority, duration, resource, possible_windows):
        self.priority = priority
        self.duration = duration
        self.resource = resource
        self.possible_windows = possible_windows
        # free_windows keeps track of which of the possible_windows 
        # are free.
        self.free_windows    = copy.copy(self.possible_windows)
        # clean up free windows by removing ones that are too small:
        #            self.clean_up_free_windows()
        Reservation_v2.resID     += 1
        self.resID                = Reservation_v2.resID
        # these fields are defined when the reservation is ultimately scheduled
        self.scheduled_start      = None
        self.scheduled_quantum    = None
        self.scheduled            = False
        self.scheduled_timepoints = None
        self.scheduled_by         = None
        # order is the parameter used for grouping & ordering in scheduling
        self.order                = 1


    def schedule_anywhere(self):
        # find the first available spot & stick it there
        start = self.free_windows.find_interval_of_length(self.duration)
        if start >=0:
            self.schedule(start, self.duration, 'reservation_v2.schedule_anywhere()')
            return True
        else:
            return False

    
    def schedule(self, start, quantum, scheduler_description=None):
        self.scheduled_start    = start
        self.scheduled_quantum  = quantum
        self.scheduled          = True
        self.scheduled_timepoints = [Timepoint(start, 'start'), 
        Timepoint(start+quantum, 'end')]
        self.scheduled_by       = scheduler_description


    def unschedule(self):
        self.scheduled_start    = None
        self.scheduled_quantum  = None
        self.scheduled          = False
        self.scheduled_timepoints = None
        self.scheduled_by       = None


    def __str__(self):
        str = "Reservation ID: {0} \
        \n\tpriority: {1} \
        \n\tduration: {2} \
        \n\tresource: {3} \
        \n\tpossible windows: {4}\
        \n\tis scheduled: {5}\n".format(self.resID, self.priority, 
                                        self.duration, self.resource, 
                                        self.possible_windows, self.scheduled)
        if self.scheduled:
            str += "\t\tscheduled start: {0}\n\t\tscheduled quantum: {1}\n\t\tscheduled by: {2}\n". format(self.scheduled_start, self.scheduled_quantum, self.scheduled_by)
        return str
                    

    def __lt__(self, other):
        return self.priority < other.priority

    
    def get_ID(self):
        return self.resID


    def remove_from_free_windows(self, interval):
        self.free_windows = self.free_windows.subtract(interval)
        self.clean_up_free_windows()

        
    def clean_up_free_windows(self):
        self.free_windows.remove_intervals_smaller_than(self.duration)


class CompoundReservation_v2(object):

    valid_types = {
        'single' : 'A single one of the provided blocks is to be scheduled',
        'oneof'  : 'One of the provided blocks are to be scheduled',
        'and'    : 'All of the provided blocks are to be scheduled',
        }

    def __init__(self, reservation_list, type='single'):
        self.reservation_list = reservation_list
        self.type = type
        # allowed types are:
        # single
        # oneof
        # and
        self.size       = len(reservation_list)
        if type == 'single' and self.size > 1:
            msg = ( "Initializing a CompoundReservation as 'single' but with %d "
                    "individual reservations. Ignoring all but the first."
                    % self.size )
            print msg
            self.size = 1
            self.reservation_list = [reservation_list.pop(0)]
        if (type == 'and') and (self.size == 1):
            msg = ( "Initializing a CompoundReservation as 'and' but with %d "
                    "individual reservation."
                    % self.size )
            print msg
        if type == 'oneof' and self.size == 1:
            msg = ( "Initializing a CompoundReservation as 'oneof' but with %d "
                    "individual reservation."
                    % self.size )
            print msg


    def issingle(self):
        if self.type == "single":
            return True
        else:
            return False


    def isoneof(self):
        if self.type == "oneof":
            return True
        else:
            return False


    def isand(self):
        if self.type == "and":
            return True
        else:
            return False

