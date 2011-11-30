#!/usr/bin/env python

'''
Reservation and CompoundReservation classes for scheduling.

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
July 2011
'''

class Reservation:
    resID=0
    def __init__(self, priority, duration, earliest_start, latest_start=None):

        self.priority       = priority
        self.duration       = duration
        self.earliest_start = earliest_start

        if latest_start == None:
            self.latest_start = earliest_start
            self.start        = earliest_start
            self.end          = earliest_start+duration
        else:
            self.latest_start = latest_start
            self.start        = None
            self.end          = None
        self.deadline     = self.latest_start+duration
        self.scheduled    = False
        Reservation.resID = Reservation.resID+1
        self.resID        = Reservation.resID

    def __lt__(self, other):
        return self.deadline < other.deadline



class CompoundReservation:
    def __init__(self, reservation_list, type='single'):
        self.reservation_list = reservation_list
        self.type = type
        # allowed types are:
        # single
        # oneof
        # and
        self.size = len(reservation_list)
        if type == 'single' and self.size > 1:
            msg = ( "Initializing a CompoundReservation as 'single' but with %d "
                    "individual reservations. Ignoring all but the first."
                    % self.size )
            print msg
            self.size = 1
            self.reservation_list = [reservation_list.pop(0)]


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
