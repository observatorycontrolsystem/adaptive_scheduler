#!/usr/bin/env python

'''
uncontendedscheduler.py -- Schedules reservations in uncontended windows, if
possible.

Author: Sotiria Lampoudi (slampoud@gmail.com)
November 2011
'''

from reservation_v3 import *

class UncontendedScheduler(object):

    def __init__(self, reservation_list, resource):
        self.reservation_list = reservation_list
        self.resource         = resource
        self.scheduled_reservations         = []


    def find_uncontended_windows(self, reservation):
        uncontended = []
        if self.resource in reservation.free_windows_dict:
            uncontended = reservation.free_windows_dict[self.resource] #intervals
            for r in self.reservation_list:
                if self.resource in r.free_windows_dict:
                    if r == reservation: 
                        continue
                    elif r.scheduled:
                        uncontended = uncontended.subtract(Intervals(r.scheduled_timepoints, 'busy'))
                    else:
                        uncontended = uncontended.subtract(r.free_windows_dict[self.resource])
        return uncontended 

    
    def fit_reservation_in_uncontended_windows(self, reservation, windows):
        ret = windows.find_interval_of_length(reservation.duration)
        if ret >= 0:
            reservation.schedule(ret, reservation.duration, 
                                 self.resource, 
                                 'uncontended scheduler')
            self.scheduled_reservations.append(reservation)
            return True
        return False


    def schedule(self):
        for r in self.reservation_list:
            if not r.scheduled:
                uncontended = self.find_uncontended_windows(r)
                if uncontended:
                    self.fit_reservation_in_uncontended_windows(r, uncontended)
        return self.scheduled_reservations
