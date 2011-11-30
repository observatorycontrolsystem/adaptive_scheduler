#!/usr/bin/env python

'''
Dynamic Programming Scheduler:
* Input: list of reservation requests
  (earliest & latest start, duration, priority)
* Output: schedule of non-overlapping reservations

Following notation from:
http://www.cs.princeton.edu/courses/archive/spr05/cos423/lectures/06dynamic-programming.pdf

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
June 2011
'''

class DPExtScheduler:
    def __init__(self, reservation_list = None):
        self.reservations = reservation_list or []
        self.schedule     = None
        self.is_sorted    = False


    def add_reservation(self, r):
        self.reservations.append(r)
        if self.is_sorted:
            self.reservations.sort()


    def sort_reservations(self):
        if not self.is_sorted:
            self.reservations.sort()
            self.is_sorted=True


    def get_reservation(self, idx):
        # convert from the 1...n indexing of the algo to the
        # 0...n-1 indexing of the reservation list data structure
        return self.reservations[idx-1]


    def max_compatible(self, idx):
        # this is where the Ext magic happens!
        # max_compatible(idx) requires all i<idx to have concrete
        # start & end times. This is guaranteed if max_compatible
        # is called on i...n in order.

        self.sort_reservations()

        i=idx-1
        while i > 0:
            if ( self.get_reservation(i).end
                 <= self.get_reservation(idx).latest_start ):
                # make the start & end of idx concrete
                self.get_reservation(idx).start = max( self.get_reservation(idx).earliest_start, self.get_reservation(i).end )
                self.get_reservation(idx).end = self.get_reservation(idx).start + self.get_reservation(idx).duration
                return i
            else:
                i=i-1

               # either this is the first request, or idx is not compatible
        # with any i < idx
        # either way, fix the start & end time to be as early as pos.
        self.get_reservation(idx).start=self.get_reservation(idx).earliest_start
        self.get_reservation(idx).end = self.get_reservation(idx).earliest_start + self.get_reservation(idx).duration
        return 0


    def recursive_find_schedule(self, j):
        if j == 0:
            return
        elif ( self.get_reservation(j).priority + self.M[self.p[j]] ) > self.M[j-1]:
            self.schedule.append(self.get_reservation(j))
            self.get_reservation(j).scheduled=True
            self.recursive_find_schedule(self.p[j])
        else:
            self.recursive_find_schedule(j-1)


    def schedule_reservations(self):
        # p[i] = j means j is the max index job (in the sorted list)
        #        compatible with job i
        # M[i] = sum max weight of everything at & before i
        self.p = [0]
        self.M = [0]
        self.sort_reservations()
        for i in range(1, len(self.reservations)+1):
            self.p.append(self.max_compatible(i))
        for i in range(1, len(self.reservations)+1):
            self.M.append(max( self.get_reservation(i).priority +
                        self.M[self.p[i]], self.M[i-1] ))
        self.schedule = []
        self.recursive_find_schedule(len(self.reservations))
        self.schedule.sort()

        return self.schedule
