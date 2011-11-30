#!/usr/bin/env python

'''
Harness for scheduling compound reservations

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
July 2011
'''

import copy
from dpextscheduler import *

class CompoundScheduler:
    def __init__(self, reslist=None):
        self.submissions     = [[]]
        self.compoundreslist = reslist or []
        for cr in self.compoundreslist:
            self.add_compound_reservation(cr)
        self.schedules   = []
        self.priorities  = []
        self.constraints = []
        # constraints are given as lists of reservations
        # if n reservations are in the same list (==constraint) then
        # they are and-ed


    def add_compound_reservation(self, cr):
        # if the type is:
        # * single: add reservation to all sched. submissions
        # * and: add all reservations to all submissions
        # *      add constraint specifying all or none must be included
        # * oneof: for each res. clone all submissions and add res.
        if cr.issingle():
            for s in self.submissions:
                s.append(cr.reservation_list[0])
        elif cr.isand():
            # add all the reservations to all the submissions
            for s in self.submissions:
                s.extend(cr.reservation_list)
            # add the constraint to the list of constraints
            self.constraints.append(cr.reservation_list)
        elif cr.isoneof():
            self.clone_all_submissions(cr.size)
            i = 0
            for s in self.submissions:
                s.append(cr.reservation_list[i])
                i = (i+1)%cr.size


    def clone_all_submissions(self, n):
        original=copy.deepcopy(self.submissions)
        for i in range(n-1):
            self.submissions.extend(original)


    def find_reservation_in_schedule(self, s, reservation):
        for r in s:
            if r.resID == reservation.resID:
                return True
        return False


    def remove_reservation_from_schedule(self, s, reservation):
        for r in s:
            if r.resID == reservation.resID:
                s.remove(r)


    def check_all_constraints(self, s):
        # check all constraints against specific schedule
        for c in self.constraints:
            counter = 0
            size=len(c)
            for r in c:
                if self.find_reservation_in_schedule(s, r):
                    counter=counter+1
            if counter > 0 and counter != size:
                # we can either just return False, or fix it
                # by removing the scheduled res's, and then
                # return. Here we fix it first
                for r in c:
                    self.remove_reservation_from_schedule(s,r)
                return False
        return True


    def get_sum_priority(self, schedule):
        sum = 0
        for s in schedule:
            sum = sum + s.priority
        return sum


    def schedule_compound_reservations(self):
        for s in self.submissions:
            scheduler    = DPExtScheduler(s)
            schedule_out = scheduler.schedule_reservations()
            # check against constraints
            if self.check_all_constraints(schedule_out):
                # keep the schedule if
                # all constraints are satisfied
                self.schedules.append(schedule_out)
                # calculate priority
                self.priorities.append(
                    self.get_sum_priority(schedule_out))
            # now self.schedules contains viable schedules
            # and self.priorities contains the corresponding priorities
        if len(self.schedules) > 0:
            print 'There are ' + str(self.priorities.count(max(self.priorities))) + ' schedules with max priority and ' + str(len(self.constraints)) + ' constraints'
            idx=self.priorities.index(max(self.priorities))
            return self.schedules[idx]
        else:
            print 'No schedules matching ' + str(len(self.constraints)) + ' constraints'
            return []

