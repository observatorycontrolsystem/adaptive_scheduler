#!/usr/bin/env python

'''
test_uncontendedscheduler_v1.py

Author: Sotiria Lampoudi
November 2011
'''

from nose.tools import assert_equal

from adaptive_scheduler.kernel.uncontendedscheduler import *

class TestUncontendedScheduler_v1(object):

    def setup(self):
        s1 = [Timepoint(1, 'start', 'foo'), 
              Timepoint(2, 'end', 'foo')] # 1-2
        s2 = [Timepoint(2, 'start', 'foo'), 
              Timepoint(4, 'end', 'foo')] # --2--4
        s3 = [Timepoint(1, 'start', 'foo'), 
              Timepoint(3, 'end', 'foo')] # 1--3

        self.r1 = Reservation_v2(1, 1, s1)
        self.r2 = Reservation_v2(1, 2, s2)
        self.r3 = Reservation_v2(1, 1, s3)

        
    def test_create(self):
        us = UncontendedScheduler([self.r1, self.r2], 'foo')
        assert_equal(us.reservation_list, [self.r1, self.r2])
        assert_equal(us.resource, 'foo')
        assert_equal(us.scheduled_reservations, [])


    def test_find_uncontended_windows_1(self):
        us = UncontendedScheduler([self.r1, self.r2], 'foo')
        u = us.find_uncontended_windows(self.r1)
        assert_equal(u.timepoints[0].time, 1)
        assert_equal(u.timepoints[0].type, 'start')
        assert_equal(u.timepoints[1].time, 2)
        assert_equal(u.timepoints[1].type, 'end')
        

    def test_find_uncontended_windows_2(self):
        us = UncontendedScheduler([self.r1, self.r2], 'foo')
        u = us.find_uncontended_windows(self.r2)
        assert_equal(u.timepoints[0].time, 2)
        assert_equal(u.timepoints[0].type, 'start')
        assert_equal(u.timepoints[1].time, 4)
        assert_equal(u.timepoints[1].type, 'end')


    def test_find_uncontended_windows_3(self):
        us = UncontendedScheduler([self.r3, self.r2], 'foo')
        u = us.find_uncontended_windows(self.r2)
        assert_equal(u.timepoints[0].time, 3)
        assert_equal(u.timepoints[0].type, 'start')
        assert_equal(u.timepoints[1].time, 4)
        assert_equal(u.timepoints[1].type, 'end')
        u = us.find_uncontended_windows(self.r3)
        assert_equal(u.timepoints[0].time, 1)
        assert_equal(u.timepoints[0].type, 'start')
        assert_equal(u.timepoints[1].time, 2)
        assert_equal(u.timepoints[1].type, 'end')


    def test_fit_reservation_in_uncontended_window(self):
        us = UncontendedScheduler([self.r3, self.r2], 'foo')
        u2 = us.find_uncontended_windows(self.r2)
        u3 = us.find_uncontended_windows(self.r3)
        assert_equal(us.fit_reservation_in_uncontended_windows(self.r2, u2), False)
        assert_equal(us.fit_reservation_in_uncontended_windows(self.r3, u3), True)
        assert_equal(us.scheduled_reservations[0], self.r3)
        
