#!/usr/bin/env python

'''
test_reservation_v2.py

Author: Sotiria Lampoudi
November 2011
'''

from nose.tools import assert_equal

from adaptive_scheduler.kernel.timepoint import *
from adaptive_scheduler.kernel.intervals import *
from adaptive_scheduler.kernel.reservation_v2 import *

class TestReservation_v2(object):
    
    def setup(self):
        s1 = Intervals([Timepoint(1, 'start'), Timepoint(2, 'end')]) 
        s2 = Intervals([Timepoint(2, 'start'), Timepoint(4, 'end')]) 
        s3 = Intervals([Timepoint(2, 'start'), Timepoint(6, 'end')]) 
        self.r1 = Reservation_v2(1, 1, 'foo', s1)
        self.r2 = Reservation_v2(1, 2, 'bar', s2)
        self.r3 = Reservation_v2(2, 1, 'foo', s3)
    
        self.cr1 = CompoundReservation_v2([self.r1])
        self.cr2 = CompoundReservation_v2([self.r1, self.r2], 'and')
        self.cr3 = CompoundReservation_v2([self.r1, self.r3], 'oneof')


    def test_create_reservation(self):
        assert_equal(self.r1.priority, 1)
        assert_equal(self.r1.duration, 1)
        assert_equal(self.r1.resource, 'foo')

    def test_print(self):
        print self.r1

    
    def test_print_2(self):
        self.r1.schedule(1, 1)
        assert_equal(self.r1.scheduled_start, 1)
        assert_equal(self.r1.scheduled_quantum, 1)
        assert_equal(self.r1.scheduled, True)
        assert_equal(self.r1.scheduled_timepoints[0].time, 1)
        assert_equal(self.r1.scheduled_timepoints[0].type, 'start')
        assert_equal(self.r1.scheduled_timepoints[1].time, 2)
        assert_equal(self.r1.scheduled_timepoints[1].type, 'end')
        print self.r1


    def test_remove_from_free_windows_1(self):
        self.r1.remove_from_free_windows(Intervals([Timepoint(1, 'start'), Timepoint(2, 'end')]))
        assert_equal(self.r1.free_windows.timepoints, [])


    def test_remove_from_free_windows_2(self):
        self.r2.remove_from_free_windows(Intervals([Timepoint(3, 'start'), Timepoint(4, 'end')]))
        assert_equal(self.r2.free_windows.timepoints, [])



    def test_remove_from_free_windows_3(self):
        self.r3.remove_from_free_windows(Intervals([Timepoint(2, 'start'), Timepoint(3, 'end')]))
        assert_equal(self.r3.free_windows.timepoints[0].time, 3)
        assert_equal(self.r3.free_windows.timepoints[0].type, 'start')
        assert_equal(self.r3.free_windows.timepoints[1].time, 6)
        assert_equal(self.r3.free_windows.timepoints[1].type, 'end')


    def test_lt(self):
        '''Sorting by priority'''
        assert self.r1 < self.r3


    def test_schedule(self):
        self.r1.schedule(1, 1)
        assert_equal(self.r1.scheduled_start, 1)
        assert_equal(self.r1.scheduled_quantum, 1)
        assert_equal(self.r1.scheduled, True)
        assert_equal(self.r1.scheduled_timepoints[0].time, 1)
        assert_equal(self.r1.scheduled_timepoints[0].type, 'start')
        assert_equal(self.r1.scheduled_timepoints[1].time, 2)
        assert_equal(self.r1.scheduled_timepoints[1].type, 'end')
        

    def test_create_compound(self):
        assert_equal(self.cr1.reservation_list, [self.r1])
        assert_equal(self.cr1.type, 'single')
        assert_equal(self.cr1.size, 1)
        assert self.cr1.issingle()

    def test_create_compound_2(self):
        assert_equal(self.cr2.reservation_list, [self.r1, self.r2])
        assert_equal(self.cr2.type, 'and')
        assert_equal(self.cr2.size, 2)
        assert self.cr2.isand()


    def test_create_compound_3(self):
        assert_equal(self.cr3.reservation_list, [self.r1, self.r3])
        assert_equal(self.cr3.type, 'oneof')
        assert_equal(self.cr3.size, 2)
        assert self.cr3.isoneof()
