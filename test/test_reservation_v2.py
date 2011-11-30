#!/usr/bin/env python

'''
test_reservation_v2.py

Author: Sotiria Lampoudi
November 2011
'''

from nose.tools import assert_equal

from timepoint import *
from intervals import *
from reservation_v2 import *

class TestReservation_v2(object):
    
    def setup(self):
        self.s1 = Slot(1,1,'foo')
        self.s2 = Slot(2,2,'bar')

        self.r1 = Reservation_v2(1, 1, [self.s1,self.s2])
        self.r2 = Reservation_v2(1, 2, [self.s1,self.s2])
    
        self.cr1 = CompoundReservation_v2([self.r1])
        self.cr2 = CompoundReservation_v2([self.r1, self.r2], 'and')
        self.cr3 = CompoundReservation_v2([self.r1], 'nof', 2)

    def test_create_slot(self):
        assert_equal(self.s1.start, 1)
        assert_equal(self.s1.length, 1)
        assert_equal(self.s1.end, 2)
        assert_equal(self.s1.resource, 'foo')
        assert_equal(self.s1.timepoints[0].time, 1)
        assert_equal(self.s1.timepoints[0].type, 'start')
        assert_equal(self.s1.timepoints[1].time, 2)
        assert_equal(self.s1.timepoints[1].type, 'end')


    def test_create_reservation(self):
        assert_equal(self.r1.priority, 1)
        assert_equal(self.r1.duration, 1)
        assert_equal(self.r1.slots[0], self.s1)
        assert_equal(self.r1.slots[1], self.s2)
        

    def test_create_reservation_2(self):
        '''
        Constructor should drop s1 slot, leave only s2
        '''
        assert_equal(self.r2.duration, 2)
        assert_equal(self.r2.slots[0], self.s2)
        

    def test_convert_slots_to_intervals(self):
        assert self.r1.possible_windows_dict['foo']
        assert self.r1.possible_windows_dict['bar']
        assert_equal(self.r1.possible_windows_dict['foo'].timepoints[0].time, 1)
        assert_equal(self.r1.possible_windows_dict['foo'].timepoints[0].type, 'start')


    def test_lt(self):
        assert self.r1 < self.r2


    def test_schedule(self):
        self.r1.schedule(1, 'foo', 1)
        assert_equal(self.r1.scheduled_start, 1)
        assert_equal(self.r1.scheduled_resource, 'foo')
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
        assert_equal(self.cr1.repeats, 1)
        assert self.cr1.issingle()

    def test_create_compound_2(self):
        assert_equal(self.cr2.reservation_list, [self.r1, self.r2])
        assert_equal(self.cr2.type, 'and')
        assert_equal(self.cr2.size, 2)
        assert_equal(self.cr2.repeats, 1)
        assert self.cr2.isand()


    def test_create_compound_3(self):
        assert_equal(self.cr3.reservation_list, [self.r1])
        assert_equal(self.cr3.type, 'nof')
        assert_equal(self.cr3.size, 1)
        assert_equal(self.cr3.repeats, 2)
        assert self.cr3.isnof()
