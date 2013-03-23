#!/usr/bin/env python

'''
test_slicedipscheduler.py

Author: Sotiria Lampoudi
Dec 2012
'''

from nose.tools import assert_equal
from adaptive_scheduler.kernel.timepoint import *
from adaptive_scheduler.kernel.intervals import *
from adaptive_scheduler.kernel.slicedipscheduler import *
import copy

class TestSlicedIPScheduler(object):

    def setup(self):
        s1 = Intervals([Timepoint(1, 'start'),
                        Timepoint(2, 'end')]) # 1-2
        s2 = Intervals([Timepoint(2, 'start'),
                        Timepoint(4, 'end')]) # --2--4
        s3 = copy.copy(s1)
        s4 = copy.copy(s1)
        s5 = copy.copy(s2)

        self.r1 = Reservation_v3(1, 1, {'foo': s1})
        self.r2 = Reservation_v3(2, 2, {'bar': s2})
        self.r3 = Reservation_v3(1, 1, {'foo': s3})
        self.r4 = Reservation_v3(1, 1, {'foo': s4})
        self.r5 = Reservation_v3(2, 2, {'bar': s5})
        self.r6 = Reservation_v3(1, 2, {'bar': s5})

        self.cr1 = CompoundReservation_v2([self.r1])
        self.cr2 = CompoundReservation_v2([self.r3, self.r2], 'and')
        self.cr3 = CompoundReservation_v2([self.r4])
        self.cr4 = CompoundReservation_v2([self.r5])
        self.cr5 = CompoundReservation_v2([self.r4, self.r5], 'oneof')
        self.cr6 = CompoundReservation_v2([self.r3])
        self.cr7 = CompoundReservation_v2([self.r2])
        self.cr8 = CompoundReservation_v2([self.r4, self.r6], 'oneof')
        self.cr9 = CompoundReservation_v2([self.r4, self.r1, self.r3], 'oneof')
        self.gpw = {}
        self.gpw['foo'] = [Timepoint(1, 'start'), Timepoint(5, 'end')]
        
        self.gpw2 = {}
        self.gpw2['foo'] = Intervals([Timepoint(1, 'start'), Timepoint(5, 'end')], 'free')
        self.gpw2['bar'] = Intervals([Timepoint(1, 'start'), Timepoint(5, 'end')], 'free')

        slice_dict = {}
        slice_dict['foo'] = [0,1]
        slice_dict['bar'] = [0,1]
        self.fs1 = SlicedIPScheduler([self.cr1, self.cr2, self.cr3], 
                                    self.gpw2, [], slice_dict)
        self.fs2 = SlicedIPScheduler([self.cr1, self.cr4],
                                    self.gpw2, [], slice_dict)
        self.fs3 = SlicedIPScheduler([self.cr5],
                                    self.gpw2, [], slice_dict)
        self.fs4 = SlicedIPScheduler([self.cr8, self.cr6, self.cr7],
                                    self.gpw2, [], slice_dict)
        

    def test_create(self):
        assert_equal(self.fs1.compound_reservation_list, [self.cr1, self.cr2, self.cr3])
        assert_equal(self.fs1.globally_possible_windows_dict, self.gpw2)
        assert_equal(self.fs1.contractual_obligation_list, [])
        assert_equal(self.fs1.schedule_dict_free['foo'].timepoints[0].time, 1)
        assert_equal(self.fs1.schedule_dict_free['foo'].timepoints[0].type, 
                     'start')
        assert_equal(self.fs1.schedule_dict_free['foo'].timepoints[1].time, 5)
        assert_equal(self.fs1.schedule_dict_free['foo'].timepoints[1].type, 
                     'end')
        

    def test_create_2(self):
        assert_equal(self.fs2.resource_list, ['foo', 'bar'])
        assert_equal(self.fs2.schedule_dict['foo'], [])
        assert_equal(self.fs2.schedule_dict['bar'], [])


    def test_convert_compound_to_simple_1(self):
        assert_equal(self.fs1.reservation_list[0], self.r1)
        assert_equal(self.fs1.reservation_list[1], self.r3)
        assert_equal(self.fs1.reservation_list[2], self.r2)
        assert_equal(self.fs1.reservation_list[3], self.r4)
        assert_equal(self.fs1.and_constraints[0][0], self.r3)
        assert_equal(self.fs1.and_constraints[0][1], self.r2)


    def test_convert_compound_to_simple_2(self):
        assert_equal(self.fs3.reservation_list[0], self.r4)
        assert_equal(self.fs3.reservation_list[1], self.r5)
        assert_equal(self.fs3.oneof_constraints[0][0], self.r4)
        assert_equal(self.fs3.oneof_constraints[0][1], self.r5)

    def test_get_slices_1(self):
        i = Intervals([Timepoint(0, 'start'), Timepoint(1, 'end')])
        s, si = self.fs1.get_slices(i, 0, 1, 1)
        assert_equal(s[0][0], 0)
        assert_equal(si[0], 0)


    def test_get_slices_2(self):
        ''' Multiple starts in one interval '''
        i = Intervals([Timepoint(0, 'start'), Timepoint(2, 'end')])
        s, si = self.fs1.get_slices(i, 0, 1, 1)
        assert_equal(s[0][0], 0)
        assert_equal(s[1][0], 1)
        assert_equal(si[0], 0)
        assert_equal(si[1], 1)


    def test_get_slices_3(self):
        ''' Duration doesn't fit in interval '''
        i = Intervals([Timepoint(0, 'start'), Timepoint(1, 'end')])
        s, si = self.fs1.get_slices(i, 0, 1, 2)
        assert_equal(s, [])
        assert_equal(si, [])

        
    def test_get_slices_4(self):
        ''' Multiple intervals '''
        i = Intervals([Timepoint(0, 'start'), Timepoint(1, 'end'), Timepoint(3, 'start'), Timepoint(4, 'end')])
        s, si = self.fs1.get_slices(i, 0, 1, 1)
        assert_equal(s[0][0], 0)
        assert_equal(s[1][0], 3)
        assert_equal(si[0], 0)
        assert_equal(si[1], 3)


    def test_get_slices_5(self):
        ''' slice_alignment eliminates an interval '''
        i = Intervals([Timepoint(0, 'start'), Timepoint(1, 'end'), Timepoint(3, 'start'), Timepoint(4, 'end')])
        s, si = self.fs1.get_slices(i, 1, 1, 1)
        assert_equal(s[0][0], 3)
        assert_equal(si[0], 3)
        

    def test_get_slices_6(self):
        ''' slice_alignment starts in the middle of an interval '''
        i = Intervals([Timepoint(0, 'start'), Timepoint(2, 'end'), Timepoint(3, 'start'), Timepoint(4, 'end')])
        s, si = self.fs1.get_slices(i, 1, 1, 1)
        assert_equal(s[0][0], 1)
        assert_equal(s[1][0], 3)
        assert_equal(si[0], 1)
        assert_equal(si[1], 3)


    def test_get_slices_7(self):
        ''' internal start case '''
        i = Intervals([Timepoint(17, 'start'), Timepoint(25, 'end')])
        s, si = self.fs1.get_slices(i, 10, 5, 5)
        assert_equal(s[0][0], 15)
        assert_equal(s[0][1], 20)
        assert_equal(s[1][0], 20)
        assert_equal(si[0], 17)
        assert_equal(si[1], 20)
        
    
    def test_get_slices_8(self):
        i = Intervals([Timepoint(1, 'start'), Timepoint(4, 'end')])
        s, si = self.fs1.get_slices(i, 0, 1, 1)
        assert_equal(s[0][0], 1)
        assert_equal(s[1][0], 2)
        assert_equal(s[2][0], 3)
        assert_equal(si[0], 1)
        assert_equal(si[1], 2)
        assert_equal(si[2], 3)


    def test_get_slices_9(self):
        i = Intervals([Timepoint(2, 'start'), Timepoint(5, 'end')])
        s, si = self.fs1.get_slices(i, 0, 1, 3)
        assert_equal(s[0][0], 2)
        assert_equal(s[0][1], 3)
        assert_equal(s[0][2], 4)
        assert_equal(si[0], 2)


    def test_get_slices_10(self):
        i = Intervals([Timepoint(2, 'start'), Timepoint(5, 'end')])
        s, si = self.fs1.get_slices(i, 0, 10, 3)
        assert_equal(s[0][0], 0)
        assert_equal(si[0], 2)
