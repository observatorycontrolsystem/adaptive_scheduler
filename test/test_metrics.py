#!/usr/bin/env python

'''
test_metrics.py

Author: Sotiria Lampoudi
May 2012
'''

from nose.tools import assert_equal
import copy
from adaptive_scheduler.kernel.metrics import *


class TestMetrics(object):

    def setup(self):
        s1 = Intervals([Timepoint(1, 'start'),
                        Timepoint(2, 'end')]) # 1-2
        s2 = Intervals([Timepoint(2, 'start'),
                        Timepoint(4, 'end')]) # --2--4
        s3 = copy.copy(s1)
        s4 = copy.copy(s1)
        s5 = copy.copy(s2)
        self.s1 = s1
        self.s2 = s2

        self.r1 = Reservation_v2(1, 1, 'foo', s1)
        self.r2 = Reservation_v2(2, 2, 'bar', s2)
        self.r3 = Reservation_v2(1, 1, 'foo', s3)
        self.r4 = Reservation_v2(1, 1, 'foo', s4)
        self.r5 = Reservation_v2(2, 2, 'bar', s5)

        self.cr1 = CompoundReservation_v2([self.r1])
        self.cr2 = CompoundReservation_v2([self.r3, self.r2], 'and')
        self.cr3 = CompoundReservation_v2([self.r4])
        self.cr4 = CompoundReservation_v2([self.r5])
        self.cr5 = CompoundReservation_v2([self.r4, self.r5], 'oneof')

        self.gpw = {}
        self.gpw['foo'] = [Timepoint(1, 'start'), Timepoint(5, 'end')]
        
        self.gpw2 = {}
        self.gpw2['foo'] = Intervals([Timepoint(1, 'start'), Timepoint(5, 'end')], 'free')
        self.gpw2['bar'] = Intervals([Timepoint(1, 'start'), Timepoint(5, 'end')], 'free')

        self.mv1 = Metrics([self.cr1, self.cr2, self.cr3], 
                           self.gpw2, [])
        self.mv3 = Metrics([self.cr5],
                           self.gpw2, [])

    
    def test_create(self):
        assert_equal(self.mv1.compound_reservation_list, [self.cr1, self.cr2, self.cr3])
        assert_equal(self.mv1.globally_possible_windows_dict, self.gpw2)
        assert_equal(self.mv1.contractual_obligation_list, [])
        assert_equal(self.mv1.resource_list, ['foo', 'bar'])        


    def test_convert_compound_to_simple_1(self):
        assert_equal(self.mv1.reservation_list[0], self.r1)
        assert_equal(self.mv1.reservation_list[1], self.r3)
        assert_equal(self.mv1.reservation_list[2], self.r2)
        assert_equal(self.mv1.reservation_list[3], self.r4)
        assert_equal(self.mv1.and_constraints[0][0], self.r3)
        assert_equal(self.mv1.and_constraints[0][1], self.r2)


    def test_convert_compound_to_simple_2(self):
        assert_equal(self.mv3.reservation_list[0], self.r4)
        assert_equal(self.mv3.reservation_list[1], self.r5)
        assert_equal(self.mv3.oneof_constraints[0][0], self.r4)
        assert_equal(self.mv3.oneof_constraints[0][1], self.r5)
