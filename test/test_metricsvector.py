#!/usr/bin/env python

'''
test_metricsvector.py

Author: Sotiria Lampoudi
February 2012
'''

from nose.tools import assert_equal
import copy
from adaptive_scheduler.kernel.metricsvector import *
 
class TestMetricsVector(object):

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

        self.mv1 = MetricsVector([self.cr1, self.cr2, self.cr3], 
                                 self.gpw2, [])
        self.mv2 = MetricsVector([self.cr1, self.cr4],
                                 self.gpw2, [])
        self.mv3 = MetricsVector([self.cr5],
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


    def test_intervals_to_retval(self):
        rl = self.mv1.intervals_to_retval(self.s1, 0)
        assert_equal(rl, [[1, 2, 0]])
        rl = self.mv1.intervals_to_retval(self.s1, 1)
        assert_equal(rl, [[1, 2, 1]])
        

    def test_get_coverage_by_resource(self):
        rl = self.mv1.get_coverage_by_resource('foo')
        assert_equal(rl[1], [2,5,0])
        assert_equal(rl[0], [1,2,1])
        rl = self.mv1.get_coverage_by_resource('bar')
        assert_equal(rl, [[1, 2, 0], [2, 4, 1], [4, 5, 0]])

