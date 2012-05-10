#!/usr/bin/env python

'''
test_metricsprescheduler.py

Author: Sotiria Lampoudi
February, May 2012
'''

from nose.tools import assert_equal
import copy
from adaptive_scheduler.kernel.metricsprescheduler import *



class TestMetricsPreSchedulerScalar(object):

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

        self.mv1 = MetricsPreSchedulerScalar([self.cr1, self.cr2, self.cr3], 
                                 self.gpw2, [])
        self.mv2 = MetricsPreSchedulerScalar([self.cr1, self.cr4],
                                 self.gpw2, [])
        self.mv3 = MetricsPreSchedulerScalar([self.cr5],
                                 self.gpw2, [])


    def test_get_number_of_crs(self):
        assert_equal(self.mv1.get_number_of_compound_reservations(),3)
        assert_equal(self.mv2.get_number_of_compound_reservations(),2)
        assert_equal(self.mv3.get_number_of_compound_reservations(),1)
        assert_equal(self.mv3.get_number_of_compound_reservations('oneof'),1)
        assert_equal(self.mv1.get_number_of_compound_reservations('and'),1)
        assert_equal(self.mv1.get_number_of_compound_reservations('single'),2)


    def test_get_number_of_contractual_obligations(self):
        assert_equal(self.mv1.get_number_of_contractual_obligations(), 0)


    def test_get_number_of_resources(self):
        assert_equal(self.mv1.get_number_of_resources(), 2)
        assert_equal(self.mv2.get_number_of_resources(), 2)
        assert_equal(self.mv3.get_number_of_resources(), 2)



class TestMetricsPreSchedulerVector(object):

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

        self.mv1 = MetricsPreSchedulerVector([self.cr1, self.cr2, self.cr3], 
                                 self.gpw2, [])


    def test_get_coverage_by_resource_binary(self):
        rl = self.mv1.get_coverage_by_resource('foo', 'binary')
        assert_equal(rl[1], [2,5,0])
        assert_equal(rl[0], [1,2,1])
        rl = self.mv1.get_coverage_by_resource('bar', 'binary')
        assert_equal(rl, [[1, 2, 0], [2, 4, 1], [4, 5, 0]])


    def test_get_coverage_by_resource_count(self):
        rl = self.mv1.get_coverage_by_resource('foo', 'count')
        assert_equal(rl[0], [1,2,3])
        assert_equal(rl[1], [2,5,0])
        rl = self.mv1.get_coverage_by_resource('bar', 'count')
        assert_equal(rl, [[1, 2, 0], [2, 4, 1], [4, 5, 0]])

