#!/usr/bin/env python

'''
test_bipartitescheduler.py

Author: Sotiria Lampoudi
Dec 2012
'''

from nose.tools import assert_equal
import copy
from adaptive_scheduler.kernel.bipartitescheduler import *

class TestBipartiteScheduler(object):

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
        
        self.fs1 = BipartiteScheduler([self.cr1, self.cr2, self.cr3], 
                                      self.gpw2, [])
        self.fs2 = BipartiteScheduler([self.cr1, self.cr4],
                                      self.gpw2, [])
        
        self.fs3 = BipartiteScheduler([self.cr5],
                                      self.gpw2, [])
        

    def test_create(self):
        assert_equal(self.fs1.current_order, 1)
        assert_equal(self.fs1.current_resource, None)


    def test_cluster_and_order_reservations(self):
        n = self.fs1.cluster_and_order_reservations(2)
#        assert_equal(n, 2)
        print self.fs1.reservation_list[0].order
        print self.fs1.reservation_list[1].order
        print self.fs1.reservation_list[2].order
        print self.fs1.reservation_list[3].order

        # assert_equal(self.fs1.reservation_list[0].order, 2)
        # assert_equal(self.fs1.reservation_list[1].order, 2)
        # assert_equal(self.fs1.reservation_list[2].order, 1)
        # assert_equal(self.fs1.reservation_list[3].order, 2)


    def test_enforce_and_constraints_1(self):
        self.fs1.enforce_and_constraints()


    def test_enforce_and_constraints_2(self):
        self.r3.schedule(1, 1, 'foo', 'test')
        self.fs1.commit_reservation_to_schedule(self.r3)
        self.fs1.enforce_and_constraints()
        assert_equal(self.fs1.schedule_dict['foo'], [])


    def test_enforce_and_constraints_3(self):
        self.r3.schedule(1, 1, 'foo', 'test')
        self.r2.schedule(2, 2, 'bar', 'test')
        self.fs1.commit_reservation_to_schedule(self.r3)
        self.fs1.commit_reservation_to_schedule(self.r2)
        self.fs1.enforce_and_constraints()
        assert_equal(self.fs1.schedule_dict['foo'], [self.r3])
        assert_equal(self.fs1.schedule_dict['bar'], [self.r2])

