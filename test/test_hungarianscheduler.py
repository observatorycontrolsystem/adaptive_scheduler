#!/usr/bin/env python

'''
test_hungarianscheduler.py

Author: Sotiria Lampoudi
March 2013
'''

from nose.tools import assert_equal

from adaptive_scheduler.kernel.hungarianscheduler import *

class TestBipartiteScheduler(object):

    def setup(self):
        s1 = Intervals([Timepoint(1, 'start'),
                        Timepoint(2, 'end')]) # 1-2

        s2 = Intervals([Timepoint(2, 'start'),
                        Timepoint(4, 'end')]) # --2--4

        self.r1 = Reservation_v3(1, 1, {'foo': s1})
        self.r2 = Reservation_v3(2, 2, {'bar': s2})
    
        self.bs = HungarianScheduler([self.r1, self.r2], ['foo', 'bar'])


    def test_create(self):
        assert_equal(self.bs.reservation_list, [self.r1, self.r2])
        assert_equal(self.bs.resource_list, ['foo', 'bar'])
        assert_equal(self.bs.scheduled_reservations, [])


    def test_get_reservation_by_ID(self):
        id = self.r1.get_ID()
        r = self.bs.get_reservation_by_ID(id)
        assert_equal(r, self.r1)
    def test_schedule(self):
        bs2 = HungarianScheduler([self.r1], ['foo']) 
        sr = bs2.schedule()
        assert_equal(sr, [self.r1])

# TODO: tests to write

# def test_convert_sparse_to_dense_matrix(self):
# def test_create_constraint_matrix(self):
# def test_merge_constraints(self):
