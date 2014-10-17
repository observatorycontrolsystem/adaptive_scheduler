#!/usr/bin/env python

'''
test_hopcroftkarpscheduler.py

Author: Sotiria Lampoudi
November 2011
'''

from nose.tools import assert_equal

from adaptive_scheduler.kernel.hopcroftkarpscheduler import *
from adaptive_scheduler.kernel.fullscheduler_v1 import *

class TestHopcroftKarpScheduler(object):

    def setup(self):
        s1 = Intervals([Timepoint(1, 'start'),
                        Timepoint(2, 'end')]) # 1-2

        s2 = Intervals([Timepoint(2, 'start'),
                        Timepoint(4, 'end')]) # --2--4

        self.r1 = Reservation_v3(1, 1, {'foo': s1})
        self.r2 = Reservation_v3(2, 2, {'bar': s2})
    
        self.bs = HopcroftKarpScheduler([self.r1, self.r2], ['foo', 'bar'])


    def test_create(self):
        assert_equal(self.bs.reservation_list, [self.r1, self.r2])
        assert_equal(self.bs.resource_list, ['foo', 'bar'])
        assert_equal(self.bs.scheduled_reservations, [])


    def test_schedule(self):
        bs2 = HopcroftKarpScheduler([self.r1], ['foo']) 
        sr = bs2.schedule()
        assert_equal(sr, [self.r1])


    def test_schedule(self):
        sr = self.bs.schedule()
        assert_equal(sr, [self.r2, self.r1])


    def test_merge_constraints(self):
        assert_equal(len(self.bs.constraint_graph[self.r1.get_ID()]), 1)
        self.bs.merge_constraints(self.r1.get_ID(), self.r2.get_ID())
        assert_equal(len(self.bs.constraint_graph[self.r1.get_ID()]), 2)
        assert not self.r2.get_ID() in self.bs.constraint_graph.keys()


