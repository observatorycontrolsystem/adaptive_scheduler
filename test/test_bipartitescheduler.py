#!/usr/bin/env python

'''
test_bipartitescheduler.py

Author: Sotiria Lampoudi
November 2011
'''

from nose.tools import assert_equal

from adaptive_scheduler.kernel.fullscheduler_v1 import *
from adaptive_scheduler.kernel.bipartitescheduler import *

class TestBipartiteScheduler(object):

    def setup(self):
        s1 = Intervals([Timepoint(1, 'start'),
                        Timepoint(2, 'end')]) # 1-2

        s2 = Intervals([Timepoint(2, 'start'),
                        Timepoint(4, 'end')]) # --2--4

        self.r1 = Reservation_v2(1, 1, 'foo', s1)
        self.r2 = Reservation_v2(2, 2, 'bar', s2)
    
        self.bs = BipartiteScheduler([self.r1, self.r2], ['foo', 'bar'])


    def test_create(self):
        assert_equal(self.bs.reservation_list, [self.r1, self.r2])
        assert_equal(self.bs.resource_list, ['foo', 'bar'])
        assert_equal(self.bs.scheduled_reservations, [])


    def test_get_reservation_by_ID(self):
        id = self.r1.get_ID()
        r = self.bs.get_reservation_by_ID(id)
        assert_equal(r, self.r1)


    def test_hash_and_unhash_quantum_start(self):
        tmp = self.bs.hash_quantum_start('foo', 2, 1)
        [resource, start, quantum] = self.bs.unhash_quantum_start(tmp)
        assert_equal(resource, 'foo')
        assert_equal(start, 2)
        assert_equal(quantum, 1)
       
 
    def test_quantize_windows_1(self):
        qs = self.bs.quantize_windows(self.r1, 1)
        [resource, start, quantum] = self.bs.unhash_quantum_start(qs[0])
        assert_equal(resource, 'foo')
        assert_equal(start, 1)
        assert_equal(quantum, 1)


    def test_quantize_windows_2(self):
        qs = self.bs.quantize_windows(self.r1, 1)
        [resource, start, quantum] = self.bs.unhash_quantum_start(qs[0])
        assert_equal(resource, 'foo')
        assert_equal(start, 1)
        assert_equal(quantum, 1)
        

    def test_schedule(self):
        bs2 = BipartiteScheduler([self.r1], ['foo']) 
        sr = bs2.schedule()
        assert_equal(sr, [self.r1])

