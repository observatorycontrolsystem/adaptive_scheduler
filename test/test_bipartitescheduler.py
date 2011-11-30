#!/usr/bin/env python

'''
test_bipartitescheduler.py

Author: Sotiria Lampoudi
November 2011
'''

from nose.tools import assert_equal

from fullscheduler_v1 import *
from bipartitescheduler import *

class TestFullScheduler_v1(object):

    def setup(self):
        self.s1 = Slot(1,1,'foo')
        self.s2 = Slot(2,2,'bar')

        self.r1 = Reservation_v2(1, 1, [self.s1,self.s2])
        self.r2 = Reservation_v2(2, 2, [self.s1,self.s2])
    
        self.cr1 = CompoundReservation_v2([self.r1])
        self.cr2 = CompoundReservation_v2([self.r1, self.r2], 'and')
        self.cr3 = CompoundReservation_v2([self.r1], 'nof', 2)
        self.cr4 = CompoundReservation_v2([self.r2])

        self.gpw = {}
        self.gpw['foo'] = [Timepoint(1, 'start'), Timepoint(5, 'end')]
        
        self.gpw2 = {}
        self.gpw2['foo'] = [Timepoint(1, 'start'), Timepoint(5, 'end')]
        self.gpw2['bar'] = [Timepoint(1, 'start'), Timepoint(5, 'end')]
        
        self.fs1 = FullScheduler_v1([self.cr1, self.cr2, self.cr3], 
                                    self.gpw, [])
        self.fs2 = FullScheduler_v1([self.cr1, self.cr4],
                                    self.gpw2, [])
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
        [resource, start, quantum] = self.bs.unhash_quantum_start(qs[1])
        assert_equal(resource, 'bar')
        assert_equal(start, 2)
        assert_equal(quantum, 1)
        [resource, start, quantum] = self.bs.unhash_quantum_start(qs[2])
        assert_equal(resource, 'bar')
        assert_equal(start, 3)
        assert_equal(quantum, 1)
        

    def test_schedule_contended_reservations_pass_1(self):
        self.r1.order = 1
        self.r2.order = 2
        bs2 = BipartiteScheduler([self.r1], ['foo']) 
        sr = bs2.schedule()
        assert_equal(sr, [self.r1])

