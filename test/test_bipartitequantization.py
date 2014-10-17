#!/usr/bin/env python

'''
test_bipartitequantization.py

Author: Sotiria Lampoudi
Dec 2012
'''

from nose.tools import assert_equal

from adaptive_scheduler.kernel.bipartitequantization import *

class TestBipartiteQuantization(object):

    def setup(self):
        s1 = Intervals([Timepoint(1, 'start'),
                        Timepoint(2, 'end')]) # 1-2

        s2 = Intervals([Timepoint(2, 'start'),
                        Timepoint(4, 'end')]) # --2--4

        self.r1 = Reservation_v3(1, 1, {'foo': s1})
        self.r2 = Reservation_v3(2, 2, {'bar': s2})
    
        self.bs = BipartiteQuantization()


    def test_hash_and_unhash_quantum_start(self):
        tmp = self.bs.hash_quantum_start('foo', 2, 1)
        [resource, start, quantum] = self.bs.unhash_quantum_start(tmp)
        assert_equal(resource, 'foo')
        assert_equal(start, 2)
        assert_equal(quantum, 1)
       
 
    def test_quantize_windows_1(self):
        qs = self.bs.quantize_windows(self.r1, 1, 'foo')
        [resource, start, quantum] = self.bs.unhash_quantum_start(qs[0])
        assert_equal(resource, 'foo')
        assert_equal(start, 1)
        assert_equal(quantum, 1)


    def test_quantize_windows_2(self):
        qs = self.bs.quantize_windows(self.r1, 1, 'foo')
        [resource, start, quantum] = self.bs.unhash_quantum_start(qs[0])
        assert_equal(resource, 'foo')
        assert_equal(start, 1)
        assert_equal(quantum, 1)

    def test_get_quantum_starts_1(self):
        t1=Timepoint(1,'start');
        t2=Timepoint(3, 'end');
        t5=Timepoint(4, 'start');
        t6=Timepoint(5, 'end');

        i1=Intervals([t1, t2, t5, t6], 'free')

        qs = self.bs.get_quantum_starts(i1,1)
        assert_equal(qs[0], 1)
        assert_equal(qs[1], 2)
        assert_equal(qs[2], 4)


    def test_get_quantum_starts_2(self):
        '''
        Impossible to align w/ start of quantum
        '''
        t1=Timepoint(1,'start');
        t2=Timepoint(3, 'end');
        t5=Timepoint(4, 'start');
        t6=Timepoint(5, 'end');

        i1=Intervals([t1, t2, t5, t6], 'free')

        qs = self.bs.get_quantum_starts(i1,2)
        assert_equal(qs, [])


    def test_get_quantum_starts_3(self):
        t1 = Timepoint(1, 'start')
        t2 = Timepoint(2, 'end')
        i1 = Intervals([t1,t2], 'free')
        qs = self.bs.get_quantum_starts(i1,2)
	assert_equal(qs, [])
