#!/usr/bin/env python
from __future__ import division

from nose.tools import eq_, assert_equal, raises

# Import the module to test
from adaptive_scheduler.maxheap import heappush, heappop, _negate



class test_maxheap(object):

    def __init__(self):
        pass
    
    
    def setup(self):
        self.heap = []
    
    def teardown(self):
        pass
        
    
    def test_can_negate_a_scalar(self):
        assert_equal(_negate(2), -2)

    def test_can_negate_a_tuple(self):
        assert_equal(_negate((2,'foo')), (-2, 'foo'))

    def test_heap_push_and_pop_integers(self):
        heappush(self.heap, 3)
        heappush(self.heap, 20)
        heappush(self.heap, -3)
        heappush(self.heap, 5)

        assert_equal(heappop(self.heap), 20)
        assert_equal(heappop(self.heap), 5)
        assert_equal(heappop(self.heap), 3)
        assert_equal(heappop(self.heap), -3)


    def test_heap_push_and_pop_integers_interleaved(self):
        heappush(self.heap, 3)
        heappush(self.heap, 20)
        
        assert_equal(heappop(self.heap), 20)
        
        heappush(self.heap, -3)
        heappush(self.heap, 2)
        assert_equal(heappop(self.heap), 3)


    def test_heap_push_and_pop_tuples(self):
        ratings = ( (1, 'python'), (14, 'perl'), (12, 'lisp') )
    
        heappush(self.heap, ratings[0])
        heappush(self.heap, ratings[1]),
        heappush(self.heap, ratings[2])
        
        assert_equal(heappop(self.heap), ratings[1])
        assert_equal(heappop(self.heap), ratings[2])
        assert_equal(heappop(self.heap), ratings[0])
