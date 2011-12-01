#!/usr/bin/env python

'''
test_timepoint.py 

Author: Sotiria Lampoudi
August 2011
'''

from nose.tools import assert_equal

from adaptive_scheduler.kernel.timepoint import *

class TestTimepoint(object):
    def setup(self):
        self.tp1 = Timepoint(10, 'start')
        self.tp2 = Timepoint(10, 'end', 'foo')
        self.tp3 = Timepoint(20, 'start')

        
    def test_create_start(self):
        assert_equal(self.tp1.time, 10)
        assert_equal(self.tp1.type, 'start')
        assert_equal(self.tp1.resource, None)


    def test_create_end(self):
        assert_equal(self.tp2.time, 10)
        assert_equal(self.tp2.type, 'end')
        assert_equal(self.tp2.resource, 'foo')


    def test_sort_unequal_time(self):
        l1 = [self.tp1, self.tp3]
        l1.sort()
        assert_equal(l1[0].time, 10)


    def test_sort_equal_time(self):
        l2 = [self.tp1, self.tp2]
        l2.sort()
        assert_equal(l2[0].type, 'end')
