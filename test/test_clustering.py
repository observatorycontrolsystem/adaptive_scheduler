#!/usr/bin/env python

'''
test_clustering.py

Author: Sotiria Lampoudi
November 2011
'''

from nose.tools import assert_equal

from adaptive_scheduler.kernel.clustering import *
from adaptive_scheduler.kernel.reservation_v2 import *

class TestClustering(object):

    def setup(self):
        s1 = Intervals([Timepoint(1, 'start'),
                        Timepoint(2, 'end')]) # 1-2

        s2 = Intervals([Timepoint(2, 'start'),
                        Timepoint(4, 'end')]) # --2--4

        self.r1 = Reservation_v2(1, 1, 'foo', s1)
        self.r2 = Reservation_v2(2, 2, 'foo', s1)
        self.r3 = Reservation_v2(3, 1, 'foo', s1)
        
        self.c1 = Clustering([self.r1, self.r2, self.r3])


    def test_create(self):
        assert_equal(self.c1.reservation_list[0], self.r1)
        assert_equal(self.c1.reservation_list[1], self.r2)
        assert_equal(self.c1.reservation_list[2], self.r3)
        assert_equal(self.c1.reservation_list[0].order, 1)
        assert_equal(self.c1.reservation_list[1].order, 1)
        assert_equal(self.c1.reservation_list[2].order, 1)


    def test_max_priority(self):
        assert_equal(self.c1.max_priority(), 3)


    def test_min_priority(self):
        assert_equal(self.c1.min_priority(), 1)


    def test_cut_into_n_by_prio_1(self):
        self.c1.cut_into_n_by_priority(1)
        assert_equal(self.r1.order, 1)
        assert_equal(self.r2.order, 1)
        assert_equal(self.r3.order, 1)


    def test_cut_into_n_by_prio_2(self):
        self.c1.cut_into_n_by_priority(2)
        assert_equal(self.r1.order, 2)
        assert_equal(self.r2.order, 1)
        assert_equal(self.r3.order, 1)


    def test_cut_into_n_by_prio_3(self):
        self.c1.cut_into_n_by_priority(3)
        assert_equal(self.r1.order, 3)
        assert_equal(self.r2.order, 2)
        assert_equal(self.r3.order, 1)


    def test_cluster_into_n_by_prio_1(self):
        self.c1.cluster_into_n_by_priority(1)
        assert_equal(self.r1.order, 1)
        assert_equal(self.r2.order, 1)
        assert_equal(self.r3.order, 1)


    def test_cluster_into_n_by_prio_2(self):
        self.c1.cluster_into_n_by_priority(2)
        assert_equal(self.r1.order, 2)
        assert_equal(self.r2.order, 2)
        assert_equal(self.r3.order, 1)


    def test_cluster_into_n_by_prio_3(self):
        self.c1.cluster_into_n_by_priority(3)
        assert_equal(self.r1.order, 3)
        assert_equal(self.r2.order, 2)
        assert_equal(self.r3.order, 1)
        

    def test_cluster_adaptively_by_what(self):
        n = self.c1.cluster_adaptively_by_what('priority')
        print n
