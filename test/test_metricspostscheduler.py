#!/usr/bin/env python

'''
test_metricspostscheduler.py

Author: Sotiria Lampoudi
May 2012
'''

from nose.tools import assert_equal
import copy
from adaptive_scheduler.kernel.metricspostscheduler import *
from adaptive_scheduler.kernel.fullscheduler_v2 import *

class TestMetricsPostScheduler(object):

    def setup(self):
        s1 = Intervals([Timepoint(1, 'start'),
                        Timepoint(2, 'end')]) # 1-2
        s2 = Intervals([Timepoint(2, 'start'),
                        Timepoint(4, 'end')]) # --2--4
        s3 = copy.copy(s1)
        s4 = copy.copy(s1)

        self.r1 = Reservation_v2(1, 1, 'foo', s1)
        self.r2 = Reservation_v2(2, 2, 'bar', s2)
        self.r3 = Reservation_v2(1, 1, 'foo', s3)
        self.r4 = Reservation_v2(1, 1, 'foo', s4)

        self.cr1 = CompoundReservation_v2([self.r1])
        self.cr2 = CompoundReservation_v2([self.r3, self.r2], 'and')
        self.cr3 = CompoundReservation_v2([self.r4])
        
        self.gpw2 = {}
        self.gpw2['foo'] = Intervals([Timepoint(1, 'start'), Timepoint(5, 'end')], 'free')
        self.gpw2['bar'] = Intervals([Timepoint(1, 'start'), Timepoint(5, 'end')], 'free')
        
        self.fs1 = FullScheduler_v2([self.cr1, self.cr2, self.cr3], 
                                    self.gpw2, [])
        self.sched1_d = self.fs1.schedule_all()
        self.mv1 = MetricsPostScheduler([self.cr1, self.cr2, self.cr3], 
                                       self.gpw2, [], self.sched1_d)

    def test_create(self):
        assert_equal(self.sched1_d, self.mv1.schedule_dict)
        assert_equal(self.mv1.compound_reservation_list, [self.cr1, self.cr2, self.cr3])


    
class TestMetricsPostSchedulerScalar(object):

    def setup(self):
        s1 = Intervals([Timepoint(1, 'start'),
                        Timepoint(2, 'end')]) # 1-2
        s2 = Intervals([Timepoint(2, 'start'),
                        Timepoint(4, 'end')]) # --2--4
        s3 = copy.copy(s1)
        s4 = copy.copy(s1)

        self.r1 = Reservation_v2(1, 1, 'foo', s1)
        self.r2 = Reservation_v2(2, 2, 'bar', s2)
        self.r3 = Reservation_v2(1, 1, 'foo', s3)
        self.r4 = Reservation_v2(1, 1, 'foo', s4)

        self.cr1 = CompoundReservation_v2([self.r1])
        self.cr2 = CompoundReservation_v2([self.r3, self.r2], 'and')
        self.cr3 = CompoundReservation_v2([self.r4])
        
        self.gpw2 = {}
        self.gpw2['foo'] = Intervals([Timepoint(1, 'start'), Timepoint(5, 'end')], 'free')
        self.gpw2['bar'] = Intervals([Timepoint(1, 'start'), Timepoint(5, 'end')], 'free')
        
        self.fs1 = FullScheduler_v2([self.cr1, self.cr2, self.cr3], 
                                    self.gpw2, [])
        self.sched1_d = self.fs1.schedule_all()
#        print self.sched1_d
        self.mv1 = MetricsPostSchedulerScalar([self.cr1, self.cr2, self.cr3],
                                       self.gpw2, [], self.sched1_d)


    def test_get_fraction_of_crs_scheduled(self):
        assert_equal(self.mv1.get_fraction_of_crs_scheduled(), float(1)/float(3))


#class TestMetricsPostSchedulerVector(object):

