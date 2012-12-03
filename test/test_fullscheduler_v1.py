#!/usr/bin/env python

'''
test_fullscheduler_v1.py

Author: Sotiria Lampoudi
November 2011
'''

from nose.tools import assert_equal
import copy
from adaptive_scheduler.kernel.fullscheduler_v1 import *

class TestFullScheduler_v1(object):

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
        
        self.fs1 = FullScheduler_v1([self.cr1, self.cr2, self.cr3], 
                                    self.gpw2, [])
        self.fs2 = FullScheduler_v1([self.cr1, self.cr4],
                                    self.gpw2, [])

        self.fs3 = FullScheduler_v1([self.cr5],
                                    self.gpw2, [])


    def test_schedule_contended_reservations_pass_1(self):
        self.r1.order = 1
        self.r5.order = 2
        self.fs2.schedule_contended_reservations_pass(1)
        assert_equal(self.fs2.schedule_dict['foo'][0].get_ID(), self.r1.get_ID())
        assert_equal(self.fs2.schedule_dict['foo'][0].scheduled_start, 1)
        assert_equal(self.fs2.schedule_dict_busy['foo'].timepoints[0].time, 1)
        assert_equal(self.fs2.schedule_dict_busy['foo'].timepoints[0].type, 'start')
        assert_equal(self.fs2.schedule_dict_busy['foo'].timepoints[1].time, 2)
        assert_equal(self.fs2.schedule_dict_busy['foo'].timepoints[1].type, 'end')
        assert_equal(self.fs2.schedule_dict_free['foo'].timepoints[0].time, 2)
        assert_equal(self.fs2.schedule_dict_free['foo'].timepoints[0].type, 'start')
        assert_equal(self.fs2.schedule_dict_free['foo'].timepoints[1].time, 5)
        assert_equal(self.fs2.schedule_dict_free['foo'].timepoints[1].type, 'end')
        

    def test_schedule_contended_reservations_1(self):
        self.fs2.schedule_contended_reservations()
        assert_equal(len(self.fs2.schedule_dict['bar']), 1)
        assert_equal(self.fs2.schedule_dict['bar'][0].scheduled_start, 2)
        assert_equal(self.fs2.schedule_dict_busy['bar'].timepoints[0].time, 2)
        assert_equal(self.fs2.schedule_dict_busy['bar'].timepoints[0].type, 'start')
        assert_equal(self.fs2.schedule_dict_busy['bar'].timepoints[1].time, 4)
        assert_equal(self.fs2.schedule_dict_busy['bar'].timepoints[1].type, 'end')
        assert_equal(self.fs2.schedule_dict_free['bar'].timepoints[0].time, 1)
        assert_equal(self.fs2.schedule_dict_free['bar'].timepoints[0].type, 'start')
        assert_equal(self.fs2.schedule_dict_free['bar'].timepoints[1].time, 2)
        assert_equal(self.fs2.schedule_dict_free['bar'].timepoints[1].type, 'end')
        assert_equal(self.fs2.schedule_dict_free['bar'].timepoints[2].time, 4)
        assert_equal(self.fs2.schedule_dict_free['bar'].timepoints[2].type, 'start')
        assert_equal(self.fs2.schedule_dict_free['bar'].timepoints[3].time, 5)
        assert_equal(self.fs2.schedule_dict_free['bar'].timepoints[3].type, 'end')
        
