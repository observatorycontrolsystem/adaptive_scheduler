#!/usr/bin/env python

'''
test_fullscheduler_v3.py

Author: Sotiria Lampoudi
June 2012
'''

from nose.tools import assert_equal
import copy
from adaptive_scheduler.kernel.fullscheduler_v3 import *

class TestFullScheduler_v3(object):

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
        self.r6 = Reservation_v3(1, 2, {'bar': s5})

        self.cr1 = CompoundReservation_v2([self.r1])
        self.cr2 = CompoundReservation_v2([self.r3, self.r2], 'and')
        self.cr3 = CompoundReservation_v2([self.r4])
        self.cr4 = CompoundReservation_v2([self.r5])
        self.cr5 = CompoundReservation_v2([self.r4, self.r5], 'oneof')
        self.cr6 = CompoundReservation_v2([self.r3])
        self.cr7 = CompoundReservation_v2([self.r2])
        self.cr8 = CompoundReservation_v2([self.r4, self.r6], 'oneof')
        self.cr9 = CompoundReservation_v2([self.r4, self.r1, self.r3], 'oneof')
        self.gpw = {}
        self.gpw['foo'] = [Timepoint(1, 'start'), Timepoint(5, 'end')]
        
        self.gpw2 = {}
        self.gpw2['foo'] = Intervals([Timepoint(1, 'start'), Timepoint(5, 'end')], 'free')
        self.gpw2['bar'] = Intervals([Timepoint(1, 'start'), Timepoint(5, 'end')], 'free')
        
        self.fs1 = FullScheduler_v3([self.cr1, self.cr2, self.cr3], 
                                    self.gpw2, [])
        self.fs2 = FullScheduler_v3([self.cr1, self.cr4],
                                    self.gpw2, [])

        self.fs3 = FullScheduler_v3([self.cr5],
                                    self.gpw2, [])
        self.fs4 = FullScheduler_v3([self.cr8, self.cr6, self.cr7],
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
        


    def test_schedule_multi_resource_oneof(self):
        schedule_dict = self.fs4.schedule_all()


    def test_schedule_5_7_2012(self):
        s1 = Intervals([Timepoint(93710, 'start'), 
                        Timepoint(114484, 'end'),
                        Timepoint(180058, 'start'), 
                        Timepoint(200648, 'end')])
        r1 = Reservation_v3(1, 30, {'foo': s1})
        s2 = copy.copy(s1)
        r2 = Reservation_v3(1, 30, {'goo': s2})

        cr = CompoundReservation_v2([r1,r2], 'oneof')
        gpw = {}
        gpw['foo'] = Intervals([Timepoint(90000, 'start'), 
                                Timepoint(201000, 'end')])
        gpw['goo'] = Intervals([Timepoint(90000, 'start'), 
                                Timepoint(201000, 'end')])
        fs = FullScheduler_v3([cr], gpw, [])
        schedule = fs.schedule_all()


    def test_schedule_all_1(self):
	''' this test illustrates an example in which, if we were considering 
	alternatives with the same weight, we might have caught the 'and'
	and achieved a greater score'''
        d = self.fs1.schedule_all()

    def test_schedule_all_2(self):
        self.fs2.schedule_all()
        
        
    def test_schedule_all_3(self):
        self.fs3.schedule_all()


    def test_schedule_all_4(self):
        self.fs4.schedule_all()


    def test_schedule_triple_oneof(self):
        fs = FullScheduler_v3([self.cr9],
                              self.gpw2, [])
        s = fs.schedule_all()
        assert_equal(len(s['foo']), 1)
