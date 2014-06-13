#!/usr/bin/env python

'''
test_fullscheduler_v5.py

Author: Sotiria Lampoudi
August 2012
'''

from nose.tools import assert_equal
from adaptive_scheduler.kernel.timepoint import *
from adaptive_scheduler.kernel.intervals import *
from adaptive_scheduler.kernel.fullscheduler_v6 import *
from adaptive_scheduler.kernel.reservation_v3 import Reservation_v3, CompoundReservation_v2
import copy

class TestFullScheduler_v6(object):

    def setup(self):
        s1 = Intervals([Timepoint(1, 'start'),
                        Timepoint(2, 'end')]) # 1-2
        s2 = Intervals([Timepoint(2, 'start'),
                        Timepoint(4, 'end')]) # --2--4
        s3 = copy.copy(s1)
        s4 = copy.copy(s1)
        s5 = copy.copy(s2)
        s6 = copy.copy(s1)
        s7 = copy.copy(s1)
        s8 = copy.copy(s1)
        s9 = copy.copy(s2)
        s10 = Intervals([Timepoint(1, 'start'), 
                         Timepoint(10, 'end')])
        s11 = copy.copy(s10)
        s12 = copy.copy(s10)
        s13 = copy.copy(s10)

        self.r1 = Reservation_v3(1, 1, {'foo': s1})
        self.r2 = Reservation_v3(2, 2, {'bar': s2})
        self.r3 = Reservation_v3(1, 1, {'foo': s3})
        self.r4 = Reservation_v3(1, 1, {'foo': s4})
        self.r5 = Reservation_v3(2, 2, {'bar': s5})
        self.r6 = Reservation_v3(1, 2, {'bar': s5})
        self.r7 = Reservation_v3(1, 1, {'bar': s6, 'foo' : s5})
        self.r8 = Reservation_v3(1, 1, {'foo': s6, 'bar' : s7})
        self.r9 = Reservation_v3(1, 1, {'foo': s8})
        self.r10 = Reservation_v3(2, 2, {'bar': s9})
        self.r11 = Reservation_v3(1, 1, {'bar': s10})
        self.r12 = Reservation_v3(1, 1, {'bar': s11})
        self.r13 = Reservation_v3(1, 1, {'bar': s12})        
        self.r14 = Reservation_v3(1, 1, {'bar': s13})
        
        self.r15 = Reservation_v3(1, 9, {'bar': s13})
        self.r16 = Reservation_v3(1, 9, {'foo': s13})
        self.r17 = Reservation_v3(2, 9, {'bar': s13})
        self.r18 = Reservation_v3(3, 9, {'foo': s13})



        self.cr1 = CompoundReservation_v2([self.r1])
        self.cr2 = CompoundReservation_v2([self.r3, self.r2], 'and')
        self.cr3 = CompoundReservation_v2([self.r4])
        self.cr4 = CompoundReservation_v2([self.r5])
        self.cr5 = CompoundReservation_v2([self.r4, self.r5], 'oneof')
        self.cr6 = CompoundReservation_v2([self.r3])
        self.cr7 = CompoundReservation_v2([self.r2])
        self.cr8 = CompoundReservation_v2([self.r4, self.r6], 'oneof')
        self.cr9 = CompoundReservation_v2([self.r4, self.r1, self.r3], 'oneof')
        self.cr10 = CompoundReservation_v2([self.r7])
        self.cr11 = CompoundReservation_v2([self.r8])
        self.cr12 = CompoundReservation_v2([self.r9, self.r10], 'oneof')
        self.cr13 = CompoundReservation_v2([self.r11])
        self.cr14 = CompoundReservation_v2([self.r12])
        self.cr15 = CompoundReservation_v2([self.r13])
        self.cr16 = CompoundReservation_v2([self.r14])

        self.cr17 = CompoundReservation_v2([self.r15,self.r16],'and')
        self.cr18 = CompoundReservation_v2([self.r17])
        self.cr19 = CompoundReservation_v2([self.r18])

        self.gpw2 = {}
        self.gpw2['foo'] = Intervals([Timepoint(1, 'start'), Timepoint(10, 'end')], 'free')
        self.gpw2['bar'] = Intervals([Timepoint(1, 'start'), Timepoint(10, 'end')], 'free')

        self.gpw3 = {}
        self.gpw3['foo'] = Intervals([Timepoint(5, 'start'), Timepoint(10, 'end')], 'free')
        self.gpw3['bar'] = Intervals([Timepoint(5, 'start'), Timepoint(10, 'end')], 'free')

        self.gpw4 = {}
        self.gpw4['bar'] = Intervals([Timepoint(1, 'start'), Timepoint(10, 'end')], 'free')

        self.fs1 = FullScheduler_v6([self.cr1, self.cr2, self.cr3], 
                                    self.gpw2, [], 1)
        self.fs2 = FullScheduler_v6([self.cr1, self.cr4],
                                    self.gpw2, [], 1)
        self.fs3 = FullScheduler_v6([self.cr5],
                                    self.gpw2, [], 1)
        self.fs4 = FullScheduler_v6([self.cr8, self.cr6, self.cr7],
                                    self.gpw2, [], 1)
        self.fs5 = FullScheduler_v6([self.cr10, self.cr2, self.cr3], 
                                    self.gpw2, [], 1)
        self.fs6 = FullScheduler_v6([self.cr11, self.cr2, self.cr3], 
                                    self.gpw2, [], 1)
        self.fs7 = FullScheduler_v6([self.cr12],
                                    self.gpw3, [], 1)
        self.fs8 = FullScheduler_v6([self.cr13, self.cr14, self.cr15, self.cr16],
                                    self.gpw4, [], 1)
        self.fs9 = FullScheduler_v6([self.cr17, self.cr18, self.cr19],
                                    self.gpw2, [], 1)

    def test_schedule_noneofand(self):
        self.fs9.schedule_all()
        assert_equal(self.r15.scheduled,False)
        assert_equal(self.r16.scheduled,False)
        assert_equal(self.r17.scheduled,True)
        assert_equal(self.r18.scheduled,True)

    def test_schedule_all_4inarow(self):
#        print self.fs8.reservation_list
        self.fs8.schedule_all()
#        print self.fs8.reservation_list
        assert_equal(self.r11.scheduled, True)
        assert_equal(self.r12.scheduled, True)
        assert_equal(self.r13.scheduled, True)
        assert_equal(self.r14.scheduled, True)


    def test_schedule_all_1(self):
        d = self.fs1.schedule_all()
        assert_equal(self.r1.scheduled, False)
        assert_equal(self.r2.scheduled, True)
        assert_equal(self.r3.scheduled, True)
        assert_equal(self.r4.scheduled, False)


    def test_schedule_all_multi_resource(self):
        d = self.fs5.schedule_all()
        assert_equal(self.r7.scheduled, True)
        assert_equal(self.r2.scheduled, True)
        assert_equal(self.r3.scheduled, True)
        assert_equal(self.r4.scheduled, False)


    def test_schedule_all_multi_resource_2(self):
        d = self.fs6.schedule_all()
        assert_equal(self.r8.scheduled, True)
        assert_equal(self.r2.scheduled, True)
        assert_equal(self.r3.scheduled, True)
        assert_equal(self.r4.scheduled, False)


    def test_schedule_all_2(self):
        d = self.fs2.schedule_all()
        assert_equal(self.r1.scheduled, True)
        assert_equal(self.r5.scheduled, True)
        

    def test_schedule_all_3(self):
        d = self.fs3.schedule_all()
        assert_equal(self.r4.scheduled, False)
        assert_equal(self.r5.scheduled, True)


    def test_schedule_all_4(self):
        d = self.fs4.schedule_all()
        assert_equal(self.r2.scheduled, True)
        assert_equal(self.r6.scheduled, False)
        # either r3 or r4 should be scheduled, not both
        if self.r3.scheduled:
            assert_equal(self.r4.scheduled, False)
        else:
            assert_equal(self.r4.scheduled, True)


    def test_schedule_triple_oneof(self):
        slice_dict = {}
        slice_dict['foo'] = [0,1]
        slice_dict['bar'] = [0,1]
        fs = FullScheduler_v6([self.cr9],
                              self.gpw2, [], 1)
        s = fs.schedule_all()
        # only one should be scheduled

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

        fs = FullScheduler_v6([cr], gpw, [], 60)
        schedule = fs.schedule_all()


    def test_schedule_all_gaw(self):
        d = self.fs7.schedule_all()
        assert_equal(self.r9.scheduled, False)
        assert_equal(self.r10.scheduled, False)
        
    
    def test_schedule_order_dependent_resources(self):
        s1 = Intervals([Timepoint(0, 'start'), Timepoint(1000, 'end')])
        s2 = Intervals([Timepoint(0, 'start'), Timepoint(1000, 'end')])
        r1 = Reservation_v3(1, 30, {'foo': s1, 'goo': s2})
        cr = CompoundReservation_v2([r1], 'single')
        gpw = {}
        gpw['goo'] = Intervals([Timepoint(250, 'start'), Timepoint(750, 'end')])
        gpw['foo'] = Intervals([])#[Timepoint(1500, 'start'), Timepoint(2000, 'end')])
        
        fs = FullScheduler_v6([cr], gpw, [], 60)
        schedule = fs.schedule_all()
        print schedule
        assert_equal(1, len(schedule['goo']))

        s1 = Intervals([Timepoint(0, 'start'), Timepoint(1000, 'end')])
        s2 = Intervals([Timepoint(0, 'start'), Timepoint(1000, 'end')])
        r1 = Reservation_v3(1, 30, {'foo': s1, 'goo': s2})
        cr = CompoundReservation_v2([r1], 'single')
        gpw = {}
        gpw['goo'] = Intervals([Timepoint(250, 'start'), Timepoint(750, 'end')])
        gpw['foo'] = Intervals([Timepoint(1500, 'start'), Timepoint(2000, 'end')])

        fs = FullScheduler_v6([cr], gpw, [], 60)
        schedule = fs.schedule_all()
        print schedule
        assert_equal(1, len(schedule['goo']))
        
        s1 = Intervals([Timepoint(0, 'start'), Timepoint(1000, 'end')])
        s2 = Intervals([Timepoint(0, 'start'), Timepoint(1000, 'end')])
        r1 = Reservation_v3(1, 30, {'foo': s1, 'goo': s2})
        cr = CompoundReservation_v2([r1], 'single')
        gpw = {}
        gpw['foo'] = Intervals([Timepoint(250, 'start'), Timepoint(750, 'end')])
        gpw['goo'] = Intervals([Timepoint(1500, 'start'), Timepoint(2000, 'end')])

        
        fs = FullScheduler_v6([cr], gpw, [], 60)
        schedule = fs.schedule_all()
        print schedule
        assert_equal(1, len(schedule['foo']))

        s1 = Intervals([Timepoint(0, 'start'), Timepoint(1000, 'end')])
        s2 = Intervals([Timepoint(0, 'start'), Timepoint(1000, 'end')])
        r1 = Reservation_v3(1, 30, {'foo': s1, 'goo': s2})
        cr = CompoundReservation_v2([r1], 'single')
        gpw = {}
        gpw['foo'] = Intervals([Timepoint(250, 'start'), Timepoint(750, 'end')])
        gpw['goo'] = Intervals([Timepoint(1500, 'start'), Timepoint(2000, 'end')])

        
        fs = FullScheduler_v6([cr], gpw, [], 60)
        schedule = fs.schedule_all()
        print schedule
        assert_equal(1, len(schedule['foo'])) 
        
        
    def test_schedule_no_available_windows(self):
        s1 = Intervals([Timepoint(0, 'start'), Timepoint(1000, 'end')])
        s2 = Intervals([Timepoint(0, 'start'), Timepoint(1000, 'end')])
        r1 = Reservation_v3(1, 30, {'foo': s1, 'goo': s2})
        cr = CompoundReservation_v2([r1], 'single')
        gpw = {}
        gpw['goo'] = Intervals([Timepoint(250, 'start'), Timepoint(750, 'end')])
        
        fs = FullScheduler_v6([cr], gpw, [], 60)
        schedule = fs.schedule_all()
