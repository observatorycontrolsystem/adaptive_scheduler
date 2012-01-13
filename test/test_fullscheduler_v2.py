#!/usr/bin/env python

'''
test_fullscheduler_v2.py

Author: Sotiria Lampoudi
November 2011
'''

from nose.tools import assert_equal
import copy
from adaptive_scheduler.kernel.fullscheduler_v2 import *

class TestFullScheduler_v2(object):

    def setup(self):
        s1 = Intervals([Timepoint(1, 'start'),
                        Timepoint(2, 'end')]) # 1-2
        s2 = Intervals([Timepoint(2, 'start'),
                        Timepoint(4, 'end')]) # --2--4
        s3 = copy.copy(s1)
        s4 = copy.copy(s1)
        s5 = copy.copy(s2)

        self.r1 = Reservation_v2(1, 1, 'foo', s1)
        self.r2 = Reservation_v2(2, 2, 'bar', s2)
        self.r3 = Reservation_v2(1, 1, 'foo', s3)
        self.r4 = Reservation_v2(1, 1, 'foo', s4)
        self.r5 = Reservation_v2(2, 2, 'bar', s5)
        self.r6 = Reservation_v2(1, 2, 'bar', s5)

        self.cr1 = CompoundReservation_v2([self.r1])
        self.cr2 = CompoundReservation_v2([self.r3, self.r2], 'and')
        self.cr3 = CompoundReservation_v2([self.r4])
        self.cr4 = CompoundReservation_v2([self.r5])
        self.cr5 = CompoundReservation_v2([self.r4, self.r5], 'oneof')
        self.cr6 = CompoundReservation_v2([self.r3])
        self.cr7 = CompoundReservation_v2([self.r2])
        self.cr8 = CompoundReservation_v2([self.r4, self.r6], 'oneof')
        self.gpw = {}
        self.gpw['foo'] = [Timepoint(1, 'start'), Timepoint(5, 'end')]
        
        self.gpw2 = {}
        self.gpw2['foo'] = Intervals([Timepoint(1, 'start'), Timepoint(5, 'end')], 'free')
        self.gpw2['bar'] = Intervals([Timepoint(1, 'start'), Timepoint(5, 'end')], 'free')
        
        self.fs1 = FullScheduler_v2([self.cr1, self.cr2, self.cr3], 
                                    self.gpw2, [])
        self.fs2 = FullScheduler_v2([self.cr1, self.cr4],
                                    self.gpw2, [])

        self.fs3 = FullScheduler_v2([self.cr5],
                                    self.gpw2, [])
        self.fs4 = FullScheduler_v2([self.cr8, self.cr6, self.cr7],
                                    self.gpw2, [])
        

    def test_create(self):
        assert_equal(self.fs1.compound_reservation_list, [self.cr1, self.cr2, self.cr3])
        assert_equal(self.fs1.globally_possible_windows_dict, self.gpw2)
        assert_equal(self.fs1.contractual_obligation_list, [])
        assert_equal(self.fs1.schedule_dict_free['foo'].timepoints[0].time, 1)
        assert_equal(self.fs1.schedule_dict_free['foo'].timepoints[0].type, 
                     'start')
        assert_equal(self.fs1.schedule_dict_free['foo'].timepoints[1].time, 5)
        assert_equal(self.fs1.schedule_dict_free['foo'].timepoints[1].type, 
                     'end')
        

    def test_create_2(self):
        assert_equal(self.fs2.resource_list, ['foo', 'bar'])
        assert_equal(self.fs2.schedule_dict['foo'], [])
        assert_equal(self.fs2.schedule_dict['bar'], [])


    def test_convert_compound_to_simple_1(self):
        assert_equal(self.fs1.reservation_list[0], self.r1)
        assert_equal(self.fs1.reservation_list[1], self.r3)
        assert_equal(self.fs1.reservation_list[2], self.r2)
        assert_equal(self.fs1.reservation_list[3], self.r4)
        assert_equal(self.fs1.and_constraints[0][0], self.r3)
        assert_equal(self.fs1.and_constraints[0][1], self.r2)


    def test_convert_compound_to_simple_2(self):
        assert_equal(self.fs3.reservation_list[0], self.r4)
        assert_equal(self.fs3.reservation_list[1], self.r5)
        assert_equal(self.fs3.oneof_constraints[0][0], self.r4)
        assert_equal(self.fs3.oneof_constraints[0][1], self.r5)


    def test_cluster_and_order_reservations(self):
        n = self.fs1.cluster_and_order_reservations(2)
        assert_equal(n, 2)
        assert_equal(self.fs1.reservation_list[0].order, 1)
        assert_equal(self.fs1.reservation_list[1].order, 1)
        assert_equal(self.fs1.reservation_list[2].order, 2)
        assert_equal(self.fs1.reservation_list[3].order, 1)
        

    def test_order_equals_1(self):
        self.fs1.current_order = 2
        self.r2.order = 2
        assert self.fs1.order_equals(self.r2)


    def test_enforce_and_constraints_1(self):
        self.fs1.enforce_and_constraints()

        
    def test_enforce_and_constraints_2(self):
	self.r3.schedule(1, 1, 'foo', 'test')
        self.fs1.commit_reservation_to_schedule(self.r3)
        self.fs1.enforce_and_constraints()
        assert_equal(self.fs1.schedule_dict['foo'], [])


    def test_enforce_and_constraints_3(self):
	self.r3.schedule(1, 1, 'foo', 'test')
	self.r2.schedule(2, 2, 'bar', 'test')
        self.fs1.commit_reservation_to_schedule(self.r3)
        self.fs1.commit_reservation_to_schedule(self.r2)
        self.fs1.enforce_and_constraints()
        assert_equal(self.fs1.schedule_dict['foo'], [self.r3])
        assert_equal(self.fs1.schedule_dict['bar'], [self.r2])


    def test_commit_reservation_to_schedule_1(self):
        self.r1.schedule(1, 1, 'foo', 'test')
        self.fs2.commit_reservation_to_schedule(self.r1)
        assert_equal(self.fs2.schedule_dict['foo'][0].scheduled_start, 1)
        assert_equal(self.fs2.schedule_dict_busy['foo'].timepoints[0].time, 1)
        assert_equal(self.fs2.schedule_dict_busy['foo'].timepoints[0].type, 'start')
        assert_equal(self.fs2.schedule_dict_busy['foo'].timepoints[1].time, 2)
        assert_equal(self.fs2.schedule_dict_busy['foo'].timepoints[1].type, 'end')
        assert_equal(self.fs2.schedule_dict_free['foo'].timepoints[0].time, 2)
        assert_equal(self.fs2.schedule_dict_free['foo'].timepoints[0].type, 'start')
        assert_equal(self.fs2.schedule_dict_free['foo'].timepoints[1].time, 5)
        assert_equal(self.fs2.schedule_dict_free['foo'].timepoints[1].type, 'end')


    def test_uncommit_reservation_from_schedule(self):
        assert self.r1 in self.fs2.unscheduled_reservation_list
	self.r1.schedule(1, 1, 'foo', 'test')
        self.fs2.commit_reservation_to_schedule(self.r1)
        assert self.r1 not in self.fs2.unscheduled_reservation_list
        self.fs2.uncommit_reservation_from_schedule(self.r1)
        assert self.r1 in self.fs2.unscheduled_reservation_list


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
#         print self.r2.get_ID()
#         print self.r3.get_ID()
#         print self.r4.get_ID()
#         print self.r6.get_ID()
#         print schedule_dict
