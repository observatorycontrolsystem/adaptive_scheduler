#!/usr/bin/env python

'''
test_fullscheduler_v1.py

Author: Sotiria Lampoudi
November 2011
'''

from nose.tools import assert_equal

from fullscheduler_v1 import *

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



    def test_create(self):
        assert_equal(self.fs1.compound_reservation_list, [self.cr1, self.cr2, self.cr3])
        assert_equal(self.fs1.globally_possible_windows_dict, self.gpw)
        assert_equal(self.fs1.contractual_obligation_list, [])
        assert_equal(self.fs1.schedule_dict_free['foo'].timepoints[0].time, 1)
        assert_equal(self.fs1.schedule_dict_free['foo'].timepoints[0].type, 
                     'start')
        

    def test_convert_compound_to_simple(self):
        assert_equal(self.fs1.reservation_list[0], self.r1)
        assert_equal(self.fs1.reservation_list[1], self.r1)
        assert_equal(self.fs1.reservation_list[2], self.r2)
        assert_equal(self.fs1.reservation_list[3], self.r1)
        assert_equal(self.fs1.constraints[0][0], self.r1)
        assert_equal(self.fs1.constraints[0][1], self.r2)
        

    def test_cluster_and_order_reservations(self):
        n = self.fs1.cluster_and_order_reservations(2)
        assert_equal(n, 2)
        assert_equal(self.fs1.reservation_list[0].order, 1)
        assert_equal(self.fs1.reservation_list[2].order, 2)
        

    def test_order_equals_1(self):
        self.fs1.current_order = 2
        self.r2.order = 2
        assert self.fs1.order_equals(self.r2)


    def test_uncommit_reservation_from_schedule(self):
        assert self.r1 in self.fs2.unscheduled_reservation_list
	self.r1.schedule(1, 'foo', 1)
        self.fs2.commit_reservation_to_schedule(self.r1)
        assert self.r1 not in self.fs2.unscheduled_reservation_list
        self.fs2.uncommit_reservation_from_schedule(self.r1)
        assert self.r1 in self.fs2.unscheduled_reservation_list


    def test_enforce_all_constraints_1(self):
        self.fs1.enforce_all_constraints()

        
    def test_enforce_all_constraints_2(self):
	self.r1.schedule(1, 'foo', 1)
        self.fs1.commit_reservation_to_schedule(self.r1)
        self.fs1.enforce_all_constraints()
        assert_equal(self.fs1.schedule_dict['foo'], [])


    def test_enforce_all_constraints_3(self):
	self.r1.schedule(1, 'foo', 1)
	self.r2.schedule(2, 'foo', 2)
        self.fs1.commit_reservation_to_schedule(self.r1)
        self.fs1.commit_reservation_to_schedule(self.r2)
        self.fs1.enforce_all_constraints()
        assert_equal(self.fs1.schedule_dict['foo'], [self.r1, self.r2])


    def test_commit_reservation_to_schedule_1(self):
        self.r1.schedule(1, 'foo', 1)
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
        


    def test_schedule_contended_reservations_pass_1(self):
        self.r1.order = 1
        self.r2.order = 2
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
        assert_equal(self.fs2.schedule_dict['foo'][0].get_ID(), self.r1.get_ID())
        assert_equal(self.fs2.schedule_dict['bar'][0].get_ID(), self.r2.get_ID())
        assert_equal(self.fs2.schedule_dict['foo'][0].scheduled_start, 1)
        assert_equal(self.fs2.schedule_dict['bar'][0].scheduled_start, 2)
        assert_equal(self.fs2.schedule_dict_busy['foo'].timepoints[0].time, 1)
        assert_equal(self.fs2.schedule_dict_busy['foo'].timepoints[0].type, 'start')
        assert_equal(self.fs2.schedule_dict_busy['foo'].timepoints[1].time, 2)
        assert_equal(self.fs2.schedule_dict_busy['foo'].timepoints[1].type, 'end')
        assert_equal(self.fs2.schedule_dict_busy['bar'].timepoints[0].time, 2)
        assert_equal(self.fs2.schedule_dict_busy['bar'].timepoints[0].type, 'start')
        assert_equal(self.fs2.schedule_dict_busy['bar'].timepoints[1].time, 4)
        assert_equal(self.fs2.schedule_dict_busy['bar'].timepoints[1].type, 'end')
        assert_equal(self.fs2.schedule_dict_free['foo'].timepoints[0].time, 2)
        assert_equal(self.fs2.schedule_dict_free['foo'].timepoints[0].type, 'start')
        assert_equal(self.fs2.schedule_dict_free['foo'].timepoints[1].time, 5)
        assert_equal(self.fs2.schedule_dict_free['foo'].timepoints[1].type, 'end')
        assert_equal(self.fs2.schedule_dict_free['bar'].timepoints[0].time, 1)
        assert_equal(self.fs2.schedule_dict_free['bar'].timepoints[0].type, 'start')
        assert_equal(self.fs2.schedule_dict_free['bar'].timepoints[1].time, 2)
        assert_equal(self.fs2.schedule_dict_free['bar'].timepoints[1].type, 'end')
        assert_equal(self.fs2.schedule_dict_free['bar'].timepoints[2].time, 4)
        assert_equal(self.fs2.schedule_dict_free['bar'].timepoints[2].type, 'start')
        assert_equal(self.fs2.schedule_dict_free['bar'].timepoints[3].time, 5)
        assert_equal(self.fs2.schedule_dict_free['bar'].timepoints[3].type, 'end')
        
