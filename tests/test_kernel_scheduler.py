#!/usr/bin/env python
'''
test_scheduler.py

Author: Sotiria Lampoudi
Dec 2012
'''
# TODO: write test for make_windows_consistent()

import copy
from time_intervals.intervals import Intervals
from adaptive_scheduler.kernel.scheduler import Scheduler
from adaptive_scheduler.kernel.reservation import Reservation, CompoundReservation


class TestScheduler(object):

    def setup(self):
        s1 = Intervals([{'time': 1, 'type': 'start'},
                        {'time': 2, 'type': 'end'}])  # 1-2
        s2 = Intervals([{'time': 2, 'type': 'start'},
                        {'time': 4, 'type': 'end'}])  # --2--4
        s3 = copy.copy(s1)
        s4 = copy.copy(s1)
        s5 = copy.copy(s2)

        self.r1 = Reservation(1, 1, {'foo': s1})
        self.r2 = Reservation(2, 2, {'bar': s2})
        self.r3 = Reservation(1, 1, {'foo': s3})
        self.r4 = Reservation(1, 1, {'foo': s4})
        self.r5 = Reservation(2, 2, {'bar': s5})

        self.cr1 = CompoundReservation([self.r1])
        self.cr2 = CompoundReservation([self.r3, self.r2], 'and')
        self.cr3 = CompoundReservation([self.r4])
        self.cr4 = CompoundReservation([self.r5])
        self.cr5 = CompoundReservation([self.r4, self.r5], 'oneof')

        self.gpw = {}
        self.gpw['foo'] = [{'time': 1, 'type': 'start'}, {'time': 5, 'type': 'end'}]

        self.gpw2 = {}
        self.gpw2['foo'] = Intervals([{'time': 1, 'type': 'start'}, {'time': 5, 'type': 'end'}], 'free')
        self.gpw2['bar'] = Intervals([{'time': 1, 'type': 'start'}, {'time': 5, 'type': 'end'}], 'free')

        self.sched = Scheduler([self.cr1, self.cr2, self.cr3],
                               self.gpw2, [])
        self.sched2 = Scheduler([self.cr1, self.cr4],
                                self.gpw2, [])

        self.sched3 = Scheduler([self.cr5],
                                self.gpw2, [])

    def test_order_equals_1(self):
        self.sched.current_order = 2
        self.r2.order = 2
        assert self.sched.order_equals(self.r2)

    def test_create(self):
        assert self.sched.compound_reservation_list == [self.cr1, self.cr2, self.cr3]
        assert self.sched.globally_possible_windows_dict == self.gpw2
        assert self.sched.contractual_obligation_list == []
        assert self.sched.schedule_dict_free['foo'].timepoints[0]['time'] == 1
        assert self.sched.schedule_dict_free['foo'].timepoints[0]['type'] == 'start'
        assert self.sched.schedule_dict_free['foo'].timepoints[1]['time'] == 5
        assert self.sched.schedule_dict_free['foo'].timepoints[1]['type'] == 'end'

    def test_create_2(self):
        assert set(self.sched2.resource_list) == set(['foo', 'bar'])
        assert self.sched2.schedule_dict['foo'] == []
        assert self.sched2.schedule_dict['bar'] == []

    def test_convert_compound_to_simple_1(self):
        assert self.sched.reservation_list[0] == self.r1
        assert self.sched.reservation_list[1] == self.r3
        assert self.sched.reservation_list[2] == self.r2
        assert self.sched.reservation_list[3] == self.r4
        assert self.sched.and_constraints[0][0] == self.r3
        assert self.sched.and_constraints[0][1] == self.r2

    def test_convert_compound_to_simple_2(self):
        assert self.sched3.reservation_list[0] == self.r4
        assert self.sched3.reservation_list[1] == self.r5
        assert self.sched3.oneof_constraints[0][0] == self.r4
        assert self.sched3.oneof_constraints[0][1] == self.r5

    def test_commit_reservation_to_schedule_1(self):
        self.r1.schedule(1, 1, 'foo', 'test')
        self.sched2.commit_reservation_to_schedule(self.r1)
        assert self.sched2.schedule_dict['foo'][0].scheduled_start == 1
        assert self.sched2.schedule_dict_busy['foo'].timepoints[0]['time'] == 1
        assert self.sched2.schedule_dict_busy['foo'].timepoints[0]['type'] == 'start'
        assert self.sched2.schedule_dict_busy['foo'].timepoints[1]['time'] == 2
        assert self.sched2.schedule_dict_busy['foo'].timepoints[1]['type'] == 'end'
        assert self.sched2.schedule_dict_free['foo'].timepoints[0]['time'] == 2
        assert self.sched2.schedule_dict_free['foo'].timepoints[0]['type'] == 'start'
        assert self.sched2.schedule_dict_free['foo'].timepoints[1]['time'] == 5
        assert self.sched2.schedule_dict_free['foo'].timepoints[1]['type'] == 'end'

    def test_uncommit_reservation_from_schedule(self):
        assert self.r1 in self.sched2.unscheduled_reservation_list
        self.r1.schedule(1, 1, 'foo', 'test')
        self.sched2.commit_reservation_to_schedule(self.r1)
        assert self.r1 not in self.sched2.unscheduled_reservation_list
        self.sched2.uncommit_reservation_from_schedule(self.r1)
        assert self.r1 in self.sched2.unscheduled_reservation_list

    def test_get_reservation_by_ID(self):
        request_id = self.r1.get_ID()
        r = self.sched.get_reservation_by_ID(request_id)
        assert r == self.r1
