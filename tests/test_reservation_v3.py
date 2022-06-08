#!/usr/bin/env python
'''
test_reservation_v3.py

Author: Sotiria Lampoudi
November 2011
'''


from time_intervals.intervals import Intervals
from adaptive_scheduler.kernel.reservation_v3 import Reservation_v3, CompoundReservation_v2


class TestReservation_v3(object):

    def setup(self):
        s1 = Intervals([{'time': 1, 'type': 'start'}, {'time': 2, 'type': 'end'}])
        s2 = Intervals([{'time': 2, 'type': 'start'}, {'time': 4, 'type': 'end'}])
        s3 = Intervals([{'time': 2, 'type': 'start'}, {'time': 6, 'type': 'end'}])
        self.r1 = Reservation_v3(1, 1, {'foo': s1})
        self.r2 = Reservation_v3(1, 2, {'bar': s2})
        self.r3 = Reservation_v3(2, 1, {'foo': s3})
        self.r4 = Reservation_v3(1, 1, {'foo': s1, 'bar': s2})
        self.cr1 = CompoundReservation_v2([self.r1])
        self.cr2 = CompoundReservation_v2([self.r1, self.r2], 'and')
        self.cr3 = CompoundReservation_v2([self.r1, self.r3], 'oneof')

    def test_create_reservation(self):
        assert self.r1.priority == 1
        assert self.r1.duration == 1
        assert list(self.r1.possible_windows_dict.keys()) == ['foo']

    def test_create_reservation_2(self):
        assert set(self.r4.possible_windows_dict.keys()) == set(['foo', 'bar'])

    def test_print(self):
        print(self.r1)

    def test_print_2(self):
        self.r1.schedule(1, 1, 'foo', 'test')
        print(self.r1)

    def test_remove_from_free_windows_1(self):
        self.r1.remove_from_free_windows(Intervals([{'time': 1, 'type': 'start'}, {'time': 2, 'type': 'end'}]), 'foo')
        assert self.r1.free_windows_dict['foo'].timepoints == []

    def test_remove_from_free_windows_2(self):
        self.r2.remove_from_free_windows(Intervals([{'time': 3, 'type': 'start'}, {'time': 4, 'type': 'end'}]), 'bar')
        assert self.r2.free_windows_dict['bar'].timepoints == []

    def test_remove_from_free_windows_3(self):
        self.r3.remove_from_free_windows(Intervals([{'time': 2, 'type': 'start'}, {'time': 3, 'type': 'end'}]), 'foo')
        assert self.r3.free_windows_dict['foo'].timepoints[0]['time'] == 3
        assert self.r3.free_windows_dict['foo'].timepoints[0]['type'] == 'start'
        assert self.r3.free_windows_dict['foo'].timepoints[1]['time'] == 6
        assert self.r3.free_windows_dict['foo'].timepoints[1]['type'] == 'end'

    def test_lt(self):
        '''Sorting by priority'''
        assert self.r1 > self.r3

    def test_schedule(self):
        self.r1.schedule(1, 1, 'foo', 'test')
        assert self.r1.scheduled_start == 1
        assert self.r1.scheduled_quantum == 1
        assert self.r1.scheduled == True
        assert self.r1.scheduled_timepoints[0]['time'] == 1
        assert self.r1.scheduled_timepoints[0]['type'] == 'start'
        assert self.r1.scheduled_timepoints[1]['time'] == 2
        assert self.r1.scheduled_timepoints[1]['type'] == 'end'

    def test_schedule_anywhere(self):
        self.r1.schedule_anywhere()
        assert self.r1.scheduled_start == 1
        assert self.r1.scheduled_quantum == 1
        assert self.r1.scheduled == True
        assert self.r1.scheduled_timepoints[0]['time'] == 1
        assert self.r1.scheduled_timepoints[0]['type'] == 'start'
        assert self.r1.scheduled_timepoints[1]['time'] == 2
        assert self.r1.scheduled_timepoints[1]['type'] == 'end'
        assert self.r1.scheduled_by == 'reservation_v3.schedule_anywhere()'

    def test_create_compound(self):
        assert self.cr1.reservation_list == [self.r1]
        assert self.cr1.type == 'single'
        assert self.cr1.size == 1
        assert self.cr1.issingle()

    def test_create_compound_2(self):
        assert self.cr2.reservation_list == [self.r1, self.r2]
        assert self.cr2.type == 'and'
        assert self.cr2.size == 2
        assert self.cr2.isand()

    def test_create_compound_3(self):
        assert self.cr3.reservation_list == [self.r1, self.r3]
        assert self.cr3.type == 'oneof'
        assert self.cr3.size == 2
        assert self.cr3.isoneof()
