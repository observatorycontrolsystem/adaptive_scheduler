#!/usr/bin/env python
'''
test_fullscheduler_v5.py

Author: Sotiria Lampoudi
August 2012
'''

from nose.tools import assert_equal, nottest
from time_intervals.intervals import Intervals
from adaptive_scheduler.kernel.reservation_v3 import Reservation_v3, CompoundReservation_v2

try:
    from adaptive_scheduler.kernel.fullscheduler_v5 import FullScheduler_v5
except Exception:
    pass
import copy


@nottest
class TestFullScheduler_v5(object):

    def setup(self):
        s1 = Intervals([{'time': 1, 'type': 'start'},
                        {'time': 2, 'type': 'end'}])  # 1-2
        s2 = Intervals([{'time': 2, 'type': 'start'},
                        {'time': 4, 'type': 'end'}])  # --2--4
        s3 = copy.copy(s1)
        s4 = copy.copy(s1)
        s5 = copy.copy(s2)
        s6 = copy.copy(s1)
        s7 = copy.copy(s1)
        s8 = copy.copy(s1)
        s9 = copy.copy(s2)
        s10 = Intervals([{'time': 1, 'type': 'start'},
                         {'time': 10, 'type': 'end'}])
        s11 = copy.copy(s10)
        s12 = copy.copy(s10)
        s13 = copy.copy(s10)

        self.r1 = Reservation_v3(1, 1, {'foo': s1})
        self.r2 = Reservation_v3(2, 2, {'bar': s2})
        self.r3 = Reservation_v3(1, 1, {'foo': s3})
        self.r4 = Reservation_v3(1, 1, {'foo': s4})
        self.r5 = Reservation_v3(2, 2, {'bar': s5})
        self.r6 = Reservation_v3(1, 2, {'bar': s5})
        self.r7 = Reservation_v3(1, 1, {'bar': s6, 'foo': s5})
        self.r8 = Reservation_v3(1, 1, {'foo': s6, 'bar': s7})
        self.r9 = Reservation_v3(1, 1, {'foo': s8})
        self.r10 = Reservation_v3(2, 2, {'bar': s9})
        self.r11 = Reservation_v3(1, 1, {'bar': s10})
        self.r12 = Reservation_v3(1, 1, {'bar': s11})
        self.r13 = Reservation_v3(1, 1, {'bar': s12})
        self.r14 = Reservation_v3(1, 1, {'bar': s13})

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

        self.gpw2 = {}
        self.gpw2['foo'] = Intervals([{'time': 1, 'type': 'start'}, {'time': 10, 'type': 'end'}], 'free')
        self.gpw2['bar'] = Intervals([{'time': 1, 'type': 'start'}, {'time': 10, 'type': 'end'}], 'free')

        self.gpw3 = {}
        self.gpw3['foo'] = Intervals([{'time': 5, 'type': 'start'}, {'time': 10, 'type': 'end'}], 'free')
        self.gpw3['bar'] = Intervals([{'time': 5, 'type': 'start'}, {'time': 10, 'type': 'end'}], 'free')

        self.gpw4 = {}
        self.gpw4['bar'] = Intervals([{'time': 1, 'type': 'start'}, {'time': 10, 'type': 'end'}], 'free')

        self.fs1 = FullScheduler_v5([self.cr1, self.cr2, self.cr3],
                                    self.gpw2, [], 1)
        self.fs2 = FullScheduler_v5([self.cr1, self.cr4],
                                    self.gpw2, [], 1)
        self.fs3 = FullScheduler_v5([self.cr5],
                                    self.gpw2, [], 1)
        self.fs4 = FullScheduler_v5([self.cr8, self.cr6, self.cr7],
                                    self.gpw2, [], 1)
        self.fs5 = FullScheduler_v5([self.cr10, self.cr2, self.cr3],
                                    self.gpw2, [], 1)
        self.fs6 = FullScheduler_v5([self.cr11, self.cr2, self.cr3],
                                    self.gpw2, [], 1)
        self.fs7 = FullScheduler_v5([self.cr12],
                                    self.gpw3, [], 1)
        self.fs8 = FullScheduler_v5([self.cr13, self.cr14, self.cr15, self.cr16],
                                    self.gpw4, [], 1)

    def test_schedule_all_4inarow(self):
        self.fs8.schedule_all()
        assert_equal(self.r11.scheduled, True)
        assert_equal(self.r12.scheduled, True)
        assert_equal(self.r13.scheduled, True)
        assert_equal(self.r14.scheduled, True)

    def test_schedule_all_1(self):
        self.fs1.schedule_all()
        assert_equal(self.r1.scheduled, False)
        assert_equal(self.r2.scheduled, True)
        assert_equal(self.r3.scheduled, True)
        assert_equal(self.r4.scheduled, False)

    def test_schedule_all_multi_resource(self):
        self.fs5.schedule_all()
        assert_equal(self.r7.scheduled, True)
        assert_equal(self.r2.scheduled, True)
        assert_equal(self.r3.scheduled, True)
        assert_equal(self.r4.scheduled, False)

    def test_schedule_all_multi_resource_2(self):
        self.fs6.schedule_all()
        assert_equal(self.r8.scheduled, True)
        assert_equal(self.r2.scheduled, True)
        assert_equal(self.r3.scheduled, True)
        assert_equal(self.r4.scheduled, False)

    def test_schedule_all_2(self):
        self.fs2.schedule_all()
        assert_equal(self.r1.scheduled, True)
        assert_equal(self.r5.scheduled, True)

    def test_schedule_all_3(self):
        self.fs3.schedule_all()
        assert_equal(self.r4.scheduled, False)
        assert_equal(self.r5.scheduled, True)

    def test_schedule_all_4(self):
        self.fs4.schedule_all()
        assert_equal(self.r2.scheduled, True)
        assert_equal(self.r6.scheduled, False)
        # either r3 or r4 should be scheduled, not both
        if self.r3.scheduled:
            assert_equal(self.r4.scheduled, False)
        else:
            assert_equal(self.r4.scheduled, True)

    def test_schedule_triple_oneof(self):
        slice_dict = {}
        slice_dict['foo'] = [0, 1]
        slice_dict['bar'] = [0, 1]
        fs = FullScheduler_v5([self.cr9],
                              self.gpw2, [], 1)
        fs.schedule_all()
        # only one should be scheduled

    def test_schedule_5_7_2012(self):
        s1 = Intervals([{'time': 93710, 'type': 'start'},
                        {'time': 114484, 'type': 'end'},
                        {'time': 180058, 'type': 'start'},
                        {'time': 200648, 'type': 'end'}])
        r1 = Reservation_v3(1, 30, {'foo': s1})
        s2 = copy.copy(s1)
        r2 = Reservation_v3(1, 30, {'goo': s2})

        cr = CompoundReservation_v2([r1, r2], 'oneof')
        gpw = {}
        gpw['foo'] = Intervals([{'time': 90000, 'type': 'start'},
                                {'time': 201000, 'type': 'end'}])
        gpw['goo'] = Intervals([{'time': 90000, 'type': 'start'},
                                {'time': 201000, 'type': 'end'}])
        fs = FullScheduler_v5([cr], gpw, [], 60)
        fs.schedule_all()

    def test_schedule_all_gaw(self):
        self.fs7.schedule_all()
        assert_equal(self.r9.scheduled, False)
        assert_equal(self.r10.scheduled, False)
