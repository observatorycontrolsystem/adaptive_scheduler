#!/usr/bin/env python
'''
test_fullscheduler_gurobi.py

Author: Sotiria Lampoudi
August 2012
'''

import copy
import os

# Gurobi requires 64-bit OS
from nose import SkipTest

if os.uname()[4] != 'x86_64':
    raise SkipTest('ortoolkit requires a 64-bit OS')

from nose.tools import assert_equal, assert_true
from time_intervals.intervals import Intervals

try:
    from adaptive_scheduler.kernel.fullscheduler_ortoolkit import FullScheduler_ortoolkit
except ImportError:
    raise SkipTest('ORToolkit is not properly installed, skipping these tests.')
from adaptive_scheduler.kernel.reservation_v3 import Reservation_v3, CompoundReservation_v2
from test.requires_third_party.fullscheduler_ortoolkit_helper import Fullscheduler_ortoolkit_helper


class TestFullScheduler_cbc(Fullscheduler_ortoolkit_helper):
    def setup(self):
        self.algorithm = 'CBC'
        super().setup(self.algorithm)

    # This is testing that we schedule earlier rather than later if given the choice
    def test_schedule_early(self):
        self.fs10.schedule_all()
        assert_true(self.r19.scheduled_start <= 3)
        assert_true(self.r20.scheduled_start <= 3)
        assert_true(self.r21.scheduled_start <= 3)

    # This should schedule the two separate over the two "and"ed blocks
    def test_schedule_noneofand(self):
        self.fs9.schedule_all()
        assert_equal(self.r15.scheduled, False)
        assert_equal(self.r16.scheduled, False)
        assert_equal(self.r17.scheduled, True)
        assert_equal(self.r18.scheduled, True)

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
        slice_size_seconds = 1
        fs = FullScheduler_ortoolkit(self.algorithm, [self.cr9],
                                     self.gpw2, [], slice_size_seconds, 0.01, False)
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
        slice_size_seconds = 300
        fs = FullScheduler_ortoolkit(self.algorithm, [cr], gpw, [], slice_size_seconds, 0.01, False)
        fs.schedule_all()

    def test_schedule_all_gaw(self):
        self.fs7.schedule_all()
        assert_equal(self.r9.scheduled, False)
        assert_equal(self.r10.scheduled, False)

    def test_schedule_order_dependent_resources(self):
        s1 = Intervals([{'time': 0, 'type': 'start'}, {'time': 1000, 'type': 'end'}])
        s2 = Intervals([{'time': 0, 'type': 'start'}, {'time': 1000, 'type': 'end'}])
        r1 = Reservation_v3(1, 30, {'foo': s1, 'goo': s2})
        cr = CompoundReservation_v2([r1], 'single')
        gpw = {}
        gpw['goo'] = Intervals([{'time': 250, 'type': 'start'}, {'time': 750, 'type': 'end'}])
        gpw['foo'] = Intervals([])  # [{'time': 1500, 'type': 'start'}, {'time': 2000, 'type': 'end'}])

        fs = FullScheduler_ortoolkit(self.algorithm, [cr], gpw, [], 60, 0.01, False)
        schedule = fs.schedule_all()
        print(schedule)
        assert_equal(1, len(schedule['goo']))

        s1 = Intervals([{'time': 0, 'type': 'start'}, {'time': 1000, 'type': 'end'}])
        s2 = Intervals([{'time': 0, 'type': 'start'}, {'time': 1000, 'type': 'end'}])
        r1 = Reservation_v3(1, 30, {'foo': s1, 'goo': s2})
        cr = CompoundReservation_v2([r1], 'single')
        gpw = {}
        gpw['goo'] = Intervals([{'time': 250, 'type': 'start'}, {'time': 750, 'type': 'end'}])
        gpw['foo'] = Intervals([{'time': 1500, 'type': 'start'}, {'time': 2000, 'type': 'end'}])

        fs = FullScheduler_ortoolkit(self.algorithm, [cr], gpw, [], 60, 0.01, False)
        schedule = fs.schedule_all()
        print(schedule)
        assert_equal(1, len(schedule['goo']))

        s1 = Intervals([{'time': 0, 'type': 'start'}, {'time': 1000, 'type': 'end'}])
        s2 = Intervals([{'time': 0, 'type': 'start'}, {'time': 1000, 'type': 'end'}])
        r1 = Reservation_v3(1, 30, {'foo': s1, 'goo': s2})
        cr = CompoundReservation_v2([r1], 'single')
        gpw = {}
        gpw['foo'] = Intervals([{'time': 250, 'type': 'start'}, {'time': 750, 'type': 'end'}])
        gpw['goo'] = Intervals([{'time': 1500, 'type': 'start'}, {'time': 2000, 'type': 'end'}])

        fs = FullScheduler_ortoolkit(self.algorithm, [cr], gpw, [], 60, 0.01, False)
        schedule = fs.schedule_all()
        print(schedule)
        assert_equal(1, len(schedule['foo']))

        s1 = Intervals([{'time': 0, 'type': 'start'}, {'time': 1000, 'type': 'end'}])
        s2 = Intervals([{'time': 0, 'type': 'start'}, {'time': 1000, 'type': 'end'}])
        r1 = Reservation_v3(1, 30, {'foo': s1, 'goo': s2})
        cr = CompoundReservation_v2([r1], 'single')
        gpw = {}
        gpw['foo'] = Intervals([{'time': 250, 'type': 'start'}, {'time': 750, 'type': 'end'}])
        gpw['goo'] = Intervals([{'time': 1500, 'type': 'start'}, {'time': 2000, 'type': 'end'}])

        fs = FullScheduler_ortoolkit(self.algorithm, [cr], gpw, [], 60, 0.01, False)
        schedule = fs.schedule_all()
        print(schedule)
        assert_equal(1, len(schedule['foo']))

    def test_schedule_no_available_windows(self):
        s1 = Intervals([{'time': 0, 'type': 'start'}, {'time': 1000, 'type': 'end'}])
        s2 = Intervals([{'time': 0, 'type': 'start'}, {'time': 1000, 'type': 'end'}])
        r1 = Reservation_v3(1, 30, {'foo': s1, 'goo': s2})
        cr = CompoundReservation_v2([r1], 'single')
        gpw = {}
        gpw['goo'] = Intervals([{'time': 250, 'type': 'start'}, {'time': 750, 'type': 'end'}])

        fs = FullScheduler_ortoolkit(self.algorithm, [cr], gpw, [], 60, 0.01, False)
        fs.schedule_all()
