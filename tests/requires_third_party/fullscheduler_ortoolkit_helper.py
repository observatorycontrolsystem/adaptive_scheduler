from time_intervals.intervals import Intervals
import copy

import pytest

try:
    from adaptive_scheduler.kernel.fullscheduler_ortoolkit import FullScheduler_ortoolkit
except ImportError:
    pytest.skip('ORToolkit is not properly installed, skipping these tests.', allow_module_level=True)

from adaptive_scheduler.kernel.reservation_v3 import Reservation_v3, CompoundReservation_v2


class Fullscheduler_ortoolkit_helper(object):
    def setup(self, algorithm):
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

        # Priority, Duration, possible_windows_dict
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

        self.r15 = Reservation_v3(1, 9, {'bar': s13})
        self.r16 = Reservation_v3(1, 9, {'foo': s13})
        self.r17 = Reservation_v3(2, 9, {'bar': s13})
        self.r18 = Reservation_v3(2, 9, {'foo': s13})

        self.r19 = Reservation_v3(1, 1, {'bar': s10})
        self.r20 = Reservation_v3(1, 1, {'bar': s10})
        self.r21 = Reservation_v3(1, 1, {'bar': s10})

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

        self.cr17 = CompoundReservation_v2([self.r15, self.r16], 'and')
        self.cr18 = CompoundReservation_v2([self.r17])
        self.cr19 = CompoundReservation_v2([self.r18])

        self.cr20 = CompoundReservation_v2([self.r19])
        self.cr21 = CompoundReservation_v2([self.r20])
        self.cr22 = CompoundReservation_v2([self.r21])

        self.gpw2 = {}
        self.gpw2['foo'] = Intervals([{'time': 1, 'type': 'start'}, {'time': 10, 'type': 'end'}], 'free')
        self.gpw2['bar'] = Intervals([{'time': 1, 'type': 'start'}, {'time': 10, 'type': 'end'}], 'free')

        self.gpw3 = {}
        self.gpw3['foo'] = Intervals([{'time': 5, 'type': 'start'}, {'time': 10, 'type': 'end'}], 'free')
        self.gpw3['bar'] = Intervals([{'time': 5, 'type': 'start'}, {'time': 10, 'type': 'end'}], 'free')

        self.gpw4 = {}
        self.gpw4['bar'] = Intervals([{'time': 1, 'type': 'start'}, {'time': 10, 'type': 'end'}], 'free')

        slice_dict = {}
        slice_dict['foo'] = [0, 1]
        slice_dict['bar'] = [0, 1]
        slice_size_seconds = 1

        self.fs1 = FullScheduler_ortoolkit(algorithm, [self.cr1, self.cr2, self.cr3],
                                           self.gpw2, [], slice_size_seconds, 0.01, False)
        self.fs2 = FullScheduler_ortoolkit(algorithm, [self.cr1, self.cr4],
                                           self.gpw2, [], slice_size_seconds, 0.01, False)
        self.fs3 = FullScheduler_ortoolkit(algorithm, [self.cr5],
                                           self.gpw2, [], slice_size_seconds, 0.01, False)
        self.fs4 = FullScheduler_ortoolkit(algorithm, [self.cr8, self.cr6, self.cr7],
                                           self.gpw2, [], slice_size_seconds, 0.01, False)
        self.fs5 = FullScheduler_ortoolkit(algorithm, [self.cr10, self.cr2, self.cr3],
                                           self.gpw2, [], slice_size_seconds, 0.01, False)
        self.fs6 = FullScheduler_ortoolkit(algorithm, [self.cr11, self.cr2, self.cr3],
                                           self.gpw2, [], slice_size_seconds, 0.01, False)
        self.fs7 = FullScheduler_ortoolkit(algorithm, [self.cr12],
                                           self.gpw3, [], slice_size_seconds, 0.01, False)
        self.fs8 = FullScheduler_ortoolkit(algorithm, [self.cr13, self.cr14, self.cr15, self.cr16],
                                           self.gpw4, [], slice_size_seconds, 0.01, False)
        self.fs9 = FullScheduler_ortoolkit(algorithm, [self.cr17, self.cr18, self.cr19],
                                           self.gpw2, [], slice_size_seconds, 0.01, False)
        self.fs10 = FullScheduler_ortoolkit(algorithm, [self.cr20, self.cr21, self.cr22],
                                            self.gpw2, [], slice_size_seconds, 0.01, False)

