#!/usr/bin/env python

'''
test_23032013.py

Author: Sotiria Lampoudi
August 2012
'''

from nose.tools import assert_equal
from time_intervals.intervals import Intervals
from adaptive_scheduler.kernel.fullscheduler_v5 import *
import copy

class Test_23032013(object):

    def setup(self):
        s1 = Intervals([{'time': 15010113, 'type': 'start'},
                        {'time': 15028389, 'type': 'end'}])
        s2 = copy.copy(s1)
        s3 = copy.copy(s1)

        self.r1 = Reservation_v3(10, 1305, {'foo': s1})
        self.r2 = Reservation_v3(10, 1305, {'foo': s2})
        self.r3 = Reservation_v3(10, 1305, {'foo': s3})

        self.cr1 = CompoundReservation_v2([self.r1])
        self.cr2 = CompoundReservation_v2([self.r2])
        self.cr3 = CompoundReservation_v2([self.r3])

        self.gpw = {}
#        self.gpw['foo'] = Intervals([{'time': 0, 'type': 'start'}, {'time': 15724800, 'type' :'end'}])
        self.gpw['foo'] = Intervals([
                                        {'time': 14947200, 'type': 'start'},
                                        {'time': 14960949, 'type': 'end'},
                                        {'time': 15010609, 'type': 'start'},
                                        {'time': 15047392, 'type': 'end'},
                                        {'time': 15096930, 'type': 'start'},
                                        {'time': 15133834, 'type': 'end'},
                                        {'time': 15183251, 'type': 'start'},
                                        {'time': 15220276, 'type': 'end'},
                                        {'time': 15269573, 'type': 'start'},
                                        {'time': 15306718, 'type': 'end'},
                                        {'time': 15355895, 'type': 'start'},
                                        {'time': 15393160, 'type': 'end'},
                                        {'time': 15442217, 'type': 'start'},
                                        {'time': 15479602, 'type': 'end'},
                                        {'time': 15528539, 'type': 'start'},
                                        {'time': 15566043, 'type': 'end'},
                                        {'time': 15614862, 'type': 'start'},
                                        {'time': 15652484, 'type': 'end'},
                                        {'time': 15701186, 'type': 'start'},
                                        {'time': 15724800, 'type': 'end'}
                                      ])

        slice_dict = {}
        slice_dict['foo'] = [0,600]

        self.fs1 = FullScheduler_v5([self.cr1, self.cr2, self.cr3], 
                                    self.gpw, [], slice_dict)


    def test_schedule_all_1(self):
        d = self.fs1.schedule_all()
        assert_equal(self.r1.scheduled, True)
        assert_equal(self.r2.scheduled, True)
        assert_equal(self.r3.scheduled, True)
        print self.r1
        print self.r2
        print self.r3
        assert(False)


