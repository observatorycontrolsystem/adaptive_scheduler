#!/usr/bin/env python

'''
test_23032013.py

Author: Sotiria Lampoudi
August 2012
'''

from nose.tools import assert_equal
from adaptive_scheduler.kernel.timepoint import *
from adaptive_scheduler.kernel.intervals import *
from adaptive_scheduler.kernel.fullscheduler_v5 import *
import copy

class Test_23032013(object):

    def setup(self):
        s1 = Intervals([Timepoint(15010113, 'start'),
                        Timepoint(15028389, 'end')])
        s2 = copy.copy(s1)
        s3 = copy.copy(s1)

        self.r1 = Reservation_v3(10, 1305, {'foo': s1})
        self.r2 = Reservation_v3(10, 1305, {'foo': s2})
        self.r3 = Reservation_v3(10, 1305, {'foo': s3})

        self.cr1 = CompoundReservation_v2([self.r1])
        self.cr2 = CompoundReservation_v2([self.r2])
        self.cr3 = CompoundReservation_v2([self.r3])

        self.gpw = {}
        self.gpw['foo'] = Intervals([Timepoint(0, 'start'), Timepoint(15724800, 'end')])

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


