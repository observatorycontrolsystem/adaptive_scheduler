#!/usr/bin/env python

'''
test_long_block_bug.py - summary line

description

Author: Eric Saunders
March 2014
'''

from nose.tools import assert_equal

from adaptive_scheduler.kernel.intervals        import Intervals
from adaptive_scheduler.kernel.timepoint        import Timepoint
from adaptive_scheduler.kernel.reservation_v3   import Reservation_v3, CompoundReservation_v2
#from adaptive_scheduler.kernel.fullscheduler_v5 import FullScheduler_v5 as FullScheduler
from adaptive_scheduler.kernel.fullscheduler_v6 import FullScheduler_v6 as FullScheduler



class TestLongBlockBug(object):


    def setup(self):
        self.pw1     = Intervals([Timepoint(0,     'start'),
                             Timepoint(22765, 'end')])
        self.pw2     = Intervals([Timepoint(0,     'start'),
                             Timepoint(22765, 'end')])

        # A short duration
        self.r1 = Reservation_v3(
                                  priority = 18,
                                  duration = 287,
                                  possible_windows_dict = {'foo' : self.pw1}
                                )

        self.cr1 = CompoundReservation_v2([self.r1])


        self.globally_possible_windows = {}
        self.globally_possible_windows['foo'] = Intervals([
                                                  Timepoint(0, 'start'),
                                                  Timepoint(50000, 'end'),
                                                ])
        self.contractual_obligations = []
        self.time_slicing_dict       = {'foo': [0, 300]}


    def test_bug1(self):
        # If duration 287 comes first in 'to_schedule', both are scheduled
        r2 = Reservation_v3(
                                  priority = 18,
                                  duration = 700,
                                  possible_windows_dict = {'foo' : self.pw2}
                                )
        cr2 = CompoundReservation_v2([r2])
        self.to_schedule = [self.cr1, cr2]
        kernel = FullScheduler(
                                self.to_schedule,
                                self.globally_possible_windows,
                                self.contractual_obligations,
                                self.time_slicing_dict
                              )
        schedule = kernel.schedule_all()

        assert_equal(len(schedule['foo']), 2)


    def test_bug2(self):
        # If duration 287 comes second in 'to_schedule', nothing is scheduled
        r2 = Reservation_v3(
                                  priority = 18,
                                  duration = 700,
                                  possible_windows_dict = {'foo' : self.pw2}
                                )
        cr2 = CompoundReservation_v2([r2])
        self.to_schedule = [cr2, self.cr1]
        kernel = FullScheduler(
                                self.to_schedule,
                                self.globally_possible_windows,
                                self.contractual_obligations,
                                self.time_slicing_dict
                              )
        schedule = kernel.schedule_all()

        assert_equal(len(schedule['foo']), 2)


    def test_bug3(self):
        # If the second Reservation is made shorter than 287, and duration 287
        # comes first, both are scheduled
        r2 = Reservation_v3(
                                  priority = 18,
                                  duration = 50,
                                  possible_windows_dict = {'foo' : self.pw2}
                                )
        cr2 = CompoundReservation_v2([r2])
        self.to_schedule = [self.cr1, cr2]
        kernel = FullScheduler(
                                self.to_schedule,
                                self.globally_possible_windows,
                                self.contractual_obligations,
                                self.time_slicing_dict
                              )
        schedule = kernel.schedule_all()

        assert_equal(len(schedule['foo']), 2)


    def test_bug4(self):
        # If the second Reservation comes first, both will be scheduled if duration <=600.
        # At 601+, neither will be scheduled. This is true up to at least 2000 (not shown in
        # this test, which stops at 700).
        success = True
        for i in xrange(1,701):
            r2 = Reservation_v3(
                                      priority = 18,
                                      duration = i,
                                      possible_windows_dict = {'foo' : self.pw2}
                                    )
            cr2 = CompoundReservation_v2([r2])
            self.to_schedule = [cr2, self.cr1]
            kernel = FullScheduler(
                                    self.to_schedule,
                                    self.globally_possible_windows,
                                    self.contractual_obligations,
                                    self.time_slicing_dict
                                  )
            schedule = kernel.schedule_all()

            try:
                assert_equal(len(schedule['foo']), 2)
            except Exception as e:
                print "Duration = %s" % i
                success = False

        # Fail the test here to get the print out
        assert(success)
