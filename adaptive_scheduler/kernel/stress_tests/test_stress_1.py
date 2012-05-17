#!/usr/bin/env python

'''
test_stress_1.py

Author: Sotiria Lampoudi
May 2011
'''

from nose.tools import assert_equal
import copy, random
from adaptive_scheduler.kernel.fullscheduler_v2 import *

class TestStress_1(object):

    def setup(self):
        ''' In this test we schedule in the overall interval between 
        t0 = 1 and tfinal
        Global window of opportunity is the whole interval [1,1000].
        There is only one resource, called 'foo'.
        We generate resnum single reservations with:
        * single possible windows w/ no slack & priority 1
        * random starts in [1,1000] 
        * random duration between 1-10 time units. 
        Let's see what happens!
        '''
        self.resource = 'foo'
        self.gpw = {}
        self.gpw[self.resource] = Intervals([Timepoint(1, 'start'), 
                                             Timepoint(1000, 'end')])
        self.priority = 1
        self.resnum = 100
        self.max_duration = 10
        self.tfinal = 1000

    def test_run_1(self, seed=None):
        '''Returns mean reservation duration and number of reservations
        scheduled.'''

        priority = self.priority
        gpw = self.gpw
        resource = self.resource
        max_duration = self.max_duration
        tfinal = self.tfinal
        resnum = self.resnum

        if seed:
            random.seed(seed)
        cr_list = []
        total_duration = 0
        for i in range(0, resnum):
            # generate random reservation w/ start in [0,1000]
            # duration 1-10
            # single possible tight windows & prio 1
            duration = random.randint(1,max_duration)
            total_duration += duration
            start = random.randint(1,tfinal)
            window = Intervals([Timepoint(start, 'start'),
                               Timepoint(start+duration, 'end')])
            reservation = Reservation_v2(1, duration, resource, window)
            cr = CompoundReservation_v2([reservation])
            cr_list.append(cr)

        fs = FullScheduler_v2(cr_list, gpw, [])
        s = fs.schedule_all()

        return [float(total_duration)/float(resnum), len(s[resource])]

    def test_run_multi(self, howmany=1000):
        md_sum = 0
        nrs_sum = 0
        for i in range(0, howmany):
            [md, nrs] = self.test_run_1()
            md_sum += md
            nrs_sum += nrs
        print 'in ',howmany,' runs, mean duration was ',md_sum/howmany, ' and mean number of reservations scheduled was ',nrs_sum/howmany

# with cluster_and_order(2)
# in  1000  runs, mean duration was  5.50137  and mean number of reservations scheduled was  39
# .
# ----------------------------------------------------------------------
# Ran 2 tests in 182.958s

# with cluster_and_order(1)
# in  1000  runs, mean duration was  5.47986  and mean number of reservations scheduled was  39
# .
# ----------------------------------------------------------------------
# Ran 2 tests in 181.592s
