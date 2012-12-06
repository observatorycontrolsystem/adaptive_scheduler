#!/usr/bin/env python

'''
test_stress_3.py

Author: Sotiria Lampoudi
May 2011

adding slack to see if # sched resources increases
'''

from nose.tools import assert_equal
import copy, random
from adaptive_scheduler.kernel.fullscheduler_v2 import FullScheduler_v2 as CurrentScheduler
from util import *

class TestStress_3(object):

    def setup(self):
        ''' In this test we schedule in the overall interval between 
        t0 = 1 and tfinal
        Global window of opportunity is the whole interval [1,1000].
        There are two resources, '1' and '2'.
        We generate resnum single reservations with:
        * single possible windows w/ 0-max_slack slack & priority 1
        * random starts in [1,1000] 
        * random duration between 1-10 time units. 
        * random resource choice between '1' and '2'
        Let's see what happens!
        '''
        self.gpw = {}
        self.gpw['1'] = Intervals([Timepoint(1, 'start'), 
                                   Timepoint(1000, 'end')])
        self.gpw['2'] = Intervals([Timepoint(1, 'start'), 
                                   Timepoint(1000, 'end')])
        self.priority = 1
        self.resnum = 100
        self.max_duration = 10
        self.tfinal = 1000
        self.max_slack = 20

    def test_run_1(self, seed=None):
        '''Returns mean reservation duration and number of reservations
        scheduled.'''

        priority = self.priority
        gpw = self.gpw
        max_duration = self.max_duration
        tfinal = self.tfinal
        resnum = self.resnum
        max_slack = self.max_slack

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
            slack = random.randint(0,max_slack)
            window = Intervals([Timepoint(start, 'start'),
                               Timepoint(start+duration+slack, 'end')])
            resource = str(random.randint(1,2))
            reservation = Reservation_v3(1, duration, {resource: window})
            cr = CompoundReservation_v2([reservation])
            cr_list.append(cr)

        fs = CurrentScheduler(cr_list, gpw, [])
        s = fs.schedule_all()
        u = Util()
        #u.get_coverage_count_plot(s)
        u.find_overlaps(s)
        return [float(total_duration)/float(resnum), len(s['1']),len(s['2'])]

    def test_run_multi(self, howmany=1000):
        md_sum = 0
        nrs1_sum = 0
        nrs2_sum = 0
        for i in range(0, howmany):
            [md, nrs1, nrs2] = self.test_run_1()
            md_sum += md
            nrs1_sum += nrs1
            nrs2_sum += nrs2
        print 'in ',howmany,' runs, mean duration was ',md_sum/howmany, ' and mean number of reservations scheduled was ',nrs1_sum/howmany, ' and ', nrs2_sum/howmany

# with cluster_and_order(1)
# with max_slack = 10
# in  1000  runs, mean duration was  5.47593  and mean number of reservations scheduled was  36  and  36
# .
# ----------------------------------------------------------------------
# Ran 2 tests in 132.029s

# with cluster_and_order(1)
# with max_slack = 20
# in  1000  runs, mean duration was  5.50543  and mean number of reservations scheduled was  39  and  39
# .
# ----------------------------------------------------------------------
# Ran 2 tests in 137.529s
