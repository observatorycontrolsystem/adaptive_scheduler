#!/usr/bin/env python

'''
test_stress_7_v4.py

Author: Sotiria Lampoudi
Sept 2011

random prio
'''

from nose.tools import assert_equal
import copy, random
from adaptive_scheduler.kernel.fullscheduler_v4 import *
from util import *

class TestStress_7(object):

    def setup(self):
        ''' In this test we schedule in the overall interval between 
        t0 = 1 and tfinal
        Global window of opportunity is the whole interval [1,1000].
        There are two resources, '1' and '2'.
        We generate resnum single reservations with:
        * random choice between single and oneof
        * single: possible windows w/ 0-max_slack slack 
        * oneof: two possible windows w/ ...
        * random starts in [1,1000] 
        * random duration between 1-10 time units. 
        * random resource choice between '1' and '2'
        * random priority
        Let's see what happens!
        '''
        self.gpw = {}
        self.gpw['1'] = Intervals([Timepoint(1, 'start'), 
                                   Timepoint(1000, 'end')])
        self.gpw['2'] = Intervals([Timepoint(1, 'start'), 
                                   Timepoint(1000, 'end')])
#        self.priority = 1
        self.resnum = 100
        self.max_duration = 10
        self.tfinal = 1000
        self.max_slack = 10
        self.max_priority = 100

    def generate_reservation(self):
#        priority = self.priority
        max_duration = self.max_duration
        tfinal = self.tfinal
        max_slack = self.max_slack
        max_priority = self.max_priority
       # generate random reservation w/ start in [0,1000]
        # duration 1-10
        # single possible window & prio 1
        
        duration = random.randint(1,max_duration)

        start = random.randint(1,tfinal)
        slack = random.randint(0,max_slack)
        window = Intervals([Timepoint(start, 'start'),
                            Timepoint(start+duration+slack, 'end')])
        resource = str(random.randint(1,2))
        priority = random.randint(1, max_priority)
        reservation = Reservation_v2(priority, duration, resource, window)
        return reservation
         

    def test_run_1(self, seed=None):
        '''Returns mean reservation duration and number of reservations
        scheduled.'''

        gpw = self.gpw
        resnum = self.resnum
        if seed:
            random.seed(seed)
        cr_list = []
        for i in range(0, resnum):
            reservation = self.generate_reservation()
            restype = random.randint(1,3)
            if restype == 1:
                cr = CompoundReservation_v2([reservation])
            elif restype == 2:
                r2 = self.generate_reservation()
                cr = CompoundReservation_v2([reservation,r2],'oneof')
            else: 
                r2 = self.generate_reservation()
                cr = CompoundReservation_v2([reservation,r2],'and')
                
            cr_list.append(cr)


	slice_dict = {}
	slice_dict['1'] = [0,1]
	slice_dict['2'] = [0,1]
        fs = FullScheduler_v4(cr_list, gpw, [], slice_dict)
        s = fs.schedule_all()
        u = Util()
        #u.get_coverage_count_plot(s)
        u.find_overlaps(s)
        return [len(s['1']),len(s['2'])]


    def test_run_multi(self, howmany=1000):
        nrs1_sum = 0
        nrs2_sum = 0
        for i in range(0, howmany):
            [nrs1, nrs2] = self.test_run_1()
            nrs1_sum += nrs1
            nrs2_sum += nrs2
        print 'in ',howmany,' runs, mean number of reservations scheduled was ',nrs1_sum/howmany, ' and ', nrs2_sum/howmany

#in  1000  runs, mean number of reservations scheduled was  50  and  50
#.
#----------------------------------------------------------------------
#Ran 2 tests in 342.338s

