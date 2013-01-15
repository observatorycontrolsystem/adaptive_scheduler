#!/usr/bin/env python

'''
test_stress_realistic_1.py

Author: Sotiria Lampoudi
Sept 2011
'''

from nose.tools import assert_equal
import copy, random
from adaptive_scheduler.kernel.fullscheduler_v3 import FullScheduler_v3 as CurrentScheduler
from util import *

class TestStress_realistic(object):

    def setup(self):
        ''' In this test we schedule numdays days.
        The unit of time is seconds. 
        Global windows of opportunity are (1,tfinal).
        There are two resources, '1' and '2'.
        We generate resnum reservations with:
        * random choice between single, and, oneof
        * single: possible windows w/ 0-max_slack slack 
        * oneof/and: two reservations
        '''
        self.numdays = 30
        self.resnum = 10000
        self.max_duration = 8*60*60 # 8 hrs * 60 min * 60 sec
        self.tfinal = self.numdays * 24 * 60 * 60 # days * hrs * min *sec
        self.max_slack = 1*60*60 # 1 hr * min * sec
        self.max_priority = 100
        self.gpw = {}
        self.gpw['1'] = Intervals([Timepoint(1, 'start'), 
                                   Timepoint(self.tfinal, 'end')])
        self.gpw['2'] = Intervals([Timepoint(1, 'start'), 
                                   Timepoint(self.tfinal, 'end')])


    def generate_reservation(self):
        max_duration = self.max_duration
        tfinal = self.tfinal
        max_slack = self.max_slack
        max_priority = self.max_priority

        duration = random.randint(1,max_duration)

        start = random.randint(1,tfinal)
        slack = random.randint(0,max_slack)
        window = Intervals([Timepoint(start, 'start'),
                            Timepoint(start+duration+slack, 'end')])
        resource = str(random.randint(1,2))
        priority = random.randint(1, max_priority)
        reservation = Reservation_v3(priority, duration, {resource: window})
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
	slice_dict['1'] = [1,5*60] # 5 min windows
	slice_dict['2'] = [1,10*60] # 10 min windows
#        fs = CurrentScheduler(cr_list, gpw, [], slice_dict)
        fs = CurrentScheduler(cr_list, gpw, [])
        s = fs.schedule_all()
        u = Util()
        #u.get_coverage_count_plot(s)
        u.find_overlaps(s)
        return [len(s['1']),len(s['2'])]


    # def test_run_multi(self, howmany=1000):
    #     nrs1_sum = 0
    #     nrs2_sum = 0
    #     for i in range(0, howmany):
    #         [nrs1, nrs2] = self.test_run_1()
    #         nrs1_sum += nrs1
    #         nrs2_sum += nrs2
    #     print 'in ',howmany,' runs, mean number of reservations scheduled was ',nrs1_sum/howmany, ' and ', nrs2_sum/howmany

# 100 c.r.s: Ran 1 test in 4.672s
# 1000 c.r.s: Ran 1 test in 401.491s
