#!/usr/bin/env python

'''
test_stress_realistic_3.py

Author: Sotiria Lampoudi
Sept 2011
'''

from nose.tools import assert_equal
import copy, random
from adaptive_scheduler.kernel.fullscheduler_v5 import FullScheduler_v5 as CurrentScheduler
from util import *

class TestStress_realistic(object):

    def setup(self):
        ''' In this test we schedule numdays days.
        The unit of time is seconds. 
        Global windows of opportunity are 8/24 hrs per day.
        There are two num_resources resources.
        We generate resnum reservations with:
        * random choice between single, and, oneof
        * single: possible windows w/ 0-max_slack slack 
        * oneof/and: two reservations
        '''
        self.numdays = 6*30
        self.avg_res_per_night_per_resource = 10
        self.resnum = self.avg_res_per_night_per_resource*2*self.numdays
        self.night_length = 8
        self.max_duration = self.night_length*60*60 # 8 hrs * 60 min * 60 sec
        self.tfinal = self.numdays * 24 * 60 * 60 # days * hrs * min *sec
        self.max_slack = 1*60*60 # 1 hr * min * sec
        self.max_priority = 100
        self.num_resources = 4

        self.gpw = {}
        self.slice_dict = {}
        tmp1 = []
        for i in range(self.numdays):
            tmp1.append(Timepoint(i*24*60*60, 'start'))
            tmp1.append(Timepoint((i*24 + self.night_length)*60*60, 
                                  'end'))
        for i in range(1,self.num_resources+1):
            tmp2 = copy.deepcopy(tmp1)
            r = str(i)
            self.gpw[r] = Intervals(tmp2)
            self.slice_dict[r] = [0,10*60] # 10 min windows


    def generate_reservation(self):
        max_duration = self.max_duration
        tfinal = self.tfinal
        max_slack = self.max_slack
        max_priority = self.max_priority

        duration = random.randint(1,max_duration)
        start_day = random.randint(0, self.numdays)
        start_time = random.randint(0, self.night_length*60*60)
        slack = random.randint(0,max_slack)
        resource = str(random.randint(1,self.num_resources))
        priority = random.randint(1, max_priority)
        start = (start_day*24)*60*60 + start_time
        window = Intervals([Timepoint(start, 'start'),
                            Timepoint(start+duration+slack, 'end')])
        reservation = Reservation_v3(priority, duration, {resource: window})
        return reservation
         

    def test_run_1(self, seed=None):
        '''Returns mean reservation duration and number of reservations
        scheduled.'''

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


        fs = CurrentScheduler(cr_list, self.gpw, [], self.slice_dict)
        s = fs.schedule_all()
        u = Util()
        #u.get_coverage_count_plot(s)
        u.find_overlaps(s)
#        return [len(s['1']),len(s['2'])]


    # def test_run_multi(self, howmany=1000):
    #     nrs1_sum = 0
    #     nrs2_sum = 0
    #     for i in range(0, howmany):
    #         [nrs1, nrs2] = self.test_run_1()
    #         nrs1_sum += nrs1
    #         nrs2_sum += nrs2
    #     print 'in ',howmany,' runs, mean number of reservations scheduled was ',nrs1_sum/howmany, ' and ', nrs2_sum/howmany

