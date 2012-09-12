#!/usr/bin/env python

'''
test_stress_realistic_2.py

Author: Sotiria Lampoudi
Sept 2011
'''

from nose.tools import assert_equal
import copy, random
from adaptive_scheduler.kernel.fullscheduler_v5 import *
from util import *

class TestStress_realistic(object):

    def setup(self):
        ''' In this test we schedule numdays days.
        The unit of time is seconds. 
        Global windows of opportunity are 8/24 hrs per day.
        There are two resources, '1' and '2'.
        We generate resnum reservations with:
        * random choice between single, and, oneof
        * single: possible windows w/ 0-max_slack slack 
        * oneof/and: two reservations
        '''
        self.numdays = 3*30
        self.avg_res_per_night_per_resource = 10
        self.resnum = self.avg_res_per_night_per_resource*2*self.numdays
        self.night_length = 8
        self.max_duration = self.night_length*60*60 # 8 hrs * 60 min * 60 sec
        self.tfinal = self.numdays * 24 * 60 * 60 # days * hrs * min *sec
        self.max_slack = 1*60*60 # 1 hr * min * sec
        self.max_priority = 100

        self.gpw = {}
        tmp1 = []
        tmp2 = []
        for i in range(self.numdays):
            # on resource '1' windows are aligned with 0
            tmp1.append(Timepoint(i*24*60*60, 'start'))
            tmp1.append(Timepoint((i*24 + self.night_length)*60*60, 
                                  'end'))
            # on resource '2' windows are aligned with hour 8
            align = 8
            tmp2.append(Timepoint((i*24+align)*60*60, 'start'))
            tmp2.append(Timepoint((i*24+align+self.night_length)*60*60,
                                  'end'))
        self.gpw['1'] = Intervals(tmp1)
        self.gpw['2'] = Intervals(tmp2)


    def generate_reservation(self):
#        priority = self.priority
        max_duration = self.max_duration
        tfinal = self.tfinal
        max_slack = self.max_slack
        max_priority = self.max_priority

        duration = random.randint(1,max_duration)
        start_day = random.randint(0, self.numdays)
        start_time = random.randint(0, self.night_length*60*60)
        slack = random.randint(0,max_slack)
        resource = str(random.randint(1,2))
        priority = random.randint(1, max_priority)
        align = 0
        if resource == '2':
            align = 8
        start = (start_day*24+align)*60*60 + start_time
        window = Intervals([Timepoint(start, 'start'),
                            Timepoint(start+duration+slack, 'end')])
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
	slice_dict['1'] = [1,10*60] # 10 min windows
	slice_dict['2'] = [1,10*60] # 10 min windows
        fs = FullScheduler_v5(cr_list, gpw, [], slice_dict)
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

# 100 1.644
# 500 11.670
# 1000 29.969
# 5000 
