#!/usr/bin/env python

'''
stresstester.py

Author: Sotiria Lampoudi
May 2013

This tester makes one big global availability window (0,tfinal) on each 
resource.
'''

import copy, random
#from adaptive_scheduler.kernel.fullscheduler_v3 import FullScheduler_v3 as CurrentScheduler
from adaptive_scheduler.kernel.fullscheduler_v5 import FullScheduler_v5 as CurrentScheduler
from util import *
from time import time
from params_3 import StressTestParams


class TestStress(object):

    def setup(self):
        sp = StressTestParams()
        self.outputfname = sp.outputfname
        self.numresources = sp.numresources
        self.numreservations = sp.numreservations
        self.tfinal = sp.tfinal

        self.min_duration = sp.duration_range[0]
        self.max_duration = sp.duration_range[1]
        self.min_slack = sp.slack_range[0]
        self.max_slack = sp.slack_range[1]
        self.min_priority = sp.priority_range[0]
        self.max_priority = sp.priority_range[1]

        # prepare global windows
        self.gpw = {}
        self.slice_dict = {}
        for i in range(sp.numresources):
            r = str(i)
            self.gpw[r] = Intervals([Timepoint(0, 'start'), 
                                     Timepoint(self.tfinal, 'end')])
            self.slice_dict[r] = [0,sp.slice_length]

        self.no_oneofs = False
        self.no_ands = False
        self.max_restype = 1
        if sp.oneof_elt_num_range[0] == sp.oneof_elt_num_range[1]:
            self.no_oneofs = True
        else:
            self.oneof_elt_num_min = sp.oneof_elt_num_range[0] 
            self.oneof_elt_num_max = sp.oneof_elt_num_range[1] 
            self.max_restype += 1
        if sp.and_elt_num_range[0] == sp.and_elt_num_range[1]:
            self.no_ands = True
        else:
            self.and_elt_num_min = sp.and_elt_num_range[0] 
            self.and_elt_num_max = sp.and_elt_num_range[1] 
            self.max_restype += 1
        self.sl_util = {}
        self.sl_num = {}
        for slice_length in range(120,660,60):
            self.sl_util[slice_length] = []
            self.sl_num[slice_length] = []
        

    def generate_reservation(self):
        duration = random.randint(self.min_duration, self.max_duration)
        start_time = random.randint(0, self.tfinal-duration)
        priority = random.randint(self.min_priority, self.max_priority)
        slack = random.randint(self.min_slack, self.max_slack)

        resource = str(random.randint(0,self.numresources-1))
        window = Intervals([Timepoint(start_time, 'start'),
                            Timepoint(start_time+duration+slack, 'end')])
        reservation = Reservation_v3(priority, duration, {resource: window})
        return reservation
         

    def test_run_1(self, seed=None):
        if seed:
            random.seed(seed)
        cr_list = []
        for i in range(self.numreservations):
            reservation = self.generate_reservation()
            reslist = [reservation]
            restype = random.randint(1, self.max_restype)

            if restype == 1:
                cr = CompoundReservation_v2(reslist)
            elif restype == 2 and not self.no_oneofs:
                resnum = random.randint(self.oneof_elt_num_min-1, 
                                        self.oneof_elt_num_max-1)
                for j in range(resnum):
                    reslist.append(self.generate_reservation())
                cr = CompoundReservation_v2(reslist,'oneof')
            else: 
                resnum = random.randint(self.and_elt_num_min-1, 
                                        self.and_elt_num_max-1)
                for j in range(resnum):
                    reslist.append(self.generate_reservation())
                cr = CompoundReservation_v2(reslist,'and')
                
            cr_list.append(cr)
        for slice_length in [1,2,5,10,30,60,120,180]:
            tstart =  time()
            if slice_length==0:
                slice_length = 1
            self.slice_dict['0'] = [0, slice_length]
            fs2 = CurrentScheduler(cr_list, self.gpw, [], self.slice_dict)
            s2 = fs2.schedule_all()
            tend = time()
            u = Util()
            #u.find_overlaps(s2)
            print slice_length, len(s2['0']), u.get_total_scheduled_time(s2), tend-tstart


    # def test_run_many(self, howmany=500):
    #     for i in range(howmany):
    #         self.run_1()
    #     for slice_length in range(120,660,60):
    #         print slice_length, sum(self.sl_util[slice_length])/float(len(self.sl_util[slice_length])), sum(self.sl_num[slice_length])/float(len(self.sl_num[slice_length])) 
