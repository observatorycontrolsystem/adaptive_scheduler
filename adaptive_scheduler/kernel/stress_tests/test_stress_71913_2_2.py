#!/usr/bin/env python

'''
Author: Sotiria Lampoudi
July 2013

This tester makes staggered nights (by 6 hrs) on each resource. 
'''

from nose.tools import assert_equal
import copy, random
from adaptive_scheduler.kernel.fullscheduler_v5 import FullScheduler_v5 as CurrentScheduler
from util import *
from time import time
from params_71913_2_2 import StressTestNightParams

class TestStressNights(object):

    def setup(self):
        sp = StressTestNightParams()
        self.outputfname = sp.outputfname
        self.numresources = sp.numresources
        self.numreservations = sp.numreservations
        self.numdays = sp.numdays
        self.tfinal = sp.tfinal
        self.night_length = sp.night_length

        self.min_duration = sp.duration_range[0]
        self.max_duration = sp.duration_range[1]
        self.min_slack = sp.slack_range[0]
        self.max_slack = sp.slack_range[1]
        self.min_priority = sp.priority_range[0]
        self.max_priority = sp.priority_range[1]

        self.gpw = {}
        self.slice_dict = {}
        tmps_dict = {}
        for j in range(0,self.numresources):
            r = str(j)
            tmps_dict[r] = []
            for i in range(self.numdays):
                tmps_dict[r].append(Timepoint((i+j*6)*24*60, 'start'))
                tmps_dict[r].append(Timepoint((i+j*6)*24*60 + 
                                              self.night_length, 
                                              'end'))
                self.gpw[r] = Intervals(tmps_dict[r])
                self.slice_dict[r] = [0,sp.slice_length]

        self.no_oneofs = False
        self.no_ands = False
        self.max_restype = 1
        if sp.oneof_elt_num_range[0] < 2: #== sp.oneof_elt_num_range[1]:
            self.no_oneofs = True
        else:
            self.oneof_elt_num_min = sp.oneof_elt_num_range[0] 
            self.oneof_elt_num_max = sp.oneof_elt_num_range[1] 
            self.max_restype += 1
        if sp.and_elt_num_range[0] < 2: #== sp.and_elt_num_range[1]:
            self.no_ands = True
        else:
            self.and_elt_num_min = sp.and_elt_num_range[0] 
            self.and_elt_num_max = sp.and_elt_num_range[1] 
            self.max_restype += 1


    def generate_reservation(self):
        start_day = random.randint(0, self.numdays-1)
     #   duration = random.randint(self.min_duration, self.max_duration)
     #   start_time = random.randint(0, self.night_length-duration)
     #   priority = random.randint(self.min_priority, self.max_priority)
     #   slack = random.randint(self.min_slack, self.max_slack)
     #   resource = str(random.randint(0,self.numresources-1))

        start = start_day*24*60 
#        window = Intervals([Timepoint(start, 'start'),
#                            Timepoint(start+duration+slack, 'end')])
        tmps_dict = {}
        for j in range(0,self.numresources):
            r = str(j)
            tmps_dict[r] =  Intervals([Timepoint(start+j*6*60, 'start'),
                                       Timepoint(start+j*6*60+12*60, 'end')])

#        reservation = Reservation_v3(priority, duration, {resource: window})
        reservation = Reservation_v3(1, 30, tmps_dict)
        return reservation
         

    def test_run_1(self, seed=None):
        if seed:
            random.seed(seed)
        cr_list = []
        for i in range(0, self.numreservations):
            reservation = self.generate_reservation()
            reslist = [reservation]
            restype = random.randint(1,self.max_restype)
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

        tstart =  time()
        fs = CurrentScheduler(cr_list, self.gpw, [], self.slice_dict)
        s = fs.schedule_all()
        elapsed = time() - tstart
        u = Util()
        #u.get_coverage_count_plot(s)
        u.find_overlaps(s)
	sched_count = sum(len(s[k]) for k in s.keys())
	utilization = u.get_total_scheduled_time(s)
	res_count_in = len(fs.reservation_list)
	print res_count_in, sched_count, utilization, elapsed
