#!/usr/bin/env python

'''
test_stress_night.py

Author: Sotiria Lampoudi
May 2013

This tester makes staggered nights on each resource, and the number of 
reservations is specified per night.
'''

from nose.tools import assert_equal
import copy, random
from adaptive_scheduler.kernel.fullscheduler_v5 import FullScheduler_v5 as CurrentScheduler
from util import *
from time import time
from params_night_1 import StressTestNightParams

class TestStressNights(object):

    def setup(self):
        sp = StressTestNightParams()
        self.outputfname = sp.outputfname
        self.numresources = sp.numresources
        self.numreservations = sp.numreservations
        self.numdays = sp.numdays
        self.tfinal = sp.numdays * 24 * 60 * 60
        self.night_length = sp.night_length

        self.min_duration = sp.duration_range[0]
        self.max_duration = sp.duration_range[1]
        self.min_slack = sp.slack_range[0]
        self.max_slack = sp.slack_range[1]
        self.min_priority = sp.priority_range[0]
        self.max_priority = sp.priority_range[1]

        self.gpw = {}
        self.slice_dict = {}
        tmp1 = []
        for i in range(self.numdays):
            tmp1.append(Timepoint(i*24*60*60, 'start'))
            tmp1.append(Timepoint(i*24*60*60 + self.night_length, 
                                  'end'))
        r = str(0)
        self.gpw[r] = Intervals(tmp1)
        self.slice_dict[r] = [0,sp.slice_length]

        # TODO: offset the nights?
        for i in range(1,self.numresources): 
            tmp2 = copy.deepcopy(tmp1)
            r = str(i)
            self.gpw[r] = Intervals(tmp2)
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


    def generate_reservation(self):
        start_day = random.randint(0, self.numdays-1)
        duration = random.randint(self.min_duration, self.max_duration)
        start_time = random.randint(0, self.night_length-duration)
        priority = random.randint(self.min_priority, self.max_priority)
        slack = random.randint(self.min_slack, self.max_slack)
        resource = str(random.randint(0,self.numresources-1))

        start = start_day*24*60*60 + start_time
        window = Intervals([Timepoint(start, 'start'),
                            Timepoint(start+duration+slack, 'end')])
        reservation = Reservation_v3(priority, duration, {resource: window})
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
        print len(s['0']), elapsed
        u = Util()
        #u.get_coverage_count_plot(s)
        u.find_overlaps(s)
