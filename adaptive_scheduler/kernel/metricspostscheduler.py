#!/usr/bin/env python

'''
MetricsPostSchedulerScalar
MetricsPostSchedulerVector

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
May 2012
'''

from reservation_v3 import *
from intervals import *
from timepoint import *
import copy
from scheduler import *

class MetricsPostScheduler(Scheduler):

    def __init__(self, compound_reservation_list,
                 globally_possible_windows_dict,
                 contractual_obligation_list,
                 schedule_dict):
        Scheduler.__init__(self, compound_reservation_list,
                 globally_possible_windows_dict,
                 contractual_obligation_list)
        self.schedule_dict = schedule_dict



class MetricsPostSchedulerScalar(MetricsPostScheduler):

    def get_fraction_of_crs_scheduled(self, type=None):
        ''' Returns the fraction of c.r.s that have been scheduled.
        If the c.r. type is specified in the optional argument, then
        it returns the fraction for only that type of c.r.'''
        scheduled_count = 0
        type_count      = 0
        for cr in self.compound_reservation_list:
            if type == None:
                if cr.scheduled:
                    scheduled_count+=1
            elif cr.type == type:
                type_count+=1
                if cr.scheduled:
                    scheduled_count+=1
        if type == None:
            type_count = len(self.compound_reservation_list)
        return float(scheduled_count)/float(type_count)


#class MetricsPostSchedulerVector(MetricsPostScheduler):
