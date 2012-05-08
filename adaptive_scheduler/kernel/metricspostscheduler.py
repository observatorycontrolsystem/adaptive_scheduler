#!/usr/bin/env python

'''
MetricsPostSchedulerScalar
MetricsPostSchedulerVector

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
May 2012
'''

from reservation_v2 import *
from intervals import *
from timepoint import *
import copy


class MetricsPostSchedulerScalar(object):

    def __init__(self, compound_reservation_list,
                 globally_possible_windows_dict,
                 contractual_obligation_list,
                 schedule_dict):
        self.compound_reservation_list   = compound_reservation_list
        self.contractual_obligation_list = contractual_obligation_list
        # globally_possible_windows_dict is a dictionary mapping:               
        # resource -> globally possible windows (Intervals) on that resource.   
        self.globally_possible_windows_dict   = globally_possible_windows_dict
        # resource_list holds the schedulable resources.                        
        self.resource_list = globally_possible_windows_dict.keys()
        self.and_constraints   = []
        self.oneof_constraints = []
        self.reservation_list  = self.convert_compound_to_simple()
        self.current_resource  = None
        self.schedule_dict = schedule_dict



class MetricsPostSchedulerVector(object):

    def __init__(self, compound_reservation_list,
                 globally_possible_windows_dict,
                 contractual_obligation_list,
                 schedule_dict):
        self.compound_reservation_list   = compound_reservation_list
        self.contractual_obligation_list = contractual_obligation_list
        # globally_possible_windows_dict is a dictionary mapping:               
        # resource -> globally possible windows (Intervals) on that resource.   
        self.globally_possible_windows_dict   = globally_possible_windows_dict
        # resource_list holds the schedulable resources.                        
        self.resource_list = globally_possible_windows_dict.keys()
        self.and_constraints   = []
        self.oneof_constraints = []
        self.reservation_list  = self.convert_compound_to_simple()
        self.current_resource  = None
        self.schedule_dict = schedule_dict
        return
