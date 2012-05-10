#!/usr/bin/env python

'''
Metrics

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
May 2012
'''

from reservation_v2 import *
from intervals import *
from timepoint import *
import copy


class Metrics(object):
    def __init__(self, compound_reservation_list, 
                 globally_possible_windows_dict, 
                 contractual_obligation_list):
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


    def convert_compound_to_simple(self):
        ''' From fullscheduler_v1'''
        reservation_list = []
        for cr in self.compound_reservation_list:
            if cr.issingle():
                reservation_list.append(cr.reservation_list[0])
            elif cr.isoneof():
                reservation_list.extend(cr.reservation_list)
                self.oneof_constraints.append(cr.reservation_list)
            elif cr.isand():
                reservation_list.extend(cr.reservation_list)
                # add the constraint to the list of constraints
                self.and_constraints.append(cr.reservation_list)
        return reservation_list

    
    def resource_equals(self, x):
        ''' From fullscheduler_v1'''
        if (self.current_resource == x.resource):
            return True
        else:
            return False
