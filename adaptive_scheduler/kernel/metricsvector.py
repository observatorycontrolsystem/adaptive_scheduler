#!/usr/bin/env python

'''
MetricsVector

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
February 2012
'''

from reservation_v2 import *
from intervals import *
from timepoint import *
import copy

class MetricsVector(object):

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

    # copied from FullScheduler_v1
    def convert_compound_to_simple(self):
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


    # copied from FullScheduler_v1
    def resource_equals(self, x):
        if (self.current_resource == x.resource):
            return True
        else:
            return False


    def get_coverage_by_resource(self, resource):
        # undefined if the interval is not in globally_possible_windows,
        # 1 if the interval is requested by some reservation on that resource,
        # 0 otherwise
        if resource in self.resource_list:
            self.current_resource = resource
            reservation_list = filter(self.resource_equals, self.reservation_list)
            available_windows = self.globally_possible_windows_dict[resource]
            available_windows.clean_up()
            if reservation_list:
                still_available_windows = copy.copy(available_windows)
                for reservation in reservation_list:
                    still_available_windows = still_available_windows.subtract(reservation.possible_windows)
                retlist = self.intervals_to_retval(still_available_windows, 0)
                busy_windows = available_windows.subtract(still_available_windows)
                retlist.extend(self.intervals_to_retval(busy_windows, 1))
            else: 
                # this means we got globally_available_windows for this 
                # resource, but no reservations. So all the windows in
                # g_a_w are 0.
                retlist = self.intervals_to_retval(available_windows, 0)
            retlist.sort()
            return retlist
        else:
            print "Error: resource not in resource list."


    def intervals_to_retval(self, intervals, retval):
        retlist = [] #  [[start, end, retval]] format
        current_interval = []
        for tp in intervals.timepoints:
            current_interval.append(tp.time)
            if tp.type == 'end':
                current_interval.append(retval)
                retlist.append(copy.copy(current_interval))
                current_interval = []
        return retlist
