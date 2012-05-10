#!/usr/bin/env python

'''
MetricsPreSchedulerScalar
MetricsPreSchedulerVector

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
Feb 2012, May 2012
'''

from reservation_v2 import *
from intervals import *
from timepoint import *
import copy
from metrics import *

class MetricsPreSchedulerScalar(Metrics):

    def get_number_of_compound_reservations(self, type=None):
        ''' Without argument, returns number of all c.r.s
        possible args are 'oneof', 'and', 'single', and return
        number of c.r.s of that type'''
        if type=='oneof':
            return len(self.oneof_constraints)
        elif type=='and':
            return len(self.and_constraints)
        elif type=='single':
            count = 0
            for cr in self.compound_reservation_list:
                if cr.issingle():
                    count+=1
            return count
        else:
            return len(self.compound_reservation_list)


    def get_number_of_contractual_obligations(self):
        return len(self.contractual_obligation_list)


    def get_number_of_resources(self):
        return len(self.resource_list)



class MetricsPreSchedulerVector(Metrics):

    def get_coverage_by_resource(self, resource, mode):
        ''' Two modes: binary, count. 
        In binary mode: 
        * undefined if the interval is not in globally_possible_windows,
        * 1 if the interval is requested by some reservation on that resource,
        * 0 otherwise
        In count mode: 
        * undefined is same
        * count of reservations covering that interval on that resource,
        * 0 otherwise.
        '''
        if resource in self.resource_list:
            self.current_resource = resource
            reservation_list = filter(self.resource_equals, self.reservation_list)
            available_windows = copy.copy(self.globally_possible_windows_dict[resource])
            available_windows.clean_up()
            iu = IntervalsUtility()
            if reservation_list:
                intervals_list = [r.possible_windows for r in reservation_list]
                if mode == 'binary':
                    retlist = iu.get_coverage_binary(available_windows, intervals_list)
                elif mode == 'count':
                    retlist = iu.get_coverage_count(available_windows, intervals_list)
            else: 
                # this means we got globally_available_windows for this 
                # resource, but no reservations. So all the windows in
                # g_a_w are 0.
                retlist = iu.intervals_to_retval(available_windows, 0)
            retlist.sort()
            return retlist
        else:
            print "Error: resource not in resource list."
