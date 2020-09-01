#!/usr/bin/env python
'''
Scheduler is the base class for all schedulers. 

Author: Sotiria Lampoudi (slampoud@gmail.com)
Dec 2012
'''

import copy
from time_intervals.intervals import Intervals


class Scheduler(object):

    def __init__(self, compound_reservation_list,
                 globally_possible_windows_dict,
                 contractual_obligation_list):
        self.compound_reservation_list = compound_reservation_list
        self.contractual_obligation_list = contractual_obligation_list
        # globally_possible_windows_dict is a dictionary mapping:
        # resource -> globally possible windows (Intervals) on that resource. 
        self.globally_possible_windows_dict = globally_possible_windows_dict
        # these dictionaries hold:
        # scheduled reservations
        self.schedule_dict = {}
        # busy intervals
        self.schedule_dict_busy = {}
        # free intervals
        self.schedule_dict_free = {}

        # sanity check: walk through and make sure none of the globally
        # possible windows are empty. Do the iteration over keys, because
        # we're modifying the dict as we go.
        for r in list(globally_possible_windows_dict.keys()):
            if globally_possible_windows_dict[r].is_empty():
                del globally_possible_windows_dict[r]
        # resource_list holds the schedulable resources.
        # possible windows specified by reservations may include
        # resources not on this list, but we cannot schedule them because
        # we do not know their globally possible windows.

        self.resource_list = list(globally_possible_windows_dict.keys())

        for resource in self.resource_list:
            # reservation list
            self.schedule_dict[resource] = []
            # busy intervals
            self.schedule_dict_busy[resource] = Intervals([], 'busy')
        # free intervals
        self.schedule_dict_free = copy.deepcopy(globally_possible_windows_dict)

        self.and_constraints = []
        self.oneof_constraints = []
        self.reservation_list, self.reservation_dict = self.convert_compound_to_simple()
        self.unscheduled_reservation_list = copy.copy(self.reservation_list)

        self.reservations_by_resource_dict = {}
        for resource in self.resource_list:
            self.reservations_by_resource_dict[resource] = []
        for reservation in self.reservation_list:
            for resource in reservation.free_windows_dict.keys():
                self.reservations_by_resource_dict[resource].append(reservation)

    def make_free_windows_consistent(self, reservation_list):
        '''Use this when some windows have been made busy in the global 
        schedule, but there are reservations that don't know about it. This
        could also be done in commit, but it's not required by all schedulers
        (only multi-pass ones), so it's better to keep it separate.'''
        for reservation in reservation_list:
            for resource in reservation.free_windows_dict.keys():
                reservation.free_windows_dict[resource] = reservation.free_windows_dict[resource].subtract(
                    self.schedule_dict_busy[resource])

    def order_equals(self, x):
        if (x.order == self.current_order):
            return True
        else:
            return False

    def resources_include(self, x):
        if (self.current_resource in x.free_windows_dict[resource]):
            return True
        else:
            return False

    # old list-based implementation
    # def get_reservation_by_ID(self, ID):
    #     for r in self.reservation_list:
    #         if r.get_ID() == ID:
    #             return r
    #     return None
    # new dict-based implementation
    def get_reservation_by_ID(self, ID):
        if ID in self.reservation_dict:
            return self.reservation_dict[ID]
        else:
            return None

    def check_against_gpw(self, reservation):
        # intersect the free_windows of reservation with the 
        # globally_possible_windows at each resource.
        # if there are no more free_windows in a specific resource, then 
        # remove that resource from the free_windows_dict.
        # if there are NO MORE resources, then return False.
        for resource in list(reservation.free_windows_dict.keys()):
            reservation.free_windows_dict[resource] = reservation.free_windows_dict[resource].intersect(
                [self.globally_possible_windows_dict.get(resource, Intervals([]))])
            reservation.clean_up_free_windows(resource)
            if reservation.free_windows_dict[resource].is_empty():
                del (reservation.free_windows_dict[resource])
        if reservation.free_windows_dict:
            return True
        else:
            return False

    def convert_compound_to_simple(self):
        reservation_list = []
        reservation_dict = {}
        for cr in self.compound_reservation_list:
            if cr.issingle():
                if self.check_against_gpw(cr.reservation_list[0]):
                    reservation_list.append(cr.reservation_list[0])
                    reservation_dict[cr.reservation_list[0].resID] = cr.reservation_list[0]
            elif cr.isoneof():
                tmp_list = []
                for reservation in cr.reservation_list:
                    if self.check_against_gpw(reservation):
                        tmp_list.append(reservation)
                        reservation_dict[reservation.resID] = reservation
                if tmp_list:
                    reservation_list.extend(tmp_list)
                self.oneof_constraints.append(tmp_list)
            elif cr.isand():
                all_good = True
                for reservation in cr.reservation_list:
                    if not self.check_against_gpw(reservation):
                        all_good = False
                if all_good:
                    reservation_list.extend(cr.reservation_list)
                    # add the constraint to the list of constraints
                    self.and_constraints.append(cr.reservation_list)
                    for r in cr.reservation_list:
                        reservation_dict[r.resID] = r
        return reservation_list, reservation_dict

    def commit_reservation_to_schedule(self, r):
        if not r.scheduled:
            print("error: trying to commit unscheduled reservation")
            return
        else:
            interval = Intervals(r.scheduled_timepoints)
            self.schedule_dict[r.scheduled_resource].append(r)

            # add interval & remove free time
            self.schedule_dict_busy[r.scheduled_resource].add(r.scheduled_timepoints)
            self.schedule_dict_free[r.scheduled_resource] = self.schedule_dict_free[r.scheduled_resource].subtract(
                interval)
            # remove from list of unscheduled reservations
            self.unscheduled_reservation_list.remove(r)
            # if we need to remove scheduled time from free windows of other 
            # reservations, then we need to call self.make_windows_consistent()

    def uncommit_reservation_from_schedule(self, r):
        resource = r.scheduled_resource
        self.schedule_dict[resource].remove(r)
        # remove interval & add back free time
        self.schedule_dict_free[resource].add(r.scheduled_timepoints)
        self.schedule_dict_busy[resource].subtract(Intervals(r.scheduled_timepoints, 'free'))
        self.unscheduled_reservation_list.append(r)
        r.unschedule()
        # TODO?: add back the window to those reservations that originally
        # included it in their possible_windows list.
        # Not bothering with this now since there is no pass following 
        # this that could benefit from this information.
