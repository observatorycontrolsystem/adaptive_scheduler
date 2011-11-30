#!/usr/bin/env python

'''
intervals.py - Class for specifying and manipulating lists of intervals.

Methods that alter self:
complement(), 
clean_up(), 
trim_to_time(),
add() -- November 2011

Methods that return a new Intervals object:
subtract(), 
intersect()

Author: Sotiria Lampoudi
August 2011
edited: November 2011 -- added .add(), .find_interval_of_length() methods
bugfixes in clean_up() and subtract() in November 2011
'''

from timepoint import *
import math

class Intervals(object):
	
    IntervalsID = 0
    def __init__(self, timepoints, type=None):
        # type should be 'busy' or 'free'
        self.timepoints       = timepoints
	self.timepoints.sort()
	self.clean_up()
        self.type             = type
        Intervals.IntervalsID = Intervals.IntervalsID+1
        self.IntervalsID      = Intervals.IntervalsID
        

    def __str__(self):
        string = ""
        for t in self.timepoints:
            if t.type == 'start':
                string += repr(t.time) + "(start) "
            else:
                string += repr(t.time) + "(end) "
        return string


    def is_empty(self):
        if self.timepoints == []:
            return True
        else:
            return False


    def add(self, timepoints):
        self.timepoints.extend(timepoints)
        self.clean_up()


    def get_total_time(self):
        ''' Returns the total amount of time in the intervals '''
        sum   = 0
        start = 0
	self.clean_up()
        if self.timepoints:
            for t in self.timepoints:
                if t.type == 'start':
                    start = t.time
                else: 
                    sum = sum + (t.time - start)
        return sum


    def find_interval_of_length(self, length):
        start = 0
        for t in self.timepoints:
            if t.type == 'start':
                start = t.time
            else:
                duration = t.time - start
                if duration >= length:
                    return start
        return -1
    

    def trim_to_time(self, total_time):
        ''' Trims the intervals from the beginning, so they sum up to 
        total_time. Alters the Intervals object itself, returns nothing.'''
        sum         = 0
        start       = 0
        trimmed_tps = []
        self.timepoints.sort()
        for t in self.timepoints:
            if t.type == 'start':
                start = t.time
                trimmed_tps.append(t)
            else: 
                sum = sum + (t.time - start)
                if sum > total_time:
                    trim_time = sum-total_time
                    trimmed_tps.append(Timepoint(t.time-trim_time, 'end'))
                    self.timepoints = trimmed_tps
                    return
                else:
                    trimmed_tps.append(t)
                if sum == total_time:
                    self.timepoints = trimmed_tps
                    return
        if sum < total_time:
            # TODO: this should be an exception
            print "error: asked me to trim intervals to more than their total time\n"


    def clean_up(self):
        if self.timepoints:
            self.timepoints.sort()
            # remove end & start with same time -- they cancel
            previous_tp   = None
            clean_tps     = []
            for t in self.timepoints:
                if (previous_tp and
                    (previous_tp.time == t.time) and 
                    (previous_tp.type == 'end') and
                    (t.type == 'start')):
                    clean_tps.pop()
                    if clean_tps:
                        previous_tp = clean_tps[-1]
                    else: 
                        previous_tp = None
                else:
                    clean_tps.append(t)
                    previous_tp = t
            # remove nested intervals
            self.timepoints = clean_tps
            clean_tps = []
            flag      = 0
            for t in self.timepoints:
                if t.type == 'start':
                    if flag < 1:
                        clean_tps.append(t)
                    flag += 1
                elif t.type == 'end':
                    if flag == 1:
                        clean_tps.append(t)
                    flag -= 1
            self.timepoints = clean_tps
            # # remove first tp if it's an end
            # if self.timepoints[0].type == 'end':
            #     self.timepoints.pop(0)
        

    def complement(self, absolute_start, absolute_end):
        ''' Turns a list of intervals denoting free times
        into a list denoting busy times and vice versa. 
        Replaces self.timepoints and returns nothing.
        absolute_start and absolute_end must be defined, so we know 
        how to close off the ends. '''
        if self.timepoints:
            self.timepoints.sort()
            # figure out the start
            if self.timepoints[0].time == absolute_start:
                start = self.timepoints[1].time
                self.timepoints.pop(0)
                self.timepoints.pop(0)
            else: 
                start = absolute_start
            complemented_tps = [Timepoint(start, 'start')]
            for t in self.timepoints:
                if t.type == 'start':
                    complemented_tps.append(Timepoint(t.time, 'end'))
                else:
                    complemented_tps.append(Timepoint(t.time, 'start')) 
            if complemented_tps[-1].type == 'start':
                if complemented_tps[-1].time == absolute_end:
                    complemented_tps.pop()
                else:
                    complemented_tps.append(Timepoint(absolute_end, 'end'))
            # store complemented_timepoints
            self.timepoints = complemented_tps

        else:
            self.timepoints.append(Timepoint(absolute_start, 'start'))
            self.timepoints.append(Timepoint(absolute_end, 'end'))
        # if the type is defined, swap it
        if self.type == 'free':
            self.type = 'busy'
        elif self.type == 'busy':
            self.type = 'free'

    def intersect(self, list_of_others):
        ''' Intersects Intervals in list_of_others with self. Returns
        a new Intervals object containing only those intervals that 
        were in the intersection of everything. If the intersection
        was empty, it returns None.'''
        intersection = []
        merged_timepoints = list(self.timepoints)
        # merge all lists of timepoints
        for other in list_of_others:
            merged_timepoints.extend(other.timepoints)
        # sort the merged list
        merged_timepoints.sort()
        # walk through merged list popping up flags
        flag     = 0
        max_flag = len(list_of_others)+1
        for t in merged_timepoints:
            if t.type == 'start':
                if flag == max_flag:
                    intersection.append(Timepoint(t.time, 'end'))
                flag += 1
                if flag == max_flag:
                    intersection.append(Timepoint(t.time, 'start'))
            elif t.type == 'end':
                if flag == max_flag:
                    intersection.append(Timepoint(t.time, 'end'))
                flag -= 1
                if flag == max_flag:
                    intersection.append(Timepoint(t.time, 'start'))
        if intersection:
            return Intervals(intersection, self.type)
        else:
            return Intervals([])


    def subtract(self, other):
        ''' Returns a new Intervals object containing those intervals in self
        that are not in other (i.e. the relative complement of other in self).
        '''
        if (other == None) or other.is_empty() or self.is_empty():
            return self
        rc = []
        for t in other.timepoints:
            t.id=1
        for t in self.timepoints:
            t.id=2
        merged_timepoints = list(self.timepoints)
        # merge the two lists
        merged_timepoints.extend(other.timepoints)
        # sort the merged list
        merged_timepoints.sort()
        # walk through merged list popping up flags
        flag = 0
        for t in merged_timepoints:
            if t.type == 'start':
                if flag == 2:
                    rc.append(Timepoint(t.time, 'end'))
                flag = flag + t.id
                if flag == 2:
                    rc.append(Timepoint(t.time, 'start'))
            elif t.type == 'end':
                if flag == 2:
                    rc.append(Timepoint(t.time, 'end'))
                flag = flag - t.id
                if flag == 2:
                    rc.append(Timepoint(t.time, 'start'))
        if self.type == other.type:
            type = self.type
        else:
            type = None
        if rc:
            rci = Intervals(rc, type)
            rci.clean_up()
            return rci
        else:
            return Intervals([], type)


    def get_quantum_starts(self, quantum_length):
        ''' Returns a list of the start times of quantums of quantum_length,
        and aligned with quantum_length boundary in the intervals. '''
        quantum_starts = []
        self.timepoints.sort()
        for t in self.timepoints:
            if t.type == 'start':
            # align the start with a quantum boundary
                start = int(math.ceil(float(t.time)/float(quantum_length))*quantum_length)
            else:
                tmp = range(start, t.time, quantum_length)
                # figure out whether the last quantum is whole
                if tmp[-1] + quantum_length > t.time:
                    tmp.pop()
                quantum_starts.extend(tmp)
        return quantum_starts
