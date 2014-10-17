#!/usr/bin/env python

'''
BipartiteQuantization provides the utility of calculating, hashing and 
unhashing quantum starts for the family of bipartite matching-based 
schedulers.

Author: Sotiria Lampoudi (slampoud@gmail.com)
Dec 2012
'''

from reservation_v3 import *

class BipartiteQuantization(object):

    def quantize_windows(self, reservation, quantum, resource):
        quantum_starts = []
        qss = self.get_quantum_starts(reservation.free_windows_dict[resource], quantum)
        for qs in qss:
            quantum_starts.append(self.hash_quantum_start(resource, qs, quantum))
        return quantum_starts


    def hash_quantum_start(self, resource, start, quantum):
        return "resource_"+resource+"_start_"+repr(start)+"_quantum_"+repr(quantum)


    def unhash_quantum_start(self, mystr):
        l = mystr.split("_")
        return [l[1], int(l[3]), int(l[5])]


    def max_duration(self, reservation_list):
        duration = -1
        for r in reservation_list:
            if r.duration > duration:
                duration = r.duration
        return duration


    def get_quantum_starts(self, intervals, quantum_length):
        ''' Returns a list of the start times of quantums of quantum_length,
        and aligned with quantum_length boundary in the intervals. '''
        quantum_starts = []
        intervals.timepoints.sort()
        for t in intervals.timepoints:
            if t.type == 'start':
                # align the start with a quantum boundary
                start = int(math.ceil(float(t.time)/float(quantum_length))*quantum_length)
            else:
                tmp = range(start, t.time, quantum_length)
                if tmp:
                    # figure out whether the last quantum is whole
                    if tmp[-1] + quantum_length > t.time:
                        tmp.pop()
                    quantum_starts.extend(tmp)
        return quantum_starts
