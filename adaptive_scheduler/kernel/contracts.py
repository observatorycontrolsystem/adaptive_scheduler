#!/usr/bin/env python

'''
contracts.py - Contractual obligations for the scheduler.


Contains bipartite matching code sourced from:
http://code.activestate.com/recipes/123641-hopcroft-karp-bipartite-matching/

Author: Eric Saunders
        Sotiria Lampoudi
August 2011
'''

import math
from timepoint import *
from intervals import *
from reservation import *

class ContractualObligation(object):

    def __init__(self, total_time, min_quantum, possible_windows, priority,
                 name, max_quantum=None):
        self.total_time  = total_time
        self.min_quantum = min_quantum
        self.max_quantum = max_quantum
        self.priority    = priority
        self.name        = name
	# possible_windows is a list of Timepoint() objects
        self.possible_windows = Intervals(possible_windows, 'free')
        # free windows will be populated with an Intervals object holding 
        # the windows that the c.o. wants and that are free in the original 
        # schedule. This is the starting point for the c.o. scheduling.
        self.free_windows = Intervals([], 'free')
        # scheduled_windows stores windows that are allocated to this c.o.
        # It is a list of Timepoints.
        self.scheduled_windows = []
        # time_to_schedule decreases as windows are allocated to the c.o.
        self.time_to_schedule = total_time


    def __lt__(self, other):
        return self.priority < other.priority



class ContractualObligationScheduler(object):
    
    def __init__(self, existing_schedule, globally_possible_windows, 
                 contractual_obligation_list):
        self.existing_schedule           = existing_schedule
        self.contractual_obligation_list = contractual_obligation_list
        self.contractual_obligation_list.sort()
        schedule_intervals               = self.build_intervals_from_schedule(existing_schedule)
        globally_possible_windows.sort()
        absolute_start = globally_possible_windows[0].time
        absolute_end   = globally_possible_windows[-1].time
        schedule_intervals.complement(absolute_start, absolute_end)
        self.schedule_free = schedule_intervals.intersect([Intervals(globally_possible_windows, 'free')])
        for co in self.contractual_obligation_list:
            co.free_windows = co.possible_windows.intersect([self.schedule_free])
        self.quantum  = contractual_obligation_list[0].min_quantum
        for co in self.contractual_obligation_list:
            if co.min_quantum < self.quantum:
                self.quantum = co.min_quantum


    def build_intervals_from_schedule(self, schedule):
        ''' Creates intervals object from schedule (list of reservations)'''
        timepoints = []
        if schedule:
            for r in schedule:
                timepoints.append(Timepoint(r.start, 'start'))
                timepoints.append(Timepoint(r.end, 'end'))
        return(Intervals(timepoints, 'busy'))


    def find_uncontended_windows(self, obligation):
        uncontended = obligation.free_windows
        for co in self.contractual_obligation_list:
            if co == obligation:
                continue
            else:
                uncontended = uncontended.subtract(co.free_windows)
                uncontended.clean_up()
        return uncontended
    

    def find_contended_windows(self, obligation):
        uncontended = self.find_uncontended_windows(obligation)
        contended = obligation.free_windows.subtract(uncontended)
        contended.clean_up()
        return contended
                

    def schedule(self):
        unsatisfied_obligations = self.schedule_uncontended_time()
        if len(unsatisfied_obligations) == 1: 
            # if there is only 1 unsatisfied obligation, then we can 
            # see whether there is enough contended time to satisfy it.
            co = unsatisfied_obligations[0]
            contended = self.find_contended_windows(co)
            contended_time = contended.get_total_time()
            if contended_time > co.time_to_schedule:
                contended.trim_to_time(co.time_to_schedule)
                contended_time = contended.get_total_time()
            co.scheduled_windows.extend(contended.timepoints)
            co.time_to_schedule = co.time_to_schedule - contended_time
            if co.time_to_schedule > 0:
                print "Unable to satisfy an obligation"
        elif len(unsatisfied_obligations) > 1:
            self.schedule_contended_time(unsatisfied_obligations)
        # create output
        list_of_schedules = {}
        for co in self.contractual_obligation_list:
            list_of_schedules[co.name] =  self.create_schedule(co)
        return list_of_schedules
        # total_schedule = self.merge_schedule_and_obligations()
        # return total_schedule


    def schedule_uncontended_time(self):
        unsatisfied_obligations = []
        for co in self.contractual_obligation_list:
            uncontended = self.find_uncontended_windows(co)
            if not uncontended.is_empty():
                uncontended_time = uncontended.get_total_time()
                if uncontended_time > co.total_time:
                    # we have more uncontended time than we need so we have to
                    # trim some off.
                    uncontended.trim_to_time(co.total_time)
                # store the windows 
                co.scheduled_windows = uncontended.timepoints
                # figure out whether this c.o. is still unsatisfied
                co.time_to_schedule = co.total_time - uncontended.get_total_time()
            if co.time_to_schedule > 0:
                unsatisfied_obligations.append(co)
        return unsatisfied_obligations


    def schedule_contended_time(self, unsatisfied_obligations):
        constraint_graph = {}
        # get contended times & standardize into slots
        co_index = 0
        for co in unsatisfied_obligations:
            number_of_quantums_to_schedule = int(math.ceil(co.time_to_schedule / self.quantum))
            contended = self.find_contended_windows(co)
            if not contended.is_empty():
                quantum_starts = contended.get_quantum_starts(self.quantum)
                for i in range(0,number_of_quantums_to_schedule):
                    mystr = self.my_hash(co_index, i)
                    constraint_graph[mystr] = quantum_starts
            co_index = co_index + 1
        output = bipartiteMatch(constraint_graph)
        matching = output[0]
        for quantum_start, mystr in matching.iteritems():
            [co_idx, quantum_idx] = self.my_unhash(mystr)
            unsatisfied_obligations[co_idx].scheduled_windows.append(Timepoint(quantum_start, 'start'))
            unsatisfied_obligations[co_idx].scheduled_windows.append(Timepoint(quantum_start + self.quantum, 'end'))
            unsatisfied_obligations[co_idx].time_to_schedule -= self.quantum
        for co in unsatisfied_obligations:
            if co.time_to_schedule > 0:
                print "Unable to satisfy an obligation"


    def my_hash(self, co_idx, quantum_idx):
        return "co_"+repr(co_idx)+"_quantum_"+repr(quantum_idx)


    def my_unhash(self, mystr):
        l = mystr.split("_")
        return [int(l[1]),int(l[3])]


    def merge_schedule_and_obligations(self):
        total_schedule = list(self.existing_schedule)
        for co in self.contractual_obligation_list:
            subsched = self.create_schedule(co)
            total_schedule.extend(subsched)
        return total_schedule


    def create_schedule(self, co):
        subschedule = []
        scheduled_windows_intervals = Intervals(co.scheduled_windows)
        scheduled_windows_intervals.clean_up()
        co.scheduled_windows = scheduled_windows_intervals.timepoints
        for t in co.scheduled_windows:
            if t.type == 'start':
                start = t.time
            else:
                r = self.create_reservation(co.priority, start, t.time)
                subschedule.append(r)
        return subschedule
        

    def create_reservation(self, priority, start, end):
        r = Reservation(priority, end-start, start, start)
        r.start     = start
        r.end       = end
        r.scheduled = True
        return r

# Hopcroft-Karp bipartite max-cardinality matching and max independent set
# David Eppstein, UC Irvine, 27 Apr 2002

def bipartiteMatch(graph):
    '''Find maximum cardinality matching of a bipartite graph (U,V,E).
    The input format is a dictionary mapping members of U to a list
    of their neighbors in V.  The output is a triple (M,A,B) where M is a
    dictionary mapping members of V to their matches in U, A is the part
    of the maximum independent set in U, and B is the part of the MIS in V.
    The same object may occur in both U and V, and is treated as two
    distinct vertices if this happens.'''

    # initialize greedy matching (redundant, but faster than full search)
    matching = {}
    for u in graph:
        for v in graph[u]:
            if v not in matching:
                matching[v] = u
                break

    while 1:
        # structure residual graph into layers
        # pred[u] gives the neighbor in the previous layer for u in U
        # preds[v] gives a list of neighbors in the previous layer for v in V
        # unmatched gives a list of unmatched vertices in final layer of V,
        # and is also used as a flag value for pred[u] when u is in the first layer
        preds = {}
        unmatched = []
        pred = dict([(u,unmatched) for u in graph])
        for v in matching:
            del pred[matching[v]]
        layer = list(pred)

        # repeatedly extend layering structure by another pair of layers
        while layer and not unmatched:
            newLayer = {}
            for u in layer:
                for v in graph[u]:
                    if v not in preds:
                        newLayer.setdefault(v,[]).append(u)
            layer = []
            for v in newLayer:
                preds[v] = newLayer[v]
                if v in matching:
                    layer.append(matching[v])
                    pred[matching[v]] = v
                else:
                    unmatched.append(v)

        # did we finish layering without finding any alternating paths?
        if not unmatched:
            unlayered = {}
            for u in graph:
                for v in graph[u]:
                    if v not in preds:
                        unlayered[v] = None
            return (matching,list(pred),list(unlayered))

        # recursively search backward through layers to find alternating paths
        # recursion returns true if found path, false otherwise
        def recurse(v):
            if v in preds:
                L = preds[v]
                del preds[v]
                for u in L:
                    if u in pred:
                        pu = pred[u]
                        del pred[u]
                        if pu is unmatched or recurse(pu):
                            matching[v] = u
                            return 1
            return 0

        for v in unmatched: recurse(v)
