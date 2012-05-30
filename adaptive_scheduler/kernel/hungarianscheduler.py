#!/usr/bin/env python

'''
Hungarian scheduler.

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
June 2012
'''

from reservation_v2 import *
from munkres import Munkres


class HungarianScheduler(object):

    def __init__(self, reservation_list, resource_list):
        self.reservation_list       = reservation_list
        self.scheduled_reservations = []
        self.resource_list          = resource_list
        self.reservations_by_resource_dict = {}
        self.constraint_graph       = None
        self.constraint_matrix = []
        self.constraint_matrix_rows_by_res = {} # reservations
        self.constraint_matrix_rows_by_idx = {} # reservations
        self.constraint_matrix_cols_by_quantum = {} # quantums
        self.constraint_matrix_cols_by_idx = {} # quantums
        self.priority_by_res = None
        for resource in resource_list:
            self.reservations_by_resource_dict[resource] = []
        for reservation in reservation_list:
            if reservation.resource in resource_list:
                self.reservations_by_resource_dict[reservation.resource].append(reservation)
            else: 
                print "what's this reservation doing here?"
        self.create_constraint_graph()


    def create_constraint_graph(self):
        constraint_graph = {}
        priority_by_res = {}
        # calculate quantum as max of all request lengths, per resource
        for resource in self.resource_list:
            quantum = self.max_duration(self.reservations_by_resource_dict[resource])
            for r in self.reservations_by_resource_dict[resource]:
                # quantize free windows of opportunity for each reservation 
                # (first checks which windows of opportunity are still free)
                quantum_starts = self.quantize_windows(r, quantum)
                # add to graph
                constraint_graph[r.get_ID()] = quantum_starts
                priority_by_res[r.get_ID()] = - r.priority
        self.constraint_graph = constraint_graph
        self.priority_by_res = priority_by_res


    def merge_constraints(self, resid1, resid2):
        self.constraint_graph[resid1].extend(self.constraint_graph[resid2])
        del self.constraint_graph[resid2]


    def convert_constraint_graph_to_matrix(self):
        # TODO: priority semantics fix
        sparse_matrix = []
        current_row = 0
        current_col = 0
        for r in self.constraint_graph.keys():
            self.constraint_matrix_rows_by_res[r] = current_row
            self.constraint_matrix_rows_by_idx[current_row] = r
            
            for q in self.constraint_graph[r]:
                # if quantum already exists, just add entry to matrix
                if q in self.constraint_matrix_cols_by_quantum.keys():
                    # get column number
                    col = self.constraint_matrix_cols_by_quantum[q]
                    sparse_matrix.append([current_row, col, self.priority_by_res[r]])
                else:
                    # quantum does not already exist in matrix
                    self.constraint_matrix_cols_by_quantum[q] = current_col
                    self.constraint_matrix_cols_by_idx[current_col] = q
                    sparse_matrix.append([current_row, current_col,self.priority_by_res[r]])
                    current_col += 1
                    
            current_row += 1
        self.constraint_matrix = self.convert_sparse_to_dense_matrix(sparse_matrix,
                                                                current_row, 
                                                                current_col)


    def convert_sparse_to_dense_matrix(self, sparse, rows, columns):
        dense = [[0]*columns for x in xrange(rows)]
        for r, c, p in sparse:
            dense[r][c] = p
        return dense


    def schedule(self):
        self.convert_constraint_graph_to_matrix()
        m = Munkres()
        indices = m.compute(self.constraint_matrix)
        for row, col in indices:
            reservation_ID = self.constraint_matrix_rows_by_idx[row]
            quantum_start = self.constraint_matrix_cols_by_idx[col]
            r = self.get_reservation_by_ID(reservation_ID)
            [resource, start, quantum] = self.unhash_quantum_start(quantum_start)
            r.schedule(start, quantum, resource, 'Hungarian scheduler')
            self.scheduled_reservations.append(r)
        return self.scheduled_reservations


    def max_duration(self, reservation_list):
        duration = -1
        for r in reservation_list:
            if r.duration > duration:
                duration = r.duration
        return duration


    def quantize_windows(self, reservation, quantum):
        quantum_starts = []
        resource = reservation.resource
        qss = reservation.free_windows.get_quantum_starts(quantum)
        for qs in qss:
            quantum_starts.append(self.hash_quantum_start(resource, qs, quantum))
        return quantum_starts


    def hash_quantum_start(self, resource, start, quantum):
        return "resource_"+resource+"_start_"+repr(start)+"_quantum_"+repr(quantum)


    def unhash_quantum_start(self, mystr):
        l = mystr.split("_")
        return [l[1], int(l[3]), int(l[5])]


    def get_reservation_by_ID(self, ID):
        for r in self.reservation_list:
            if r.get_ID() == ID:
                return r
        return None


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
