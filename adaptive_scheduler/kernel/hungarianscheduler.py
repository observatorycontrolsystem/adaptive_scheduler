#!/usr/bin/env python

'''
Hungarian scheduler.

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
June 2012
'''

from reservation_v3 import *
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
        self.sparse_constraint_matrix = None
        self.constraint_matrix_numrows = 0
        self.constraint_matrix_numcols = 0
        for resource in resource_list:
            self.reservations_by_resource_dict[resource] = []
        for resource in resource_list:
            for reservation in reservation_list:
                if resource in reservation.free_windows_dict.keys():
                    self.reservations_by_resource_dict[resource].append(reservation)
        self.create_constraint_matrix()


    def create_constraint_matrix(self):
        sparse_matrix = []
        current_row = 0
        current_col = 0
        for resource in self.resource_list:
            quantum = self.max_duration(self.reservations_by_resource_dict[resource])
            for r in self.reservations_by_resource_dict[resource]:
                rid = r.get_ID()
                self.constraint_matrix_rows_by_res[rid] = current_row
                self.constraint_matrix_rows_by_idx[current_row] = rid
                # quantize free windows of opportunity for each reservation 
                # (first checks which windows of opportunity are still free)
                quantum_starts = self.quantize_windows(r, quantum, resource)
                for q in quantum_starts:
                    # if quantum already exists, just add entry to matrix
                    if q in self.constraint_matrix_cols_by_quantum.keys():
                        # get column number
                        col = self.constraint_matrix_cols_by_quantum[q]
                        sparse_matrix.append([current_row, col, - r.priority])
                    else:
                        # quantum does not already exist in matrix
                        self.constraint_matrix_cols_by_quantum[q] = current_col
                        self.constraint_matrix_cols_by_idx[current_col] = q
                        sparse_matrix.append([current_row, current_col,- r.priority])
                        current_col += 1 # advance quantum idx
                current_row += 1 # advance res idx
        self.sparse_constraint_matrix = sparse_matrix
        self.constraint_matrix_numrows = current_row
        self.constraint_matrix_numcols = current_col


    def merge_constraints(self, resid1, resid2):
        # works on sparse constraint matrix
        row1 = self.constraint_matrix_rows_by_res[resid1]
        row2 = self.constraint_matrix_rows_by_res[resid2]
        count = 0
        for rid, cid, pid in self.sparse_constraint_matrix:
            if rid == row2:
                self.sparse_constraint_matrix[count][0] = row1
            count += 1


    def convert_sparse_to_dense_matrix(self, sparse, rows, columns):
        dense = [[0]*columns for x in xrange(rows)]
        for r, c, p in sparse:
            dense[r][c] = p
        return dense


    def schedule(self):
        self.constraint_matrix = self.convert_sparse_to_dense_matrix(self.sparse_constraint_matrix, self.constraint_matrix_numrows, self.constraint_matrix_numcols)

        m = Munkres()
        indices = m.compute(self.constraint_matrix)
        for row, col in indices:
            reservation_ID = self.constraint_matrix_rows_by_idx[row]
            quantum_start = self.constraint_matrix_cols_by_idx[col]
            r = self.get_reservation_by_ID(reservation_ID)
            [resource, start, quantum] = self.unhash_quantum_start(quantum_start)
            r.schedule(start, quantum, resource, 
                       'Hungarian scheduler')
            self.scheduled_reservations.append(r)
        return self.scheduled_reservations


    def max_duration(self, reservation_list):
        duration = -1
        for r in reservation_list:
            if r.duration > duration:
                duration = r.duration
        return duration


    def quantize_windows(self, reservation, quantum, resource):
        quantum_starts = []
        qss = get_quantum_starts(reservation.free_windows_dict[resource], quantum)
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

def get_quantum_starts(intervals, quantum_length):
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
