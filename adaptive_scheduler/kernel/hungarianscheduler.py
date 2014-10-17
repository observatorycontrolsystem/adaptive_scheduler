#!/usr/bin/env python

'''
Hungarian scheduler: weighted bipartite graph-based.

Author: Sotiria Lampoudi (slampoud@gmail.com)
June 2012
'''

from reservation_v3 import *
from munkres import Munkres
from bipartitequantization import *

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
        for reservation in reservation_list:
            for resource in reservation.free_windows_dict.keys():
                self.reservations_by_resource_dict[resource].append(reservation)
        self.bq = BipartiteQuantization()
        self.create_constraint_matrix()


    def get_reservation_by_ID(self, ID):
        for r in self.reservation_list:
            if r.get_ID() == ID:
                return r
        return None


    def create_constraint_matrix(self):
        sparse_matrix = []
        current_row = 0
        current_col = 0
        for resource in self.resource_list:
            quantum = self.bq.max_duration(self.reservations_by_resource_dict[resource])
            for r in self.reservations_by_resource_dict[resource]:
                rid = r.get_ID()
                self.constraint_matrix_rows_by_res[rid] = current_row
                self.constraint_matrix_rows_by_idx[current_row] = rid
                # quantize free windows of opportunity for each reservation 
                # (first checks which windows of opportunity are still free)
                quantum_starts = self.bq.quantize_windows(r, quantum, resource)
                for q in quantum_starts:
                    # if quantum already exists, just add entry to matrix
                    if q in self.constraint_matrix_cols_by_quantum:
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
            [resource, start, quantum] = self.bq.unhash_quantum_start(quantum_start)
            r.schedule(start, quantum, resource, 
                       'Hungarian scheduler')
            self.scheduled_reservations.append(r)
        return self.scheduled_reservations
