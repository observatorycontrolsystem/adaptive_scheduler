#!/usr/bin/env python

'''
FullScheduler_v6 class for co-scheduling reservations 
across multiple resources using time-slicing and an integer program.

Because time is discretized into time slices, this scheduler requires
information about how to generate the slices, so its signature has one
more argument than usual. 

This implementation uses a SPARSE matrix representation and direct binding
to CVXOPT + glpk.

Author: Sotiria Lampoudi (slampoud@gmail.com)
January 2014
'''

from reservation_v3 import *
#from contracts_v2 import *
import copy
import numpy
#from openopt import LP
from scipy.sparse import coo_matrix
#from slicedipscheduler import *
from slicedipscheduler_v2 import *
from adaptive_scheduler.utils import timeit

import cvxopt
from cvxopt.glpk import ilp

class Result(object):
    pass

#class FullScheduler_v5(SlicedIPScheduler):
class FullScheduler_v6(SlicedIPScheduler_v2):

    def __init__(self, compound_reservation_list, 
                 globally_possible_windows_dict, 
                 contractual_obligation_list, 
                 time_slicing_dict):
#        SlicedIPScheduler.__init__(self, compound_reservation_list, 
        SlicedIPScheduler_v2.__init__(self, compound_reservation_list, 
                                   globally_possible_windows_dict, 
                                   contractual_obligation_list, 
                                   time_slicing_dict)
        self.schedulerIDstring = 'SlicedIPSchedulerSparse'


    @timeit
    def schedule_all(self):
        if not self.reservation_list:
            return self.schedule_dict

        self.build_data_structures()
        # allocate A & b
        # find the row size of A:
        # first find the number of reservations participating in oneofs
        # oneof_reservation_num = 0
        # for c in self.oneof_constraints:
        #     oneof_reservation_num += len(c)
        # A_numrows = len(self.reservation_list) + len(self.aikt) + len(self.oneof_constraints) - oneof_reservation_num
        A_rows = []
        A_cols = []
#        A_data = []

        row = 0
        # constraint 5: oneof
        for c in self.oneof_constraints:
            for r in c:
                r.skip_constraint2 = True
                for entry in r.Yik_entries:
                    A_rows.append(row)
                    A_cols.append(entry)
#                    A_data.append(1)
            row += 1    

        # constraint 2: each res should have one start:
        # optimization: 
        # if the reservation participates in a oneof, then this is 
        # redundant with the oneof constraint added above, so don't add it.
        for r in self.reservation_list:
            if hasattr(r, 'skip_constraint2'):
                continue
            for entry in r.Yik_entries: #TODO: only necessary if > 1 entries
                A_rows.append(row)
                A_cols.append(entry)
#                A_data.append(1)
            row += 1

        # constraint 3: each slice should only have one sched. reservation:
        for s in self.aikt.keys():
            for entry in self.aikt[s]:
                A_rows.append(row)
                A_cols.append(entry)
#                A_data.append(1)
            row += 1      
        # sanity check: 
#        assert(A_numrows == row)
        A_numrows = row
#        A = cvxopt.spmatrix(A_data, A_rows, A_cols, (A_numrows, len(self.Yik)))
        A = cvxopt.spmatrix(1, A_rows, A_cols, (A_numrows, len(self.Yik)))
        b = cvxopt.matrix(1, (A_numrows, 1), 'd')

        # constraint 6: and       
        Aeq_numrows = 0
        Aeq_rows = []
        Aeq_cols = []
        Aeq_data = []
        if self.and_constraints:
            row = 0
            for c in self.and_constraints:
                constraint_size = len(c)
                Aeq_numrows += constraint_size-1
                left_idx = 0
                right_idx = 1
                while right_idx < constraint_size:
                    left_r = c[left_idx]
                    right_r = c[right_idx]
                    for entry in left_r.Yik_entries:
                        Aeq_rows.append(row)
                        Aeq_cols.append(entry)
                        Aeq_data.append(1)
                    for entry in right_r.Yik_entries:
                        Aeq_rows.append(row)
                        Aeq_cols.append(entry)
                        Aeq_data.append(-1)
                    left_idx += 1
                    right_idx += 1
                    row += 1
        Aeq = cvxopt.spmatrix(Aeq_data, Aeq_rows, Aeq_cols, (Aeq_numrows, len(self.Yik)))
        beq = cvxopt.matrix(0, (Aeq_numrows, 1), 'd')
            
        # objective function:
#        f = numpy.zeros(len(self.Yik)) #, dtype=numpy.int16)
        f = cvxopt.matrix(0, (len(self.Yik), 1), 'd')
        row = 0
        for entry in self.Yik:
#            f[row] = -entry[2] #priority
            f[row,0] = -entry[2] #priority
            row += 1
        # dump_matrix_sizes(f, A, Aeq, b, beq, 
        #                   len(self.compound_reservation_list))
# format for ilp
        # (status, x) = ilp(c, G, h, A, b, set(range(len(self.Yik))), set(range(len(self.Yik))))
# docs for options
# https://github.com/cvxopt/cvxopt/blob/master/src/C/glpk.c
        cvxopt.glpk.options['LPX_K_MSGLEV'] = 0

        (status, x) = ilp(f, A, b, Aeq, beq, set(range(len(self.Yik))), set(range(len(self.Yik))))
        if status == 'optimal':
            r = Result()
            r.xf = numpy.array(x).flatten()
            r.ff = f.T*x
            return self.unpack_result(r)
        else: 
            print "GLPK status: "+status
            return None


def dump_matrix_sizes(f, A, Aeq, b, beq, n_res):

    # Don't write the header if the file already exists
    import os.path
    path_to_file = 'matrix_sizes.dat'
    write_header = True
    if os.path.isfile(path_to_file):
        write_header = False

    from datetime import datetime
    date_time_fmt = '%Y-%m-%d %H:%M:%S'
    now = datetime.utcnow()
    now_str = now.strftime(date_time_fmt)

    out_fh  = open(path_to_file, 'a')
    fmt_str = "%-6s %-16s %-16s %-16s %-16s %-16s %-16s %-16s %-16s %-16s %-16s %-13s\n"

    out_hdr = fmt_str % ('N_res',
                         'F_shape',  'F_size',
                         'A_shape',  'A_size',
                         'b_shape',  'b_size',
                         'lb_shape', 'lb_size',
                         'ub_shape', 'ub_size',
                         'Ran_at')
    out_str = fmt_str % (n_res,
                         f.shape,  m_size(f),
                         A.shape,  sm_size(A),
                         b.shape,  m_size(b),
                         lb.shape, m_size(lb),
                         ub.shape, m_size(ub),
                         now_str)
    if write_header:
        out_fh.write(out_hdr)
    out_fh.write(out_str)

    out_fh.close()

def m_size(m):
    return m.nbytes * m.dtype.itemsize

def sm_size(m):
    return m.getnnz() * m.dtype.itemsize

def print_matrix_size(matrix):
    print "Matrix shape:", matrix.shape
    print "Matrix size (bytes):", matrix.nbytes * matrix.dtype.itemsize
    print "Matrix type:", matrix.dtype

def print_sparse_matrix_size(matrix):
    print "Matrix shape:", matrix.shape
    print "Matrix size (bytes):", matrix.getnnz() * matrix.dtype.itemsize
    print "Matrix type:", matrix.dtype
