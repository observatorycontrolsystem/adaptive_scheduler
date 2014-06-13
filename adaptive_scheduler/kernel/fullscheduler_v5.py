#!/usr/bin/env python

'''
FullScheduler_v5 class for co-scheduling reservations 
across multiple resources using time-slicing and an integer program.

Because time is discretized into time slices, this scheduler requires
information about how to generate the slices, so its signature has one
more argument than usual. 

This implementation uses a SPARSE matrix representation. 

Author: Sotiria Lampoudi (slampoud@gmail.com)
Sept 2012
Dec 2012: changed to work with Reservation_v3
'''

from reservation_v3 import *
#from contracts_v2 import *
import copy
import numpy
from openopt import LP
from scipy.sparse import coo_matrix
#from slicedipscheduler import *
from slicedipscheduler_v2 import *
from adaptive_scheduler.utils import timeit

#class FullScheduler_v5(SlicedIPScheduler):
class FullScheduler_v5(SlicedIPScheduler_v2):

    def __init__(self, compound_reservation_list, 
                 globally_possible_windows_dict, 
                 contractual_obligation_list, 
                 slice_size_seconds):
#        SlicedIPScheduler.__init__(self, compound_reservation_list, 
        SlicedIPScheduler_v2.__init__(self, compound_reservation_list, 
                                   globally_possible_windows_dict, 
                                   contractual_obligation_list, 
                                   slice_size_seconds)
        self.schedulerIDstring = 'SlicedIPSchedulerSparse'


    @timeit
    def schedule_all(self, timelimit=None):
        if not self.reservation_list:
            return self.schedule_dict

        self.build_data_structures()
        # allocate A & b
        # find the row size of A:
        # first find the number of reservations participating in oneofs
        oneof_reservation_num = 0
        for c in self.oneof_constraints:
            oneof_reservation_num += len(c)
        A_numrows = len(self.reservation_list) + len(self.aikt) + len(self.oneof_constraints) - oneof_reservation_num
        A_rows = []
        A_cols = []
        A_data = []
#        try:
#            A = numpy.zeros((A_numrows, len(self.Yik)), dtype=numpy.int)
#        except ValueError:
#            print "Number of A rows: ", A_numrows
        b = numpy.zeros(A_numrows, dtype=numpy.int16)
        # build A & b
        row = 0

        # constraint 5: oneof
        for c in self.oneof_constraints:
            for r in c:
                for entry in r.Yik_entries:
                    A_rows.append(row)
                    A_cols.append(entry)
                    A_data.append(1)
#                    A[row,entry] = 1
                r.skip_constraint2 = True
            b[row] = 1
            row += 1    

        # constraint 2: each res should have one start:
        # optimization: 
        # if the reservation participates in a oneof, then this is 
        # redundant with the oneof constraint added above, so don't add it.
        for r in self.reservation_list:
            if hasattr(r, 'skip_constraint2'):
                continue
            for entry in r.Yik_entries:
                A_rows.append(row)
                A_cols.append(entry)
                A_data.append(1)
                #A[row,entry] = 1
            b[row] = 1
            row += 1

        # constraint 3: each slice should only have one sched. reservation:
        for s in self.aikt.keys():
            for entry in self.aikt[s]:
                A_rows.append(row)
                A_cols.append(entry)
                A_data.append(1)
#                A[row,entry] = 1
            b[row] = 1
            row += 1
        
        A = coo_matrix((A_data, (A_rows, A_cols)), shape=(A_numrows, len(self.Yik)))
        # constraint 6: and       
        # figure out size of constraint matrix

        if not self.and_constraints:
            Aeq = []
            beq = []
        else:
            Aeq_numrows = 0
            for c in self.and_constraints:
                Aeq_numrows += len(c)-1
        # allocate Aeq and beq
#        Aeq = numpy.zeros((Aeq_numrows, len(self.Yik)), dtype=numpy.int)
            Aeq_rows = []
            Aeq_cols = []
            Aeq_data = []
            beq = numpy.zeros(Aeq_numrows, dtype=numpy.int16)
            row = 0
            for c in self.and_constraints:
                constraint_size = len(c)
                left_idx = 0
                right_idx = 1
                while right_idx < constraint_size:
                    left_r = c[left_idx]
                    right_r = c[right_idx]
                    for entry in left_r.Yik_entries:
#                    Aeq[row, entry] = 1
                        Aeq_rows.append(row)
                        Aeq_cols.append(entry)
                        Aeq_data.append(1)
                    for entry in right_r.Yik_entries:
                        Aeq_rows.append(row)
                        Aeq_cols.append(entry)
                        Aeq_data.append(-1)
#                    Aeq[row, entry] = -1
                    left_idx += 1
                    right_idx += 1
                    row += 1
#            print Aeq_numrows
            Aeq = coo_matrix((Aeq_data, (Aeq_rows, Aeq_cols)), shape=(Aeq_numrows, len(self.Yik)))

        # bounds:
        lb = numpy.zeros(len(self.Yik), dtype=numpy.int16)
        ub = numpy.ones(len(self.Yik), dtype=numpy.int16)

        # objective function:
        f = numpy.zeros(len(self.Yik))
        f = numpy.zeros(len(self.Yik), dtype=numpy.int16)
        row = 0
        for entry in self.Yik:
            f[row] = entry[2] #priority
            row += 1

        dump_matrix_sizes(f, A, Aeq, b, beq, lb, ub,
                          len(self.compound_reservation_list))
        p = LP(f=f, A=A, Aeq=Aeq, b=b, beq=beq, lb=lb, ub=ub)
#        r = p.minimize('pclp') 
        r = p.maximize('glpk', iprint=-1)
#        r = p.minimize('lpsolve')
        return self.unpack_result(r)


def dump_matrix_sizes(f, A, Aeq, b, beq, lb, ub, n_res):

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
