#!/usr/bin/env python
'''
FullScheduler_ortoolkit class for co-scheduling reservations
across multiple resources using time-slicing and an integer program.

Because time is discretized into time slices, this scheduler requires
information about how to generate the slices, so its signature has one
more argument than usual.

This implementation uses a SPARSE matrix representation and the ortoolkit solver
which can be configured to use Gurobi, GLPK, or CBC algorithms.

Author: Jason Eastman (jeastman@lcogt.net)
January 2014
'''

from adaptive_scheduler.kernel.slicedipscheduler_v2 import SlicedIPScheduler_v2
from adaptive_scheduler.utils import timeit, metric_timer

from ortools.linear_solver import pywraplp

from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


ALGORITHMS = {
    'CBC': pywraplp.Solver.CBC_MIXED_INTEGER_PROGRAMMING,
    'GUROBI': pywraplp.Solver.GUROBI_MIXED_INTEGER_PROGRAMMING,
    'GLPK': pywraplp.Solver.GLPK_MIXED_INTEGER_PROGRAMMING
}

class Result(object):
    pass


class FullScheduler_ortoolkit(SlicedIPScheduler_v2):
    """ Performs scheduling using an algorithm from ORToolkit
    """
    @metric_timer('kernel.init')
    def __init__(self, kernel, compound_reservation_list,
                 globally_possible_windows_dict,
                 contractual_obligation_list,
                 slice_size_seconds, mip_gap):
        super().__init__(compound_reservation_list,
                         globally_possible_windows_dict,
                         contractual_obligation_list,
                         slice_size_seconds)
        self.schedulerIDstring = 'SlicedIPSchedulerSparse'
        self.kernel = kernel
        self.mip_gap = mip_gap
        self.algorithm = ALGORITHMS[kernel.upper()]

    # A stub to get the RA/dec by request ID
    # (REQUIRED FOR AIRMASS OPTIMIZATION)
    def get_target_coords_by_reqID(self, reqID):
        return 0.0, 0.0

    # A stub to get the lat/lon by resource
    # (REQUIRED FOR AIRMASS OPTIMIZATION)
    def get_earth_coords_by_resource(self, resource):
        return 0.0, 0.0

    # A stub to translate winidx to UTC date/time
    # (REQUIRED FOR AIRMASS OPTIMIZATION)
    def get_utc_by_winidx(self, winidx):
        reqID = 0
        reservation = get_reservation_by_ID(reqID)
        start = reservation.possible_starts[winidx].internal_start
        quantum = reservation.possible_starts[winidx].all_slice_starts[-1] + \
                  self.time_slicing_dict[resource][1] - \
                  reservation.possible_starts[winidx].first_slice_start

        return self.possible_starts[winidx]

    # A stub to optimize requests by airmass
    def weight_by_airmass(self):
        return
        # for request in self.Yik:
        #     ra, dec = self.get_target_coords_by_reqID(request[0])
        #     lat, lon = self.get_earth_coords_by_resource(request[3])
        #     utc = self.get_utc_by_winidx(request[1])
        #
        #     local_hour_angle = calc_local_hour_angle(ra, lon, utc)
        #     alt = calculate_altitude(lat, dec, local_hour_angle)
        #     airmass = 1.0 / cos(pi / 2.0 - alt)
        #
        #     # map the airmass to a minimal weighting function
        #     maxairmass = 3
        #     minairmass = 1
        #     maxweight = 0.05
        #     minweight = -0.05
        #     slope = (maxweight - minweight) / (minairmass - maxairmass)
        #     intercept = maxweight - slope * minairmass
        #
        #     weight = airmass * slope + intercept
        #     request[2] = request[2] + weight

    @timeit
    @metric_timer('kernel.scheduling')
    def schedule_all(self, timelimit=0):

        if not self.reservation_list:
            return self.schedule_dict

        # populate all the windows, requests, etc
        self.build_data_structures()

        # weight the priorities in each timeslice by airmass
        self.weight_by_airmass()

        # Instantiate a Gurobi Model object
        solver = pywraplp.Solver('adaptive_scheduler', self.algorithm)

        # Constraint: Decision variable (isScheduled) must be binary (eq 4)
        requestLocations = []
        vars_by_req_id = defaultdict(list)
        scheduled_vars = []
        solution_hints = []
        for r in self.Yik:
            # create a lut of req_id to decision vars for building the constraints later
            # [(reqID, window idx, priority, resource, isScheduled)]
            var = solver.BoolVar(name=f"bool_var_{r[0]}_{len(scheduled_vars)}")
            scheduled_vars.append(var)
            vars_by_req_id[r[0]].append((r[0], r[1], r[2], r[3], var))
            requestLocations.append((r[0], r[1], r[2], r[3], var))
            solution_hints.append(r[4])

        # The warm-start hints (not supported in older ortools)
        # solver.SetHint(variables=scheduled_vars, values=solution_hints)

        # Constraint: One-of (eq 5)
        i = 0
        for oneof in self.oneof_constraints:
            match = []
            for r in oneof:
                reqid = r.get_ID()
                match.extend(vars_by_req_id[reqid])
                r.skip_constraint2 = True  # does this do what I think it does?
            nscheduled_one = solver.Sum([isScheduled for reqid, winidx, priority, resource, isScheduled in match])
            solver.Add(nscheduled_one <= 1, 'oneof_constraint_' + str(i))
            i = i + 1

        # Constraint: And (all or nothing) (eq 6)
        i = 0
        for andconstraint in self.and_constraints:
            # add decision variable that must be equal to all "and"ed blocks
            andVar = solver.BoolVar(name=f"and_var_{str(i)}")
            j = 0
            for r in andconstraint:
                reqid = r.get_ID()
                match = vars_by_req_id[reqid]
                nscheduled_and = solver.Sum([isScheduled for reqid, winidx, priority, resource, isScheduled in match])
                solver.Add(andVar == nscheduled_and, 'and_constraint_' + str(i) + "_" + str(j))
                j = j + 1
            i = i + 1

        # Constraint: No more than one request should be scheduled in each (timeslice, resource) (eq 3)
        # self.aikt.keys() indexes the requests that occupy each (timeslice, resource)
        for s in sorted(self.aikt.keys()):
            match = []
            for timeslice in self.aikt[s]:
                match.append(requestLocations[timeslice])
            nscheduled1 = solver.Sum([isScheduled for reqid, winidx, priority, resource, isScheduled in match])
            solver.Add(nscheduled1 <= 1, 'one_per_slice_constraint_' + s)

        # Constraint: No request should be scheduled more than once (eq 2)
        # skip if One-of (redundant)
        for r in self.reservation_list:
            if not hasattr(r, 'skip_constraint2'):
                reqid = r.get_ID()
                match = vars_by_req_id[reqid]
                nscheduled2 = solver.Sum([isScheduled for reqid, winidx, priority, resource, isScheduled in match])
                solver.Add(nscheduled2 <= 1, 'one_per_reqid_constraint_' + str(reqid))

        # Objective: Maximize the merit functions of all scheduled requests (eq 1);
        objective = solver.Maximize(solver.Sum(
            [isScheduled * (priority + (0.1 / (winidx + 1.0))) for req, winidx, priority, resource, isScheduled in
             requestLocations]))

        # impose a time limit (ms) on the solve
        if timelimit > 0:
            solver.SetTimeLimit(int(timelimit * 1000))

        params = pywraplp.MPSolverParameters()
        # Set the tolerance for the model solution to be within 1% of what it thinks is the best solution
        params.SetDoubleParam(pywraplp.MPSolverParameters.RELATIVE_MIP_GAP, self.mip_gap)

        # Solve the model
        solver.EnableOutput()
        solver.Solve(params)
        logger.warn("Finished solving schedule")

        # Return the optimally-scheduled windows
        r = Result()
        r.xf = []
        for request, winidx, priority, resource, isScheduled in requestLocations:
            r.xf.append(isScheduled.SolutionValue())
        logger.warn("Set SolutionValues of isScheduled")

        return self.unpack_result(r)


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

    out_fh = open(path_to_file, 'a')
    fmt_str = "%-6s %-16s %-16s %-16s %-16s %-16s %-16s %-16s %-16s %-16s %-16s %-13s\n"

    out_hdr = fmt_str % ('N_res',
                         'F_shape', 'F_size',
                         'A_shape', 'A_size',
                         'b_shape', 'b_size',
                         'lb_shape', 'lb_size',
                         'ub_shape', 'ub_size',
                         'Ran_at')
    out_str = fmt_str % (n_res,
                         f.shape, m_size(f),
                         A.shape, sm_size(A),
                         b.shape, m_size(b),
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
    print("Matrix shape: {}".format(matrix.shape))
    print("Matrix size (bytes): {}".format(matrix.nbytes * matrix.dtype.itemsize))
    print("Matrix type: {}".format(matrix.dtype))


def print_sparse_matrix_size(matrix):
    print("Matrix shape: {}".format(matrix.shape))
    print("Matrix size (bytes): {}".format(matrix.getnnz() * matrix.dtype.itemsize))
    print("Matrix type: {}".format(matrix.dtype))
