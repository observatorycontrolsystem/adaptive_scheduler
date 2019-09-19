#!/usr/bin/env python

'''
FullScheduler_gurobi class for co-scheduling reservations 
across multiple resources using time-slicing and an integer program.

Because time is discretized into time slices, this scheduler requires
information about how to generate the slices, so its signature has one
more argument than usual. 

This implementation uses a SPARSE matrix representation and direct binding
to the Gurobi solver.

Author: Jason Eastman (jeastman@lcogt.net)
January 2014
'''

from reservation_v3 import *
#from contracts_v2 import *
import copy
from slicedipscheduler_v2 import SlicedIPScheduler_v2
from adaptive_scheduler.utils import timeit, metric_timer

from rise_set.astrometry import calc_local_hour_angle, calculate_altitude
from gurobipy import Model, tuplelist, GRB, quicksum, GurobiError
from time import sleep

import logging
log = logging.getLogger(__name__)


class Result(object):
    pass

class FullScheduler_gurobi(SlicedIPScheduler_v2):
    @metric_timer('kernel.init')
    def __init__(self, compound_reservation_list, 
                 globally_possible_windows_dict, 
                 contractual_obligation_list, 
                 slice_size_seconds):
        SlicedIPScheduler_v2.__init__(self, compound_reservation_list, 
                                   globally_possible_windows_dict, 
                                   contractual_obligation_list, 
                                   slice_size_seconds)
        self.schedulerIDstring = 'SlicedIPSchedulerSparse'

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
                  self.time_slicing_dict[resource][1] -                      \
                  reservation.possible_starts[winidx].first_slice_start

        return self.possible_starts[winidx]

        return 0.0
	
    # A stub to optimize requests by airmass
    def weight_by_airmass(self):
        return

        for request in self.Yik:
            ra,dec=self.get_target_coords_by_reqID(request[0])
            lat,lon = self.get_earth_coords_by_resource(request[3])
            utc = self.get_utc_by_winidx(request[1])

            local_hour_angle = calc_local_hour_angle(ra, lon, utc)
            alt = calculate_altitude(lat,dec,local_hour_angle)
            airmass = 1.0/cos(pi/2.0-alt)

            # map the airmass to a minimal weighting function
            maxairmass = 3
            minairmass = 1
            maxweight = 0.05
            minweight = -0.05
            slope = (maxweight-minweight)/(minairmass-maxairmass)
            intercept = maxweight - slope*minairmass

            weight = airmass*slope + intercept
            request[2] = request[2] + weight

    @timeit
    @metric_timer('kernel.scheduling')
    def schedule_all(self, timelimit=None):

        if not self.reservation_list:
            return self.schedule_dict

        # populate all the windows, requests, etc
        self.build_data_structures()

        # weight the priorities in each timeslice by airmass
        self.weight_by_airmass()

        result = None
        while result is None:
            try:
                result = self._gurobi_schedule(timelimit)
            except GurobiError as ge:
                log.warn("Gurobi license not available, sleeping 10 seconds.")
                sleep(10.0)

        return result

    def _gurobi_schedule(self, timelimit=None):
        # Instantiate a Gurobi Model object
        m = Model("LCOGT Schedule")

        # Constraint: Decision variable (isScheduled) must be binary (eq 4)
        requestLocations=tuplelist()
        scheduled_vars = []
        for r in self.Yik:
            # convert self.Yik to a tuplelist for optimized searches
            # [(reqID, window idx, priority, resource, isScheduled)]
            var = m.addVar(vtype=GRB.BINARY,name=str(r[0]))
            scheduled_vars.append(var)
            requestLocations.append((r[0], r[1], r[2], r[3], var))

        # update the Gurobi model to use isScheduled variables in constraints
        m.update()

        for i, r in enumerate(self.Yik):
            scheduled_vars[i].start = r[4]

        m.update()

        # Constraint: One-of (eq 5)
        i=0
        for oneof in self.oneof_constraints:
            match = tuplelist()
            for r in oneof:
                reqid = r.get_ID()
                match += requestLocations.select(reqid,'*','*','*','*')
                r.skip_constraint2=True # does this do what I think it does?
            nscheduled = quicksum(isScheduled for reqid,winidx,priority,resource,isScheduled in match)
            m.addConstr(nscheduled <= 1, "oneof_constraint_" + str(i))
            i=i+1

        # Constraint: And (all or nothing) (eq 6)
        i=0
        andtuple = tuplelist()
        for andconstraint in self.and_constraints:
            # add decision variable that must be equal to all "and"ed blocks
            andVar = m.addVar(vtype=GRB.BINARY,name="and_var_"+str(i))
            m.update()
            j=0
            for r in andconstraint:
                reqid = r.get_ID()
                match = requestLocations.select(reqid,'*','*','*','*')
                nscheduled = quicksum(isScheduled for reqid,winidx,priority,resource,isScheduled in match)
                m.addConstr(andVar == nscheduled,"and_constraint_"+str(i)+"_"+str(j))
                j=j+1
            i=i+1

        # Constraint: No more than one request should be scheduled in each (timeslice, resource) (eq 3)
        # self.aikt.keys() indexes the requests that occupy each (timeslice, resource)
#        for s in self.aikt: # faster??
        for s in self.aikt.keys():
            match = tuplelist()
            for timeslice in self.aikt[s]:
                match.append(requestLocations[timeslice])
            nscheduled = quicksum(isScheduled for reqid,winidx,priority,resource,isScheduled in match)
            m.addConstr(nscheduled <= 1,'one_per_slice_constraint_' + s)

        # Constraint: No request should be scheduled more than once (eq 2)
        # skip if One-of (redundant)
        for r in self.reservation_list:
            if not hasattr(r,'skip_constraint2'):
                reqid = r.get_ID()
                match = requestLocations.select(reqid,'*','*','*','*')
                nscheduled = quicksum(isScheduled for reqid,winidx,priority,resource,isScheduled in match)
                m.addConstr(nscheduled <= 1,'one_per_reqid_constraint_' + str(reqid))

        # Objective: Maximize the merit functions of all scheduled requests (eq 1);
        objective = quicksum([isScheduled*(priority + 0.1/(winidx+1.0)) for req,winidx,priority,resource,isScheduled in requestLocations])

        # set the objective, and maximize it
        m.setObjective(objective)
        m.modelSense = GRB.MAXIMIZE

        # impose a time limit on the solve
        if timelimit > 0:
            m.params.timeLimit=timelimit
        # Set the tolerance for the model solution to be within 1% of what it thinks is the best solution
        m.params.MIPGap = 0.01

        # Set the Method of solving the root relaxation of the MIPs model to concurrent (default is dual simplex)
        m.params.Method = 3

        # add all the constraints to the model
        m.update()

        # Solve the model
        m.optimize()

        # Return the optimally-scheduled windows
        r = Result()
        r.xf = []
        for request, winidx,priority,resource, isScheduled in requestLocations: r.xf.append(isScheduled.x)
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
