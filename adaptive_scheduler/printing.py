'''
printing.py - Functions to pretty-print objects.

TODO: These should be folded into the __str__ or __repr__ methods of the actual
objects, but are here for now to preserve those object's APIs.

Author: Eric Saunders
December 2011
'''

from adaptive_scheduler.utils import datetime_to_normalised_epoch
from adaptive_scheduler.log   import UserRequestLogger
from datetime import timedelta

INDENT = "    "

# Set up and configure a module scope logger
import logging
log = logging.getLogger(__name__)


multi_ur_log = logging.getLogger('ur_logger')
ur_log = UserRequestLogger(multi_ur_log)


def summarise_urs(user_reqs, log_msg):
    log.debug("User Request breakdown:")
    for ur in user_reqs:
        r_nums  = [r.request_number for r in ur.requests]
        w_total = sum([r.n_windows() for r in ur.requests])
        _, w_str = pluralise(w_total, 'Window')
        r_total, r_str = pluralise(len(ur.requests), 'Request')
        r_states = [r.state for r in ur.requests]

        sum_str = ' %s: %s (%d %s, %d %s) %s'
        log.debug(sum_str, ur.tracking_number, r_nums,
                  r_total, r_str, w_total, w_str, r_states)
        msg = log_msg + sum_str
        ur_log.info(msg % (ur.tracking_number, r_nums,
                     r_total, r_str, w_total, w_str, r_states), ur.tracking_number)

    return


def log_windows(ur, log_msg):
    for r in ur.requests:
        for w in r.windows:
            ur_log.info("%s %s" % (log_msg, w), ur.tracking_number)


def log_full_ur(ur, now):
    ur_log.info("Expires = %s" % ur.expires, ur.tracking_number)
    ur_log.info("IPP Value = %s" % ur.ipp_value, ur.tracking_number)
    ur_log.info("Base Priority = %s" % ur.get_base_priority(), ur.tracking_number)
    ur_log.info("Effective Priority = %s" % ur.priority, ur.tracking_number)
    ur_log.info("Operator = %s" % ur.operator, ur.tracking_number)
    ur_log.info("Scheduling horizon = %s" % ur.scheduling_horizon(now), ur.tracking_number)

    for r in ur.requests:
        ur_log.info("Request %s: duration = %ss" % (r.request_number, r.duration),
                    ur.tracking_number)
        ur_log.info("Request %s: target = %s" % (r.request_number, r.target),
                    ur.tracking_number)
        ur_log.info("Request %s: constraints = %s" % (r.request_number, r.constraints),
                    ur.tracking_number)



def print_reservation(res):
    log.debug(res)
    for resource, interval in res.possible_windows_dict.iteritems():
        log.debug("Possible windows: %s -> %s" % ( resource, interval ))

    return


def plural_str(n, string):
    n, string = pluralise(n, string)
    return "%d %s" % (n, string)


def pluralise(n, string):
    if n != 1:
        string += 's'

    return n, string


def print_compound_reservation(compound_res):
    log.debug("CompoundReservation (%s):", compound_res.type)

    log.debug(INDENT + "made of %d %s:", *pluralise(compound_res.size, 'Reservation'))
    for res in compound_res.reservation_list:
        print_reservation(res)

    return


def print_request(req, resource_name):
    target_name = getattr(req.target, 'name', 'no name')
    log.debug("Request %s: Target %s, observed from %s",req.request_number,
                                                        target_name,
                                                        resource_name)




def print_req_summary(req, resource_name, user_intervals, rs_dark_intervals,
                      rs_up_intervals, intersection):
    print_request(req, resource_name)
    # Pull out the timepoint list for printing
    u_int = list(user_intervals.timepoints)

    if u_int:
        earliest_tp = latest_tp = u_int[0]

    else:
        log.debug("No user intervals found")
        return

    while u_int:
        start = u_int.pop(0)
        end   = u_int.pop(0)
        log.debug("    User window:          %s to %s", start.time, end.time)

        for tp in start, end:
            if tp.time < earliest_tp.time: earliest_tp = tp
            if tp.time > latest_tp.time: latest_tp = tp

    for dark_int in rs_dark_intervals:
        if dark_int[0] < latest_tp.time+timedelta(days=1) and dark_int[1] > earliest_tp.time - timedelta(days=1):
            log.debug("    Darkness:             %s to %s", dark_int[0], dark_int[1])

    for up_int in rs_up_intervals:
        if up_int[0] < latest_tp.time+timedelta(days=1) and up_int[1] > earliest_tp.time - timedelta(days=1):
            log.debug("    Target above horizon: %s to %s", up_int[0], up_int[1])

    log.debug("    Dark/rise intersections:")
    if not intersection.timepoints:
        log.debug("        <none>")
    else:
        for i in intersection.timepoints:
            log.debug("        %s (%s)", i.time, i.type)

    return


def print_resource_windows(resource_windows):
    for resource in resource_windows:
        print resource
        for i in resource_windows[resource].timepoints:
            print i.time, i.type

    return


def print_compound_reservations(to_schedule):
    log.info("Finished constructing compound reservations...")
    log.info("There are %d %s to schedule.", *pluralise(len(to_schedule), 'CompoundReservation'))
    for compound_res in to_schedule:
        print_compound_reservation(compound_res)

    return


def print_schedule(schedule, semester_start=None, semester_end=None):

    if semester_start and semester_end:
        epoch_start = datetime_to_normalised_epoch(semester_start, semester_start)
        epoch_end   = datetime_to_normalised_epoch(semester_end, semester_start)

        log.info("Scheduling for semester %s to %s" % (semester_start, semester_end))
        log.info("Scheduling for normalised epoch %s to %s" % (epoch_start, epoch_end))

    for resource_reservations in schedule.values():
        for res in resource_reservations:
            print_reservation(res)

    return

def iprint(string, indent_level=0):
    print (indent_level * INDENT) + string
