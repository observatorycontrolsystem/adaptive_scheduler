'''
printing.py - Functions to pretty-print objects.

TODO: These should be folded into the __str__ or __repr__ methods of the actual
objects, but are here for now to preserve those object's APIs.

Author: Eric Saunders
December 2011
'''

from adaptive_scheduler.utils import datetime_to_normalised_epoch
from datetime import timedelta

INDENT = "    "

# Set up and configure a module scope logger
import logging
log = logging.getLogger(__name__)

def print_reservation(res):
    print res
    for resource, interval in res.possible_windows_dict.iteritems():
        print "Possible windows: %s -> %s" % ( resource, interval )

    return


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

    for dark_int, up_int in zip(rs_dark_intervals, rs_up_intervals):
        if dark_int[0] < latest_tp.time+timedelta(days=1) and dark_int[1] > earliest_tp.time - timedelta(days=1):
            log.debug("    Darkness:             %s to %s", dark_int[0], dark_int[1])
        if up_int[0] < latest_tp.time and up_int[1] > earliest_tp.time:
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

    print "Scheduling completed. Final schedule:"
    if semester_start and semester_end:
        epoch_start = datetime_to_normalised_epoch(semester_start, semester_start)
        epoch_end   = datetime_to_normalised_epoch(semester_end, semester_start)

        print "Scheduling for semester %s to %s" % (semester_start, semester_end)
        print "Scheduling for normalised epoch %s to %s" % (epoch_start, epoch_end)

    for resource_reservations in schedule.values():
        for res in resource_reservations:
            print_reservation(res)

    return

def iprint(string, indent_level=0):
    print (indent_level * INDENT) + string
