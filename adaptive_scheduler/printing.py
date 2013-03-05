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

def print_reservation(res):
    print res
    for resource, interval in res.possible_windows_dict.iteritems():
        print "Possible windows: %s -> %s" % ( resource, interval )

    return


def pluralise(n, string):
    if n != 1:
        string += 's'

    return string

def print_compound_reservation(compound_res):
    print "CompoundReservation (%s):" % ( compound_res.type )

    res_string = pluralise(compound_res.size, 'Reservation')

    print INDENT + "made of %d %s:" % (compound_res.size, res_string)
    for res in compound_res.reservation_list:
        print_reservation(res)

    return


def print_request(req, resource_name):
    target_name = getattr(req.target, 'name', 'no name')
    print "REQUEST: target %s, observed from %s" % (
                                                      target_name,
                                                      resource_name,
                                                    )




def print_req_summary(req, resource_name, user_intervals, rs_dark_intervals,
                      rs_up_intervals, intersection):
    print_request(req, resource_name)
    # Pull out the timepoint list for printing
    u_int = list(user_intervals.timepoints)

    if u_int:
        earliest_tp = latest_tp = u_int[0]

    while u_int:
        start = u_int.pop(0)
        end   = u_int.pop(0)
        print "User window from          %s to %s" % (start.time, end.time)

        for tp in start, end:
            if tp.time < earliest_tp.time: earliest_tp = tp
            if tp.time > latest_tp.time: latest_tp = tp

    for dark_int, up_int in zip(rs_dark_intervals, rs_up_intervals):
        if dark_int[0] < latest_tp.time+timedelta(days=1) and dark_int[1] > earliest_tp.time - timedelta(days=1):
            print "Darkness from             %s to %s" % (dark_int[0], dark_int[1])
        if up_int[0] < latest_tp.time and up_int[1] > earliest_tp.time:
            print "Target above horizon from %s to %s" % (up_int[0], up_int[1])

    print "Request %s" % req.request_number
    print "    On %s, dark and rise_set intersections are:" % resource_name
    if not intersection.timepoints:
        print "    <none>"
    else:
        for i in intersection.timepoints:
            print "    %s (%s)" % (i.time, i.type)

    return


def print_resource_windows(resource_windows):
    for resource in resource_windows:
        print resource
        for i in resource_windows[resource].timepoints:
            print i.time, i.type

    return


def print_compound_reservations(to_schedule):
    print "Finished constructing compound reservations..."
    print "There are %d CompoundReservations to schedule:" % (len(to_schedule))
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
