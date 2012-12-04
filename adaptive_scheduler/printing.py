'''
printing.py - Functions to pretty-print objects.

TODO: These should be folded into the __str__ or __repr__ methods of the actual
objects, but are here for now to preserve those object's APIs.

Author: Eric Saunders
December 2011
'''

from adaptive_scheduler.utils import datetime_to_normalised_epoch

INDENT = "    "

def print_reservation(res):
    print res

    return


def print_compound_reservation(compound_res):
    print "CompoundReservation (%s):" % ( compound_res.type )

    plural = ''
    if compound_res.size > 1:
        plural = 's'

    print INDENT + "made of %d Reservation%s:" % (compound_res.size, plural)
    for res in compound_res.reservation_list:
        print_reservation(res)

    return


def print_request(req, resource_name):
    target_name = getattr(req.target, 'name', 'no name')
    print "REQUEST: target %s, observed from %s" % (
                                                      target_name,
                                                      resource_name,
                                                    )




def print_req_summary(req, resource_name, rs_dark_intervals, rs_up_intervals, intersection):
    print_request(req, resource_name)

    for interval in rs_dark_intervals:
        print "Darkness from %s to %s" % (interval[0], interval[1])
    for interval in rs_up_intervals:
        print "Target above horizon from %s to %s" % (interval[0], interval[1])

    print "Calculated intersections are:"

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
