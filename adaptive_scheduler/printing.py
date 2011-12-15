'''
printing.py - Functions to pretty-print objects.

TODO: These should be folded into the __str__ or __repr__ methods of the actual
objects, but are here for now to preserve those object's APIs.

Author: Eric Saunders
December 2011
'''

INDENT = "    "

def print_reservation(res):
    print res


def print_compound_reservation(compound_res):
    print "CompoundReservation (%s):" % ( compound_res.type )

    plural = ''
    if compound_res.size > 1:
        plural = 's'

    print INDENT + "made of %d Reservation%s:" % (compound_res.size, plural)
    for res in compound_res.reservation_list:
        print_reservation(res)


def print_request(req):
    print "REQUEST: target %s, observed from %s" % (
                                                          req.target.name,
                                                          req.telescope.name,
                                                          )



def print_req_summary(req, rs_dark_intervals, rs_up_intervals, intersection):
    print_request(req)

    for interval in rs_dark_intervals:
        print "Darkness from %s to %s" % (interval[0], interval[1])
    for interval in rs_up_intervals:
        print "Target above horizon from %s to %s" % (interval[0], interval[1])

    print "Calculated intersections are:"

    for i in intersection.timepoints:
        print "    %s (%s)" % (i.time, i.type)



def iprint(string, indent_level=0):
    print (indent_level * INDENT) + string
