'''
printing.py - Functions to pretty-print objects.

TODO: These should be folded into the __str__ or __repr__ methods of the actual
objects, but are here for now to preserve those object's APIs.

Author: Eric Saunders
December 2011
'''

INDENT = "    "

def print_reservation(res):
    print "        %s on %s (%d)" % ( 'TODO:NAME',
                                       res.resource,
                                       res.duration, )
    iprint("Windows in this reservation:", 2)
    x = 0
    while x < len(res.possible_windows.timepoints):
        print "            %s <-> %s" % ( res.possible_windows.timepoints[x+0].time,
                                          res.possible_windows.timepoints[x+1].time )
        x += 2
#    print("Possible windows:", res.possible_windows)


def print_compound_reservation(compound_res):
    print "CompoundReservation (%s):" % ( compound_res.type )

    plural = ''
    if compound_res.size > 1:
        plural = 's'

    print INDENT + "made of %d Reservation%s:" % (compound_res.size, plural)
    for res in compound_res.reservation_list:
        print_reservation(res)


def print_request(req):
    print "REQUEST: target %s (%s), observed from %s, between %s and %s" % (
                                                              req.target.name,
                                                              req.res_type,
                                                              req.telescope.name,
                                                              req.windows[0],
                                                              req.windows[1])



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
