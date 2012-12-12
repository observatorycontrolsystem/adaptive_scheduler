#!/usr/bin/env python

from adaptive_scheduler.kernel.reservation_v3 import Reservation_v3 as Reservation
from adaptive_scheduler.kernel.intervals import Intervals
from adaptive_scheduler.kernel.timepoint import Timepoint

window_dict = {
        '1m0a.domb.lsc' : Intervals([Timepoint(1225800, 'start'),
                                    Timepoint(1233000, 'end')])
        }

r = Reservation(1, 7380, window_dict)
print "pos. windows = '%s'" % r.possible_windows_dict['1m0a.domb.lsc']
print "free. windows = '%s'" % r.free_windows_dict['1m0a.domb.lsc']
for x,y in r.__dict__.iteritems():
    print x, y
