#!/usr/bin/env python

from adaptive_scheduler.kernel.reservation_v3 import Reservation_v3 as Reservation
from adaptive_scheduler.kernel.reservation_v3 import CompoundReservation_v2 as CompoundReservation
from adaptive_scheduler.kernel.intervals import Intervals
from adaptive_scheduler.kernel.timepoint import Timepoint
from adaptive_scheduler.kernel.fullscheduler_v5 import FullScheduler_v5 as FullScheduler
from adaptive_scheduler.printing import print_schedule

window_dict = {
        '1m0a.domb.lsc' : Intervals([Timepoint(1225800, 'start'),
                                    Timepoint(1233000, 'end')])
        }

r = Reservation(1, 200, window_dict)
print "pos. windows = '%s'" % r.possible_windows_dict['1m0a.domb.lsc']
print "free. windows = '%s'" % r.free_windows_dict['1m0a.domb.lsc']
for x,y in r.__dict__.iteritems():
    print x, y

cr = CompoundReservation([r])

to_schedule = [cr]
resource_windows = {}
resource_windows['1m0a.domb.lsc'] = Intervals([Timepoint(1, 'start'),
                                               Timepoint(2000000, 'end')],
                                              'free')
contractual_obs = []
time_slicing_dict = {
                        '0m4a.aqwa.bpl' : [0, 600],
                        '0m4b.aqwa.bpl' : [0, 600],
                        '1m0a.doma.elp' : [0, 600],
                        '1m0a.domb.lsc' : [0, 600],
                    }
print "Before scheduling"
scheduler = FullScheduler(to_schedule, resource_windows, contractual_obs,
                          time_slicing_dict)

schedule = scheduler.schedule_all()
print "After scheduling"

print_schedule(schedule)

