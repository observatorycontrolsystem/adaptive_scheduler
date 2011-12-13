#!/usr/bin/env python

from adaptive_scheduler.kernel.timepoint import Timepoint
from adaptive_scheduler.kernel.reservation_v2 import Reservation_v2 as Reservation
from adaptive_scheduler.kernel.fullscheduler_v1 import FullScheduler_v1 as FullScheduler
from adaptive_scheduler.kernel.reservation_v2 import CompoundReservation_v2 as CompoundReservation

from adaptive_scheduler.printing import print_compound_reservation
from adaptive_scheduler.kernel.intervals import Intervals

start = 'start'
end   = 'end'
windows = [
            Timepoint(13813, start),
            Timepoint(43128, end),
            Timepoint(100180, start),
            Timepoint(129292, end),
            Timepoint(186549, start),
            Timepoint(215456, end),
            Timepoint(272919, start),
            Timepoint(301620, end),
            Timepoint(359290, start),
            Timepoint(387784, end),
            Timepoint(445662, start),
            Timepoint(473949, end),
            Timepoint(532035, start),
            Timepoint(560113, end),
           ]

resource_windows = {
      'maui' : [
                    Timepoint(13813, start),
                    Timepoint(59238, end),
                    Timepoint(100180, start),
                    Timepoint(145667, end),
                    Timepoint(186549, start),
                    Timepoint(232097, end),
                    Timepoint(272919, start),
                    Timepoint(318528, end),
                    Timepoint(359290, start),
                    Timepoint(404959, end),
                    Timepoint(445662, start),
                    Timepoint(491390, end),
                    Timepoint(532035, start),
                    Timepoint(577822, end),
                ],
        }



res = Reservation(priority=1, duration=60, resource='maui', possible_windows=Intervals(windows))
compound_res = CompoundReservation([res], 'single')

print_compound_reservation(compound_res)

scheduler = FullScheduler([compound_res], resource_windows, [])
scheduler.schedule_all()
