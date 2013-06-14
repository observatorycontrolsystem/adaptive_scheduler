#!/usr/bin/env python

from datetime import datetime
from rise_set.visibility import Visibility
from rise_set.angle import Angle
from rise_set.sky_coordinates import RightAscension, Declination

from adaptive_scheduler.model2 import (Telescope, Target)

start = datetime(2011, 11, 1, 0, 0, 0)
end   = datetime(2011, 11, 3, 0, 0, 0)

resource_name = '1m0a.doma.bpl'
tel = Telescope(
               name      = '1m0a.doma.bpl',
               tel_class = '1m0',
               latitude  = 34.433157,
               longitude = -119.86308,
               horizon   = 25,
             )

target = Target(
                  ra  = '20 41 25.91',
                  dec = '+45 16 49.22',
                )

#visibility_from = construct_visibilities(tels, start, end)

visibility_from = {}

rs_telescope = {
                    'latitude'  : Angle(degrees=tel.latitude),
                    'longitude' : Angle(degrees=tel.longitude),
                  }

visibility_from[resource_name] = Visibility(rs_telescope, start,
                                       end, tel.horizon,
                                       twilight='nautical')


rs_target = {
              'ra'    : RightAscension('20 41 25.91'),
              'dec'   : Declination('+45 16 49.22'),
             }
rs_target = {
                'ra'    : target.ra,
                'dec'   : target.dec,
               }

visibility = visibility_from[resource_name]

# Find when it's dark, and when the target is up
rs_dark_intervals = visibility.get_dark_intervals()
rs_up_intervals   = visibility.get_target_intervals(rs_target)


for a in rs_up_intervals:
    print a
