#!/usr/bin/env python

import datetime
from rise_set.angle import Angle
from rise_set.sky_coordinates import RightAscension, Declination
from rise_set.astrometry import calc_rise_set


date = datetime.datetime(2011, 11, 1, 0, 0, 0)

maui = {
           'latitude'  : Angle(degrees = 20.7069444444),
           'longitude' : Angle(degrees = -156.258055556)
        }

canopus = {
        'name'  : 'canopus',
        'ra'    : RightAscension('06 23 57.11'),
        'dec'   : Declination('-52 40 03.5'),
        'epoch' : 2000,
    }

(transit, rise, set) = calc_rise_set(canopus, maui, date, horizon=Angle(degrees=22.0))


print "Transit", transit
print "Rise", rise
print "Set", set
