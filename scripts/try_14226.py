#!/usr/bin/env python

'''
example_rise_set.py - Example usage of rise_set functions.

This example gives some simple demonstrations of how to use the rise_set library to
calculate rise, set and transit times for a star and the sun.

Author: Eric Saunders
August 2011
'''


from rise_set.angle           import Angle
from rise_set.sky_coordinates import RightAscension, Declination
from rise_set.rates           import ProperMotion
from rise_set.astrometry      import calc_rise_set, calc_sunrise_set
from rise_set.visibility      import Visibility

from datetime import datetime, date
import logging

from adaptive_scheduler.kernel_mappings import rise_set_to_kernel_intervals
from itertools import izip_longest

def print_dt_intervals(intervals):
    for s, e in intervals:
        print "%s -> %s" % (s.strftime('%Y-%m-%d %H:%M:%S'), e.strftime('%Y-%m-%d %H:%M:%S'))

def grouper(n, iterable, fillvalue=None):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


def sunrise_sunset_from_lsc():

    # Date
    my_date = date(year=2013, month=9, day=7)

    # Site (East +ve longitude)
    site = {
        'latitude'  : Angle(degrees=-30.1673472222),
        'longitude' : Angle(degrees=-70.8046722222),
    }

    # Do the calculation
    (transit, rise, setting) = calc_sunrise_set(site, my_date, 'nautical')

    return (transit, rise, setting)



if __name__ == '__main__':

    # Configure logging
    logging.basicConfig(
        format = "%(levelname)s %(lineno)-6d %(message)s",
        level  = logging.INFO
    )


    # Site (East +ve longitude)
    site_lsc = {
        'latitude'  : Angle(degrees=-30.1673472222),
        'longitude' : Angle(degrees=-70.8046722222),
    }
    site_cpt = {
        'latitude'  : Angle(degrees=-32.38059),
        'longitude' : Angle(degrees=20.8101083333),
    }
    site_elp = {
        'latitude'  : Angle(degrees=30.6801),
        'longitude' : Angle(degrees=-104.015194444),
    }
    site_coj = {
        'latitude'  : Angle(degrees=-31.273),
        'longitude' : Angle(degrees=149.070593),
    }


    # Scheduler horizon time limits
    start = datetime.strptime('2013-09-06 23:36:23', '%Y-%m-%d %H:%M:%S')
    end   = datetime.strptime('2013-10-01 00:00:00', '%Y-%m-%d %H:%M:%S')

    u_start = datetime.strptime('2013-09-07 08:34:57', '%Y-%m-%d %H:%M:%S')
    u_end   = datetime.strptime('2013-09-07 20:34:57', '%Y-%m-%d %H:%M:%S')
    user_windows   = [(u_start, u_end)]

    tim_target = {
                   'ra'  : RightAscension(degrees=75.899167),
                   'dec' : Declination(degrees=1.5715),
                   'epoch'             : 2000,
                  }
    airmass = 1.66

    site = site_coj
    v = Visibility(site=site,
                   start_date=start,
                   end_date=end,
                   horizon=30,
                   twilight='nautical')

    rs_dark_intervals = v.get_dark_intervals()
    rs_up_intervals   = v.get_target_intervals(tim_target, airmass=airmass)

    print "\nRS Dark:"
    print_dt_intervals(rs_dark_intervals)


    print "\nRS Up:"
    print_dt_intervals(rs_up_intervals)


    # Intersection code from scheduler

    # Convert the rise_set intervals into kernel speak
    dark_intervals = rise_set_to_kernel_intervals(rs_dark_intervals)
    up_intervals   = rise_set_to_kernel_intervals(rs_up_intervals)

    # Construct the intersection (dark AND up) reprsenting actual visibility
    du_intersection = dark_intervals.intersect([up_intervals])

    print "\nDark/up intersection:"
    for s, e in grouper(2, du_intersection.timepoints):
        print "%s (%s) -> %s (%s)" % (s.time, s.type,  e.time, e.type)


    # Intersect with any window provided in the user request
    print "\nUser windows:"
    print_dt_intervals(user_windows)

    user_intervals = rise_set_to_kernel_intervals(user_windows)
    duu_intersection   = du_intersection.intersect([user_intervals])


    print "\nDark/up/user intersection:"
    for s, e in grouper(2, duu_intersection.timepoints):
        print "%s (%s) -> %s (%s)" % (s.time, s.type,  e.time, e.type)






