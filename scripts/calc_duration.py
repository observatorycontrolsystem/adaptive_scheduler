#!/usr/bin/env python

'''
calc_duration.py - Simple script to find the duration of a Request.

Tim L's Request 1182 is going straight to UNSCHEDULABLE. Why?

Author: Eric Saunders
April 2013
'''

from __future__ import division
from adaptive_scheduler.model2 import Request, Molecule


m = Molecule(
              exposure_count=47,
              exposure_time=10,
              bin_x=2,
              bin_y=2,
              filter = 'W'
            )

target         = None
windows        = None
request_number = None
molecules = [m]
r = Request(target, molecules, windows, request_number)
print "Duration = %ds (%f min)" % (r.get_duration(), r.get_duration()/60.0)

