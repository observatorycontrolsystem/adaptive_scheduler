#!/usr/bin/env python

'''
calc_duration.py - Simple script to find the duration of a Request.

Author: Eric Saunders
April 2013
'''

from __future__ import division
from adaptive_scheduler.model2 import Request, Molecule


m = Molecule(
              exposure_count=2,
              exposure_time=90,
              bin_x=2,
              bin_y=2,
              filter = 'V'
            )

target         = None
windows        = None
request_number = None
constraints    = None
molecules = [m]
r = Request(target, molecules, windows, constraints, request_number)
print "Duration = %ds (%f min)" % (r.get_duration(), r.get_duration()/60.0)

