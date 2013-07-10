#!/usr/bin/env python

'''
plot_timings.py - summary line

description

Author: Eric Saunders
July 2013
'''

import sys
import collections
from lcogt import dateutil


log_fh = open(sys.argv[1], 'r')

timed_funcs = {}
Measurement = collections.namedtuple('Measurement', 'datetime value')

n_errors = 0
for line in log_fh:

    if 'TIMER' in line:
        elements = line.split()
        dt_string = '%sT%s' % (elements[0], elements[1])
        try:
            dt = dateutil.parse(dt_string)
        except ValueError:
            n_errors += 1
            continue

        package_name = elements[6].strip('():')
        timed_func_name = '%s.%s' % (package_name, elements[5])
        value = float(elements[7])
        measurement = Measurement(datetime=dt, value=value)
        func_data = timed_funcs.setdefault(timed_func_name, [])
        func_data.append(measurement)

print "Parsing problems with %d lines" % n_errors
