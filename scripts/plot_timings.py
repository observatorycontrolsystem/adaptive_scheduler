#!/usr/bin/env python

'''
plot_timings.py - summary line

description

Author: Eric Saunders
July 2013
'''

import sys
import math
import collections
from lcogt import dateutil
from matplotlib import pyplot as plt
import numpy as np
import scipy

Measurement = collections.namedtuple('Measurement', 'datetime value')

def print_summary(data_dict, n_errors):
    print '{0:<60} {1:<10}\n'.format('Timer','Number')
    for k,v in data_dict.items():
        print '{0:<60} {1:<10}'.format(k,len(v))

    print "\n\nParsing problems with %d lines" % n_errors

def parse_data(filename):
    log_fh = open(filename, 'r')

    timed_funcs = {}

    n_errors = 0
    for line in log_fh:

        if 'TIMER' in line:
            elements  = line.split()
            dt_string = '%sT%s' % (elements[0], elements[1])
            try:
                dt = dateutil.parse(dt_string)
            except ValueError:
                n_errors += 1
                continue

            package_name    = elements[6].strip('():')
            timed_func_name = '%s.%s' % (package_name, elements[5])
            value           = float(elements[7])
            measurement     = Measurement(datetime=dt, value=value)
            func_data       = timed_funcs.setdefault(timed_func_name, [])
            func_data.append(measurement)

    log_fh.close()

    return timed_funcs, n_errors

def plot_all(timed_funcs):
    fig = plt.figure()
    ax  = fig.add_subplot(111)

    for key, timing_data in timed_funcs.iteritems():
        x,y   = zip(*timing_data)
        key = key.replace('_','')
        ax.plot(x,y,'.-',label=key)

    ax.set_title(filename)
    ax.set_xlabel('Timestamp')
    ax.set_ylabel('Duration (s)')
    ax.legend()
    ax.grid()
    fig.show(True)


def calculate_percentile(data, percentile=95):
    return scipy.percentile(data, percentile)

def generate_subplot(total=9):
    subplot_list = []
    size = int(math.ceil(math.sqrt(total)))
    for plot_number in range(1, total+1):
        subplot_list.append((size,size,plot_number))
    return subplot_list

def hist_all(timed_funcs,number_of_bins=50):
    number_of_timers = len(timed_funcs.keys())

    fig           = plt.figure()
    subplot_list  = generate_subplot(number_of_timers)

    for i, key in enumerate(timed_funcs.keys()):
        subplot     = subplot_list[i]
        ax          = fig.add_subplot(*subplot)
        timing_data = timed_funcs[key]
        x,y         = zip(*timing_data)
        key         = key.replace('_','')
        values, bins, _ = ax.hist(y,bins=number_of_bins)
#        values, bins, _ = ax.hist(y,bins=number_of_bins,label=key, cumulative=True, normed=True)
        ax.vlines(calculate_percentile(y,percentile=95), 0.0, max(values))
        ax.vlines(calculate_percentile(y,percentile=75), 0.0, max(values))
        ax.vlines(calculate_percentile(y,percentile=50), 0.0, max(values))
        ax.set_title(key)
        ax.set_xlabel('Duration(s)')
        ax.set_ylabel('N')
        ax.legend()
        ax.grid()

    fig.show(True)

# Parse the data
filename = sys.argv[1]
timed_funcs, n_errors = parse_data(filename)

# Print summary
print_summary(timed_funcs, n_errors)

# Plot timing_data
plot_all(timed_funcs)
hist_all(timed_funcs)

raw_input('Press a key to continue')
