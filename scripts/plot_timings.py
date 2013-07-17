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

def print_summary(summary_data, data_dict, n_errors):
    print '{0:<60} {1:<10}\n'.format('Timer','Number')
    for k,v in data_dict.items():
        print '{0:<60} {1:<10}'.format(k,len(v))

    print "\n\nParsing problems with %d lines" % n_errors

def extract_column(line, column_index):
    dt, elements      = parse_line(line)
    received_requests = int(elements[column_index])
    measurement       = Measurement(datetime=dt, value=received_requests)
    return measurement

def parse_line(line):
    elements  = line.split()
    dt_string = '%sT%s' % (elements[0], elements[1])
    dt = dateutil.parse(dt_string)
    return dt, elements

def parse_summary_data(line, summary_data):
    if 'Received' in line and 'User Requests from Request DB' in line:
        measurement   = extract_column(line, 5)
        received_data = summary_data.setdefault('requests_in',[])
        received_data.append(measurement)
    if 'deleted' in line and 'previously scheduled block' in line:
        measurement   = extract_column(line, 7)
        deleted_data = summary_data.setdefault('requests_deleted',[])
        deleted_data.append(measurement)
    if 'Submitted' in line and 'new blocks to the POND' in line:
        measurement   = extract_column(line, 5)
        new_data = summary_data.setdefault('requests_new',[])
        new_data.append(measurement)

def parse_timer_data(line, timed_funcs):
    if 'TIMER' in line:
        dt, elements = parse_line(line)

        package_name    = elements[6].strip('():')
        timed_func_name = '%s.%s' % (package_name, elements[5])
        value           = float(elements[7])
        measurement     = Measurement(datetime=dt, value=value)
        func_data       = timed_funcs.setdefault(timed_func_name, [])
        func_data.append(measurement)

def parse_data(filename):
    log_fh = open(filename, 'r')

    timed_funcs = {}
    summary_data= {}

    n_errors = 0
    for line in log_fh:
        try:
            parse_timer_data(line, timed_funcs)
        except ValueError, e:
            n_errors += 1
            continue

        parse_summary_data(line, summary_data)

    log_fh.close()

    return summary_data, timed_funcs, n_errors

def plot_all(summary_data, timed_funcs):
    fig = plt.figure()
    ax  = fig.add_subplot(211)

    for key, timing_data in timed_funcs.iteritems():
        x,y   = zip(*timing_data)
        key = key.replace('_','')
        ax.plot(x,y,'.-',label=key)

    ax.set_title(filename)
    ax.set_xlabel('Timestamp')
    ax.set_ylabel('Duration (s)')
    ax.legend(prop={'size':6})
    ax.grid()

    ax2 = fig.add_subplot(212)

    for key, request_data in summary_data.iteritems():
        x, y  = zip(*request_data)
        ax2.plot(x,y,'.-',label=key)
    ax2.set_xlabel('Timestamp')
    ax2.set_ylabel('Number')
    ax2.legend()
    ax2.grid()

    fig.show(True)


def calculate_percentile(data, percentile=95):
    return scipy.percentile(data, percentile)

def generate_subplot(total=9):
    subplot_list = []
    size = int(math.ceil(math.sqrt(total)))
    for plot_number in range(1, total+1):
        subplot_list.append((size,size,plot_number))
    return subplot_list

def hist_all(summary_data, timed_funcs,number_of_bins=50):
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
summary_data, timed_funcs, n_errors = parse_data(filename)

# Print summary
print_summary(summary_data, timed_funcs, n_errors)

# Plot timing_data
plot_all(summary_data, timed_funcs)
hist_all(summary_data, timed_funcs)

raw_input('Press a key to continue')
