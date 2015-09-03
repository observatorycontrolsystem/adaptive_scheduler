#!/usr/bin/env python

'''
utils.py - Miscellaneous utility functions.

Author: Eric Saunders
March 2012
'''

import calendar
from datetime import datetime, timedelta
import time
import logging
import potsdb
import socket
import numbers
import inspect
from opentsdb_http_client import http_client
from functools import wraps

log = logging.getLogger(__name__)
fh  = logging.FileHandler('timings.dat')
fh.setLevel(logging.INFO)
formatter = logging.Formatter(fmt='%(asctime)s.%(msecs).03d %(levelname)7s: %(module)15s: %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')
fh.setFormatter(formatter)

log.addHandler(fh)

# opentsdb connection stuff
tsdb_client = potsdb.Client('opentsdb.lco.gtn', port=4242, qsize=1000, host_tag=True, mps=100, check_host=True)
hostname = socket.gethostname()
bosun_indexer_client = http_client.BosunIndexerClient('alerts.lco.gtn', qsize=200, host_tag=True, mps=10, check_host=True)

def send_tsdb_metric(metric_name, value, **kwargs):
    full_metric_name = 'adaptive_scheduler.{}'.format(metric_name)
    sent_line = tsdb_client.send(full_metric_name, value, software='adaptive_scheduler', host=hostname, **kwargs)
    bosun_indexer_client.index(full_metric_name, value, software='adaptive_scheduler', host=hostname, **kwargs)

# This decorator takes in an optional parameter for the metric name, and then any number of key/value arguments
# of the format key = metric type, value = function mapping from return value to numeric metric. This can be used
# to specify any number of additional metrics to be saved off from the return data of the function being wrapped.
def metric_timer(metric_name=None, **metric_type_to_retval_mapping_function):
    def metric_timer_decorator(method):
        @wraps(method)
        def wrapper(*args, **kwargs):
            start_time = datetime.utcnow()
            result = method(*args, **kwargs)
            end_time = datetime.utcnow()

            if inspect.ismethod(method):
                class_n = method.__self__.__class__.__name__
            else:
                class_n = method.__name__

            set_class = False
            combined_metric_name = metric_name
            if not metric_name:
                set_class = True
                combined_metric_name = '{}'.format(method.__name__)
            if 'preemption_enabled' in kwargs:
                if kwargs['preemption_enabled']:
                    combined_metric_name = 'too_{}'.format(combined_metric_name)
                else:
                    combined_metric_name = 'normal_{}'.format(combined_metric_name)
            if set_class:
                combined_metric_name = '{}.{}'.format(class_n, combined_metric_name)

            send_tsdb_metric('{}.runtime'.format(combined_metric_name), (end_time-start_time).total_seconds() * 1000.0, class_name=class_n, module_name=method.__module__, method_name=method.__name__)

            for metric_type, retval_mapping_function in metric_type_to_retval_mapping_function.iteritems():
                if hasattr(retval_mapping_function, '__call__'):
                    mapped_value = retval_mapping_function(result)
                    if isinstance(mapped_value, numbers.Number):
                        send_tsdb_metric('{}.{}'.format(combined_metric_name, metric_type), mapped_value, class_name=class_n, module_name=method.__module__, method_name=method.__name__)

            return result
        return wrapper
    return metric_timer_decorator


def increment_dict_by_value(dictionary, key, value):
    '''Build a dictionary that tracks the total values of all provided keys.'''
    if key in dictionary:
        dictionary[key] += value
    else:
        dictionary[key]  = value

    return


def merge_dicts(*args):
    '''Merge any number of dictionaries. Duplicate keys, and their corresponding values
       are dropped (i.e. we assume unique keys).'''
    return {k:v for d in args for k, v in d.items()}


def iso_string_to_datetime(iso_string):
    '''Convert ISO datetime strings of the form '2012-03-03 09:05:00' to
       datetime objects. It's no coincidence that this also happens to be the string
       representation of datetime objects.'''

    # Set the format to the string representation of a datetime
    format = '%Y-%m-%d %H:%M:%S'
    return datetime.strptime(iso_string, format)

def datetime_to_epoch(dt):
    '''Convert a datetime to Unix epoch time, a continuous, integer timescale, with
       units of a second.'''
    return calendar.timegm(dt.timetuple())

def datetime_to_normalised_epoch(dt, dt_start):
    '''Convert a datetime into kernel time, a truncated Unix epoch time. We use this
       for convenience, as a continuous measure since an arbitrary start point.'''
    return normalise(datetime_to_epoch(dt), datetime_to_epoch(dt_start))

def epoch_to_datetime(epoch_time):
    '''Convert a Unix epoch time to a datetime.'''
    return datetime.utcfromtimestamp(epoch_time)

def normalised_epoch_to_datetime(epoch_time, epoch_start):
    '''Convert a normalised kernel epoch time to a datetime. The normalisation
       reference point (epoch_start) must be known.'''
    unnormed_epoch = unnormalise(epoch_time, epoch_start)
    return epoch_to_datetime(unnormed_epoch)

def normalise(value, start):
    '''Normalise any value to a positive range, starting at zero.'''
    return value - start

def unnormalise(value, start):
    '''Perform the inverse of normalise().'''
    return value + start

def get_reservation_datetimes(reservation, semester_start):
    '''Find the real-world (datetime) start and end times of the provided
       Reservation. The start of the semester must be provided to allow
       the kernel epoch times to be unnormalised.'''

    epoch_start   = datetime_to_epoch(semester_start)
    dt_start      = normalised_epoch_to_datetime(reservation.scheduled_start,
                                                 epoch_start)
    scheduled_end = reservation.scheduled_start + reservation.duration
    dt_end        = normalised_epoch_to_datetime(scheduled_end, epoch_start)

    return dt_start, dt_end

def split_location(location):
    '''
        If the location is of the form telescope.observatory.site, then
        extract those separate components and return them. Otherwise, return
        the full location in the place of each component without splitting.

        Examples:  '0m4a.aqwb.coj' -> (0m4a, aqwb, coj)
                   'Maui'          -> (Maui, Maui, Maui)
    '''
    # Split on full stops
    DELIMITER = '.'

    # Number of sections making up the full location string
    N_COMPONENTS = 3

    separated = tuple(location.split(DELIMITER))

    if len(separated) == N_COMPONENTS:
        return separated

    # Separation wasn't possible
    return (location, location, location)

def join_location(site, observatory, telescope):
    # Join on full stops
    return "%s.%s.%s" % (telescope, observatory, site)



def timeit(method):
    '''Decorator for timing methods.'''

    def timed(*args, **kwargs):
        start  = time.time()
        result = method(*args, **kwargs)
        end    = time.time()

        #print 'TIMER: %s (%s): %2.2f sec' % (method.__name__, method.__module__, end - start)
        log.info('TIMER: %s (%s): %2.2f sec' % (method.__name__, method.__module__, end - start))
        return result

    return timed


def estimate_runtime(estimated_runtime, actual_runtime, backoff_rate=2.0, pad_percent=5.0):
    '''Estimate the next scheduler runtime given a previous estimate and actual.
    If actual > estimate, new estimate = actual * backoff_rate
    If actual <= estimate, new estimate = min(actual + pad_percent*(actual), estimate - (estimate - actual)/backoff_rate)
    backoff_rate - Factor to adjust expected runtime by.
    pad_percent - Minimum percent that a new estimate will always exceed previous actual
    '''
    new_estimated_runtime = timedelta(seconds=0)

    if estimated_runtime < actual_runtime:
        # Increase estimated run time
        new_estimated_runtime += timedelta(seconds=backoff_rate * actual_runtime.total_seconds())
    else:
        # Scheduler run time was less than or equal to estimate
        difference_from_estimate = estimated_runtime  - actual_runtime
        # Decrease the run time estimate by fraction of the difference and leave a pad
        delta_for_next_run = timedelta(seconds=difference_from_estimate.total_seconds() / backoff_rate)
        minimum_runtime = timedelta(seconds=(1.0 + pad_percent/100.0) * actual_runtime.total_seconds())
        new_estimated_runtime += max(estimated_runtime - delta_for_next_run,
                                     minimum_runtime)

    return new_estimated_runtime
