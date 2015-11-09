#!/usr/bin/env python

'''
utils.py - Miscellaneous utility functions.

Author: Eric Saunders
March 2012
'''

import calendar
from datetime import datetime, timedelta
import time
from math import pi, cos
import logging
from opentsdb_python_metrics import metric_wrappers
from opentsdb_python_metrics.metric_wrappers import metric_timer
from opentsdb_python_metrics.metric_wrappers import metric_wrapper

from opentsdb_python_metrics.metric_wrappers import SendMetricMixin

log = logging.getLogger(__name__)
fh  = logging.FileHandler('timings.dat')
fh.setLevel(logging.INFO)
formatter = logging.Formatter(fmt='%(asctime)s.%(msecs).03d %(levelname)7s: %(module)15s: %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')
fh.setFormatter(formatter)

log.addHandler(fh)

# Setting the project name in the opentsdb_python_metrics library
metric_wrappers.project_name = 'adaptive_scheduler'

# Constants used to denote whether we are in a normal or ToO loop currently
NORMAL_SCHEDULE_TYPE = 'normal'
TOO_SCHEDULE_TYPE = 'too'


def set_schedule_type(schedule_type):
    '''
        Function takes in a schedule type and adjusts the global tags used in saving metrics accordingly
    :param schedule_type:
    :return:
    '''
    if schedule_type is NORMAL_SCHEDULE_TYPE:
        metric_wrappers.global_tags = {'schedule_type':'normal'}
    elif schedule_type is TOO_SCHEDULE_TYPE:
        metric_wrappers.global_tags = {'schedule_type':'too'}
    else:
        metric_wrappers.global_tags = None


def convert_proper_motion(proper_motion_ra, proper_motion_dec):
    '''
        Function converts proper motion from marcsec/year with cos(d) term to arcsec/year without term
        Simbad convention for proper motion is marcsec*cos(d)/year so this format is most convenient for users
    :param proper_motion_ra: marcsec/year with cos(dec)
    :param proper_motion_dec: marcsec/year
    :return: proper_motion_ra, proper_motion_dec
    '''
    prop_motion_dec = proper_motion_dec / 1000.0
    prop_motion_ra = (proper_motion_ra / 1000.0) / cos((prop_motion_dec * pi) / 648000.0)
    return prop_motion_ra, prop_motion_dec


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


@metric_wrapper('estimate_runtime', value=lambda x: x.total_seconds()*1000.0)
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
