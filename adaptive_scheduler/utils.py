#!/usr/bin/env python

'''
utils.py - Miscellaneous utility functions.

Author: Eric Saunders
March 2012
'''

import calendar
from datetime import datetime
import time
import logging

log = logging.getLogger(__name__)
fh  = logging.FileHandler('timings.dat')
fh.setLevel(logging.INFO)
formatter = logging.Formatter(fmt='%(asctime)s.%(msecs).03d %(levelname)7s: %(module)15s: %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')
fh.setFormatter(formatter)

log.addHandler(fh)


class ReprMixin(object):
    '''Inherit from this class if you want your object to provide a dump of its
       contents when printed. Warning: beware of circular dependencies between
       objects inheriting this, which will cause infinite recursion!'''

#    def __repr__(self):
#        return "%s(%r)" % (self.__class__, self.__dict__)


class EqualityMixin(object):
    '''Inherit from this class if you want your object to have simple equality
       properties based on common attributes (this is what you usually want).'''

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    def __ne__(self, other):
        return not self.__eq__(other)


class DefaultMixin(ReprMixin, EqualityMixin):
    '''Inherit from this class if you want some sensible default behaviour for
       your object (introspection, equality).'''
    pass


def increment_dict_by_value(dictionary, key, value):
    '''Build a dictionary that tracks the total values of all provided keys.'''
    if key in dictionary:
        dictionary[key] += value
    else:
        dictionary[key]  = value

    return

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
