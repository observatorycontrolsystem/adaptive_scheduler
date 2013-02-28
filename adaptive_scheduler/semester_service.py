'''
semester_service.py - Utility functions for getting the semester dates.

There are 2 observing semesters at LCO; semester A from April->October
and semester B from October->April. A semester is assumed to start at
midnight on April/October 1st and finish at midnight on October/April 1st
i.e. the start and stop times coincide.

Authors: Martin Norbury
February 2013
'''
from datetime import datetime

def _timestamp_is_semester_A(dt):
    return 4 <= dt.month < 10

def get_semester_start(dt=None):
    ''' Returns the semester start time (datetime)

    dt - timestamp at which to find the semester start time
         (assumed to be utcnow if not specified)
    '''
    dt = dt or datetime.utcnow()
    year, month, day = dt.year, 4, 1
    if not _timestamp_is_semester_A(dt):
        month = 10
        if dt.month < 10: year -= 1
    return datetime(year, month, day)

def get_semester_end(dt=None):
    ''' Returns the semester end time (datetime)

    dt - timestamp at which to find the semester end time
         (assumed to be utcnow if not specified)
    '''
    dt = dt or datetime.utcnow()
    year, month, day = dt.year, 10, 1
    if not _timestamp_is_semester_A(dt):
        month = 4
        if dt.month >= 10: year += 1
    return datetime(year, month, day)

def get_semester_block(dt=None):
    ''' Return the semester start and end time ( start_time, end_time )

    dt - timestamp at which to find the semester start and
         end time (assumed to be utcnow if not specified)
    '''
    return get_semester_start(dt), get_semester_end(dt)

def get_semester_code(dt=None):
    ''' Return the semester code e.g. "2013A" or "2013B" '''
    code = _timestamp_is_semester_A(dt) and 'A' or 'B'
    return '%d%s' % (get_semester_start(dt).year, code)
