'''
semester_service.py - Utility functions for getting the semester dates.

There are 2 observing semesters at LCO; semester A from April->October
and semester B from October->April. A semester is assumed to start at
midnight on April/October 1st and finish at midnight on October/April 1st
i.e. the start and stop times coincide.

SPECIAL NOTE: In 2014, the semester boundary was shifted by one month, because of
a delayed start to public operations. Therefore 2013B finishes in May 2014, and
2014A begins in May 2014, rather than the usual April. This explains the cruftiness
added to the previous elegant algorithm for these semesters.

Authors: Martin Norbury
         Eric Saunders
February 2013
'''
from datetime import datetime

def _semester_is_2014A(dt):
    if dt.year == 2014:
        if _timestamp_is_semester_A(dt):
            return True

    return False

def _timestamp_is_semester_A(dt):
    start_month = 4
    if dt.year == 2014:
        start_month = 5
    return start_month <= dt.month < 10

def get_semester_start(dt=None):
    ''' Returns the semester start time (datetime)

    dt - timestamp at which to find the semester start time
         (assumed to be utcnow if not specified)
    '''
    dt = dt or datetime.utcnow()
    year, month, day = dt.year, 4, 1
    if _semester_is_2014A(dt):
        month = 5

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
        if get_semester_code(dt) == '2013B':
            month = 5
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
    dt = dt or datetime.utcnow()
    code = _timestamp_is_semester_A(dt) and 'A' or 'B'
    return '%d%s' % (get_semester_start(dt).year, code)
