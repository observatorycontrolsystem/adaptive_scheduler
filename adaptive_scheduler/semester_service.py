'''
semester_service.py - Utility functions for getting the semester dates.

description

Authors: Martin Norbury
February 2013
'''
from datetime import datetime

def _timestamp_is_semester_A(dt):
    return 4 <= dt.month < 10

def get_semester_start(dt=None):
    dt = dt or datetime.utcnow()
    year, month, day = dt.year, 4, 1
    if not _timestamp_is_semester_A(dt):
        month = 10
        if dt.month < 10: year -= 1
    return datetime(year, month, day)

def get_semester_end(dt=None):
    dt = dt or datetime.utcnow()
    year, month, day = dt.year, 10, 1
    if not _timestamp_is_semester_A(dt):
        month = 4
        if dt.month >= 10: year += 1
    return datetime(year, month, day)

def get_semester_block(dt=None):
    return get_semester_start(dt), get_semester_end(dt)

def get_semester_code(dt=None):
    code = _timestamp_is_semester_A(dt) and 'A' or 'B'
    return '%d%s' % (get_semester_start(dt).year, code)
