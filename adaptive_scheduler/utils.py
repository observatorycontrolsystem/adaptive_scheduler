'''utils.py - Miscellaneous utility functions.

Author: Eric Saunders (esaunders@lcogt.net)

June 2010
'''

# Required for true (non-integer) division
from __future__ import division

from datetime import datetime

def dt_windows_intersect(dt1start, dt1end, dt2start, dt2end):
    '''Returns true if two datetime ranges intersect. Note that if two datetime
    ranges are adjacent, they do not intersect.
    
    Code based on:
    http://beautifulisbetterthanugly.com/posts/2009/oct/7/datetime-intersection-python/
    http://stackoverflow.com/questions/143552/comparing-date-ranges  
    '''

    if dt2end <= dt1start or dt2start >= dt1end:
        return False

    return  dt1start <= dt2end and dt1end >= dt2start
