'''availability.py - when and where an observation may be scheduled.

This package provides the Availability() class, which stores when and at which
telescopes an observation may be scheduled. It is thus a general constraint
map.

Author: Eric Saunders (esaunders@lcogt.net)

May 2010
'''

# Required for true (non-integer) division
from __future__ import division     



class Availability(object):
    
    def __init__(self, ordering=None):
        '''Constructor.
            'ordering' is a class that specifies the order in which to provide
            slots. The default is arbitrary.
        '''
        pass
