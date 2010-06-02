'''schedule.py - When and where an object will be (re)-scheduled.

This module provides the Schedule class, which is an enhanced version of the
Availability matrix, with support for rescheduling.


Author: Eric Saunders (esaunders@lcogt.net)

May 2010
'''

# Required for true (non-integer) division
from __future__ import division     



class Schedule(object):
    
    def __init__(self):
        self.matrix = Availability(name='Schedule')
           
           
    def add_target(self, target):
        print 'Checking availability for new target'


        # Compare the first slot with the current schedule                
        # If there is no observation of higher priority in the way, add it
        for slot in target:
            if self.slot_fits(slot):
                self.matrix.add_slot(slot)
        
        
        # Otherwise, move on to the next slot
        # Complain if it can't fit anywhere
