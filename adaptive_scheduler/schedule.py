'''schedule.py - When and where an object will be (re)-scheduled.

This module provides the Schedule class, which is an enhanced version of the
Availability matrix, with support for rescheduling.


Author: Eric Saunders (esaunders@lcogt.net)

May 2010
'''

# Required for true (non-integer) division
from __future__ import division     

from adaptive_scheduler.availability.domain import Availability


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


    def slot_fits(self, slot):
        # Check whether the proposed slot overlaps an existing slot
        # If there is no overlap, place the slot
        if self.matrix.has_space_for(slot):
            self.matrix.add_slot(slot)
        
        # Otherwise, compare priorities
            # If the original slot priority is higher, give up

            # Otherwise
                # Remove the old slot
                # Insert the new slot
                # Add the removed target back to the list of targets to add


        
