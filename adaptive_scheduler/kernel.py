'''kernel.py - Algorithm for constructing a schedule.

This module provides the Kernel class, which is responsible for constructing a 
valid schedule based on the priority and available slots of a set of 
observations.

Author: Eric Saunders (esaunders@lcogt.net)

May 2010
'''

# Required for true (non-integer) division
from __future__ import division     

from adaptive_scheduler.availability.domain import Availability


class Kernel(object):
    
    def __init__(self):
        pass


    def construct_schedule(self, plan):

        schedule = Availability(name='Schedule')

        while plan.has_targets():
            # Pop the next highest priority observation from the Plan        
            next = plan.pop()

            # Schedule it in its first valid slot
            if schedule.add_target(next):
            
                # Queue bumped targets for rescheduling
                # (Note - only happens on reschedule, not initial construction)
                while schedule.has_bumped_targets():
                    plan.add_target(schedule.pop_bumped_target())
            
            else:
                # If it can't be scheduled, make a note and drop it
                print "Target", next, "can't be scheduled."

        # Return the completed Schedule
        return schedule
