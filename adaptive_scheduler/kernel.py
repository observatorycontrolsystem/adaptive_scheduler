'''kernel.py - Algorithm for constructing a schedule.

This module provides the Kernel class, which is responsible for constructing a 
valid schedule based on the priority and available slots of a set of 
observations.

Author: Eric Saunders (esaunders@lcogt.net)

May 2010
'''

# Required for true (non-integer) division
from __future__ import division     

from adaptive_scheduler.schedule import Schedule


class Kernel(object):
    
    def __init__(self):
        pass


    def construct_schedule(self, plan):

        schedule = Schedule()

        # Find the highest priority observation
        for next_target in plan:
        
            # Schedule it in the first valid slot
            schedule.add_target(next_target)
        
