'''client.py - Classes for obtaining an Availability Matrix.

This module provides Availability client implementations, which construct and
return an availability matrix for a single observation from some external 
source.

Author: Eric Saunders (esaunders@lcogt.net)

June 2010
'''

# Required for true (non-integer) division
from __future__ import division     

from datetime import datetime

from adaptive_scheduler.availability.domain import Slot, Availability, Plan



class IClient(object):
    def get_current_plan(self):
        pass
        


class HardCodedClient(IClient):

    def __init__(self):
        
        self.plan = Plan()
        target1 = self.construct_target1()
        target2 = self.construct_target2()
        self.plan.add_target(target1)
        self.plan.add_target(target2)



    def construct_target1(self):
        target1_slots = (
                         Slot(
                              start_time = datetime(year=2010, month=1, day=1, 
                                                    hour=3, minute=0, second=0),
                                end_time = datetime(year=2010, month=1, day=1, 
                                                   hour=3, minute=30, second=0),
                                     tel = 'FTN'
                              ),
                         Slot(
                              start_time = datetime(year=2010, month=1, day=1, 
                                                    hour=3, minute=30, second=0),
                                end_time = datetime(year=2010, month=1, day=1, 
                                                    hour=4, minute=0, second=0),
                                     tel = 'FTN'
                              ), 
                         Slot(
                              start_time = datetime(year=2010, month=1, day=1, 
                                                hour=4, minute=0, second=0),
                                end_time = datetime(year=2010, month=1, day=1, 
                                                    hour=4, minute=30, second=0),
                                     tel = 'FTS'
                              ), 
                         )

        target1_name = 'Tau Ceti'
        target1_priority = 1

        target1 = self.construct_target(target1_name, 
                                        target1_priority, 
                                        target1_slots)

        return target1



    def construct_target2(self):
        target2_slots = (
                         Slot(
                              start_time = datetime(year=2010, month=1, day=1, 
                                                    hour=3, minute=0, second=0),
                                end_time = datetime(year=2010, month=1, day=1, 
                                                    hour=4, minute=30, second=0),
                                     tel = 'FTN'
                              ),
                         Slot(
                              start_time = datetime(year=2010, month=1, day=1, 
                                                    hour=6, minute=0, second=0),
                                end_time = datetime(year=2010, month=1, day=1, 
                                                    hour=7, minute=0, second=0),
                                     tel = 'FTS'
                              ),
                         )

        target2_name = 'Eta Carina'
        target2_priority = 2

        target2 = self.construct_target(target2_name, 
                                        target2_priority, 
                                        target2_slots)

        return target2



    def construct_target(self, name, priority, slots):
        target = Availability(name=name, priority=priority)
                        
        for slot in slots:        
           target.add_slot(slot)
                
        return target



    def get_current_plan(self):
        return self.plan
