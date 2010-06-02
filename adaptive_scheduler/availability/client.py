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

from adaptive_scheduler.availability.domain import Availability



class IClient(object):
    def get_current_plan(self):
        pass
        


class HardCodedClient(IClient):

    def __init__(self):
        self.obs1 = Availability()
        
        slots = (
                  (
                    'FTN', datetime(year=2010, month=1, day=1, 
                                      hour=3, minute=0, second=0),
                           datetime(year=2010, month=1, day=1, 
                                      hour=3, minute=30, second=0)
                   ),
                  (                   
                    'FTN', datetime(year=2010, month=1, day=1, 
                                        hour=3, minute=30, second=0),
                           datetime(year=2010, month=1, day=1, 
                                        hour=4, minute=0, second=0),
                   ), 
                  (
                    'FTS', datetime(year=2010, month=1, day=1, 
                                        hour=4, minute=0, second=0),
                           datetime(year=2010, month=1, day=1, 
                                        hour=4, minute=30, second=0),
                   ), 
                 )
        
        for (tel, start_time, end_time) in slots:        
           self.obs1.add_slot(tel, start_time, end_time)
        
        self.plan.append(self.obs1)







    def get_current_plan(self):
        return self.plan
