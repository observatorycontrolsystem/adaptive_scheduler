#!/usr/bin/env python
from __future__ import division

from nose.tools import eq_, assert_equal, raises

# Import the module to test
from adaptive_scheduler.kernel import Kernel


class test_kernel(object):
    '''Unit tests for the Adaptive Scheduler Kernel, the algorithm that 
       constructs a schedule based on a set of observations and their
       availability.'''
       
    def setup(self):
        self.kernel = Kernel()
    
    def teardown(self):
        pass
        
    
    
    def test_find_highest_priority_observation(self):
        pass
            
    def test_add_first_valid_slot_to_schedule_no_weather(self):
        pass
    
    def test_add_first_valid_slot_to_schedule_bad_weather(self):
        pass

    def test_add_first_valid_slot_to_schedule_bad_telescope(self):
        pass
