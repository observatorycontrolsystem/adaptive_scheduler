#!/usr/bin/env python
from __future__ import division

from nose.tools import eq_, assert_equal, raises

# Import the module to test
from adaptive_scheduler.domain.availability import Availability


class test_availability(object):
    '''Unit tests for the availability, the datastructure for holding an
    observation's available observing slots.'''

    def setup(self):
        self.availability = Availability()
    
    def teardown(self):
        pass
        
    
    def test_should_have_time_bounds(self):
        pass
    
    
    
    def test_get_priority(self):
        pass
    
    def test_change_priority(self):
        pass
    
    
    
    def test_add_slot(self):
        pass
        
    def test_get_next_slot(self):
        pass
    
    def test_slot_iterator_can_exhaust(self):
        pass
        
    
