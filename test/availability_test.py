#!/usr/bin/env python
from __future__ import division

from nose.tools import eq_, assert_equal, raises

# Import the module to test
from adaptive_scheduler.availability.domain import (Slot, Availability, 
                                                    TelescopeNotFoundError)

from datetime import datetime


class test_availability(object):
    '''Unit tests for the availability, the datastructure for holding an
    observation's available observing slots.'''

    def setup(self):
        self.availability = Availability('Eta Carina')
    
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



    @raises(TelescopeNotFoundError)
    def test_has_space_for_tel_not_defined(self):
        start_time = datetime(year=2010, month=1, day=1, 
                              hour=3, minute=0, second=0),
        end_time   = datetime(year=2010, month=1, day=1, 
                              hour=4, minute=0, second=0),
        new_slot = Slot('Poo',start_time, end_time)
    
        eq_(self.availability.has_space_for(new_slot), False)



        
    
class test_slots(object):

    def setup(self):
        self.start_time = datetime(year=2010, month=1, day=1, 
                                   hour=3, minute=0, second=0)
                                   
        self.end_time = datetime(year=2010, month=1, day=1, 
                                 hour=5, minute=0, second=0)

        self.target_name = 'Eta Carina'
                                   
        self.slot = Slot(tel='FTN', start_time=self.start_time, 
                         end_time=self.end_time, target_name=self.target_name)


    def teardown(self):
        pass
        
        
    def test_metadata_is_stored(self):
        eq_(self.slot.metadata['target_name'], self.target_name)
