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

        self.start_time1 = datetime(year=2010, month=1, day=1, 
                              hour=3, minute=0, second=0)
        self.end_time1   = datetime(year=2010, month=1, day=1, 
                              hour=4, minute=0, second=0)

        self.tel1  = 'FTN'


        self.start_time2 = datetime(year=2010, month=1, day=1, 
                              hour=4, minute=0, second=0)
        self.end_time2   = datetime(year=2010, month=1, day=1, 
                              hour=5, minute=0, second=0)




        
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
        new_slot = Slot('Poo',self.start_time1, self.end_time1)
    
        assert_equal(self.availability.has_space_for(new_slot), False)


    def test_has_space_for_tel_no_clashes(self):    
        # Add a non-clashing slot to seed the matrix with the telescope name
        self.slot1 = Slot(self.tel1, self.start_time1, self.end_time1)
        self.availability.add_slot(self.slot1)

        # Create a slot that abuts, but doesn't overlap    
        self.slot2 = Slot(self.tel1, self.start_time2, self.end_time2)
    
        assert_equal(self.availability.has_space_for(self.slot2), True)


        
    def test_new_slot_starts_within_existing_slot(self):
        slot1 = Slot(self.tel1, self.start_time1, self.end_time1)
        
        # Add the first slot to the matrix
        self.availability.add_slot(slot1)
        
        
        # Start time of new slot falls within the existing slot
        start_time2 = datetime(year=2010, month=1, day=1, 
                               hour=3, minute=30, second=0)
        # End time of new slot falls after the existing slot
        end_time2  = datetime(year=2010, month=1, day=1, 
                               hour=6, minute=30, second=0)
                               
        slot2 = Slot(self.tel1, start_time2, end_time2)
                        
        assert_equal(self.availability.has_space_for(slot2), False)


    def test_new_slot_ends_within_existing_slot(self):
        slot1 = Slot(self.tel1, self.start_time1, self.end_time1)
        
        # Add the first slot to the matrix
        self.availability.add_slot(slot1)
        
        # Start time of new slot falls before the existing slot begins
        start_time2 = datetime(year=2010, month=1, day=1,
                               hour=2, minute=0, second=0)

        # End time of new slot falls inside the existing slot
        end_time2 = datetime(year=2010, month=1, day=1,
                             hour=3, minute=30, second=0)
        
        slot2 = Slot(self.tel1, start_time2, end_time2)

        assert_equal(self.availability.has_space_for(slot2), False)




    
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
