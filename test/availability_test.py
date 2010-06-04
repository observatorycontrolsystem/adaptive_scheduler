#!/usr/bin/env python
from __future__ import division

from nose.tools import eq_, assert_equal, raises

# Import the module to test
from adaptive_scheduler.availability.domain import (Slot, Availability)

from datetime import datetime


class test_availability(object):
    '''Unit tests for the availability, the datastructure for holding an
    observation's available observing slots.'''

    def setup(self):

        self.start_time1 = datetime(2010, 1, 1, 3, 0)
        self.end_time1   = datetime(2010, 1, 1, 4, 0)

        self.tel1  = 'FTN'
        self.tel2  = 'FTS'

        self.start_time2 = datetime(2010, 1, 1, 4, 0)
        self.end_time2   = datetime(2010, 1, 1, 5, 0)
        
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



    def test_slots_on_different_telescopes_dont_clash(self):
        self.slot1 = Slot(self.tel1, self.start_time1, self.end_time1)
        self.availability.add_slot(self.slot1)

        self.slot2 = Slot(self.tel2, self.start_time1, self.end_time1)
        assert self.availability.add_slot(self.slot2)

                
    def test_can_add_non_clashing_slot(self):
        self.slot1 = Slot(self.tel1, self.start_time1, self.end_time1)
        self.availability.add_slot(self.slot1)

        # Create a slot that abuts, but doesn't overlap    
        self.slot2 = Slot(self.tel1, self.start_time2, self.end_time2)
    
        assert self.availability.add_slot(self.slot2)


    def test_cant_add_same_slot_twice(self):
        self.slot1 = Slot(self.tel1, self.start_time1, self.end_time1)
        self.availability.add_slot(self.slot1)

        assert not self.availability.add_slot(self.slot1)



        
    def test_new_slot_starts_within_existing_slot(self):
        slot1 = Slot(self.tel1, self.start_time1, self.end_time1)
        
        # Add the first slot to the matrix
        self.availability.add_slot(slot1)
        
        
        # Start time of new slot falls within the existing slot
        start_time2 = datetime(2010, 1, 1, 3, 30)
        # End time of new slot falls after the existing slot
        end_time2  = datetime(2010, 1, 1, 6, 30)
                               
        slot2 = Slot(self.tel1, start_time2, end_time2)
                        
        assert_equal(self.availability.slot_clashes(slot2), True)


    def test_new_slot_ends_within_existing_slot(self):
        slot1 = Slot(self.tel1, self.start_time1, self.end_time1)
        
        # Add the first slot to the matrix
        self.availability.add_slot(slot1)
        
        # Start time of new slot falls before the existing slot begins
        start_time2 = datetime(2010, 1, 1, 2, 0)

        # End time of new slot falls inside the existing slot
        end_time2 = datetime(2010, 1, 1, 3, 30)
        
        slot2 = Slot(self.tel1, start_time2, end_time2)

        assert_equal(self.availability.slot_clashes(slot2), True)




    
class test_slots(object):

    def setup(self):
        self.start_time = datetime(2010, 1, 1, 3, 0)
                                   
        self.end_time = datetime(2010, 1, 1, 5, 0)

        self.target_name = 'Eta Carina'
                                   
        self.slot1 = Slot(tel='FTN', start_time=self.start_time, 
                         end_time=self.end_time, target_name=self.target_name)

        self.slot2 = Slot(tel='FTS', start_time=self.start_time, 
                         end_time=self.end_time, target_name=self.target_name)


        self.slot3 = Slot(
                           tel='FTN', 
                           start_time=datetime(2010, 1, 1, 3, 30),
                           end_time=datetime(2010, 1, 1, 5, 30),
                           target_name=self.target_name
                          )


    def teardown(self):
        pass
        
        
    def test_metadata_is_stored(self):
        assert_equal(self.slot1.metadata['target_name'], self.target_name)


    def test_slots_on_different_telescopes_dont_clash(self):
        assert not self.slot1.clashes_with(self.slot2)


    def test_overlapping_slots_clash(self):
        assert self.slot1.clashes_with(self.slot3)
