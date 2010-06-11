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

       




    
class test_slots(object):

    def setup(self):
        self.start_time = datetime(2010, 1, 1, 3, 0)
                                   
        self.end_time = datetime(2010, 1, 1, 5, 0)

        self.target_name = 'Eta Carina'
                                   
        self.slot1 = Slot(tel='FTN', start_time=self.start_time, 
                         end_time=self.end_time)

        self.slot2 = Slot(tel='FTS', start_time=self.start_time, 
                         end_time=self.end_time)


        self.slot3 = Slot(
                           tel='FTN', 
                           start_time=datetime(2010, 1, 1, 3, 30),
                           end_time=datetime(2010, 1, 1, 5, 30),
                          )


    def teardown(self):
        pass
        
        
    def test_slots_on_different_telescopes_dont_clash(self):
        assert not self.slot1.clashes_with(self.slot2)


    def test_overlapping_slots_clash(self):
        assert self.slot1.clashes_with(self.slot3)
