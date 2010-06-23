#!/usr/bin/env python
from __future__ import division

from nose.tools import eq_, assert_equal, raises
from nose.plugins.skip import SkipTest

# Import the module to test
from adaptive_scheduler.availability.domain import (Slot, Availability)
from adaptive_scheduler.comparator import (AlwaysTrueComparator, 
                                           SimplePriorityComparator)

from datetime import datetime



class test_availability_no_priority(object):
    '''Unit tests for the availability, the datastructure for holding an
    observation's available observing slots.'''

    def setup(self):

        self.start_time1 = datetime(2010, 1, 1, 3, 0)
        self.end_time1   = datetime(2010, 1, 1, 4, 0)
        self.tel1        = 'FTN'
        self.priority1   = 1


        self.start_time2 = datetime(2010, 1, 1, 4, 0)
        self.end_time2   = datetime(2010, 1, 1, 5, 0)
        self.tel2        = 'FTS'
        self.priority2   = 2
        
        self.target = Availability('Eta Carina', AlwaysTrueComparator())

    
    def teardown(self):
        pass
        

    def test_slots_on_different_telescopes_dont_clash(self):
        slot1 = Slot(self.tel1, self.start_time1, self.end_time1)
        self.target.add_slot(slot1)

        slot2 = Slot(self.tel2, self.start_time1, self.end_time1)

        assert self.target.add_slot(slot2)

                
    def test_can_add_non_clashing_slot(self):
        slot1 = Slot(self.tel1, self.start_time1, self.end_time1)
        self.target.add_slot(slot1)

        # Create a slot that abuts, but doesn't overlap    
        slot2 = Slot(self.tel1, self.start_time2, self.end_time2)
    
        assert self.target.add_slot(slot2)


    def test_by_default_old_slot_always_wins(self):
        slot1 = Slot(self.tel1, self.start_time1, self.end_time1)
        self.target.add_slot(slot1)

        assert not self.target.add_slot(slot1)


class test_availability_with_priority(object):
    def setup(self):

        self.start_time1 = datetime(2010, 1, 1, 3, 0)
        self.end_time1   = datetime(2010, 1, 1, 4, 0)
        self.tel1        = 'FTN'
        self.priority1   = 1


        self.start_time2 = datetime(2010, 1, 1, 4, 0)
        self.end_time2   = datetime(2010, 1, 1, 5, 0)
        self.tel2        = 'FTS'
        self.priority2   = 2
        
        self.target = Availability('Eta Carina', SimplePriorityComparator())
    
    
    def teardown(self):
        pass

       
    def test_existing_higher_priority_slot_doesnt_get_bumped(self):
        slot1 = Slot(self.tel1, self.start_time1, self.end_time1, 
                     self.priority1)

        slot2 = Slot(self.tel1, self.start_time1, self.end_time1, 
                     self.priority2)

        self.target.add_slot(slot1)
        assert not self.target.add_slot(slot2)


    def test_existing_lower_priority_slot_does_get_bumped(self):
        slot1 = Slot(self.tel1, self.start_time1, self.end_time1, 
                     self.priority2)

        slot2 = Slot(self.tel1, self.start_time1, self.end_time1, 
                     self.priority1)


        self.target.add_slot(slot1)
        assert self.target.add_slot(slot2)
        expected_matrix = dict(FTN = [slot2])
        assert_equal(self.target.matrix, expected_matrix)


    def test_lower_priority_slot_gets_bumped_four_slots(self):
        
        slot3 = Slot('FTN', datetime(2010, 1, 1, 2, 30),
                datetime(2010, 1, 1, 3, 30), 3)

        slot4 = Slot('FTN', datetime(2010, 1, 1, 3, 30),
                datetime(2010, 1, 1, 4, 30), 4)
        
        slot1 = Slot('FTN', datetime(2010, 1, 1, 3, 0), 
                datetime(2010, 1, 1, 4, 0), 1)

        slot2 = Slot('FTN', datetime(2010, 1, 1, 4, 0), 
                datetime(2010, 1, 1, 5, 0), 2)

        schedule = Availability('Schedule', SimplePriorityComparator())
        assert schedule.add_slot(slot3)
        assert schedule.add_slot(slot4)
        assert schedule.add_slot(slot1)
        assert schedule.add_slot(slot2)
        expected_matrix = dict(FTN = [slot1, slot2])
        assert_equal(schedule.matrix, expected_matrix)

    def test_after_bumping_there_are_bumped_targets(self):
        slot1 = Slot(self.tel1, self.start_time1, self.end_time1, 
                     self.priority2)

        slot2 = Slot(self.tel1, self.start_time1, self.end_time1, 
                     self.priority1)

        self.target.add_slot(slot1)
        self.target.add_slot(slot2)
    
        assert self.target.has_bumped_targets()


class test_availability_use_as_a_schedule(object):

    def setup(self):
        self.slot1 = Slot('FTN', datetime(2010, 1, 1, 3, 0), 
                     datetime(2010, 1, 1, 4, 0), 1)

        self.slot2 = Slot('FTN', datetime(2010, 1, 1, 4, 0), 
                     datetime(2010, 1, 1, 5, 0), 2)

        self.slot3 = Slot('INT', datetime(2010, 1, 1, 4, 0), 
                     datetime(2010, 1, 1, 5, 0), 2)

        self.slot4 = Slot('WHT', datetime(2010, 1, 1, 4, 0), 
                     datetime(2010, 1, 1, 5, 0), 2)
        
        self.slot5 = Slot('FTN', datetime(2010, 1, 1, 2, 30),
                     datetime(2010, 1, 1, 3, 30), 3)

        self.slot6 = Slot('FTN', datetime(2010, 1, 1, 3, 30),
                     datetime(2010, 1, 1, 4, 30), 4)

        self.slot7 = Slot('FTN', datetime(2010, 1, 1, 5, 30),
                     datetime(2010, 1, 1, 6, 30), 5)
                             
        self.schedule = Availability('Schedule', SimplePriorityComparator())
            
    def teardown(self):
        pass
        

    def test_add_target_no_slots_clash_different_tels(self):        

        target1 = Availability('Eta Carina', AlwaysTrueComparator())
        target1.add_slot(self.slot1)
        target1.add_slot(self.slot2)

        target2 = Availability('M31', AlwaysTrueComparator())
        target2.add_slot(self.slot3)
        target2.add_slot(self.slot4)

        assert self.schedule.add_target(target1)
        assert self.schedule.add_target(target2)


    def test_add_target_no_slots_clash_same_tel(self):

        target1 = Availability('Eta Carina', AlwaysTrueComparator())
        target1.add_slot(self.slot1)

        target2 = Availability('M31', AlwaysTrueComparator())
        target2.add_slot(self.slot2)

        assert self.schedule.add_target(target1)
        assert self.schedule.add_target(target2)


    def test_add_target_all_slots_clash(self):
        
        target1 = Availability('Eta Carina', AlwaysTrueComparator())
        target1.add_slot(self.slot1)        
        target1.add_slot(self.slot2)    

        target2 = Availability('M31', AlwaysTrueComparator())
        target2.add_slot(self.slot5)
        target2.add_slot(self.slot6)
    
        assert self.schedule.add_target(target1)
        assert not self.schedule.add_target(target2)


    def test_add_target_one_slot_doesnt_clash(self):
    
        target1 = Availability('Eta Carina', AlwaysTrueComparator())
        target1.add_slot(self.slot1)        
        target1.add_slot(self.slot2)    

        target2 = Availability('M31', AlwaysTrueComparator())
        target2.add_slot(self.slot5)
        target2.add_slot(self.slot7)    
    
        assert self.schedule.add_target(target1)
        assert self.schedule.add_target(target2)
                
        # Slot 7 doesn't clash in time, so that should be scheduled
        expected_matrix = dict(FTN = [self.slot1, self.slot7])
        assert_equal(self.schedule.matrix, expected_matrix)
    

    def test_add_target_new_slot_higher_priority(self):
        target1 = Availability('Eta Carina', AlwaysTrueComparator())
        target1.add_slot(self.slot5)        
        target1.add_slot(self.slot6)    

        target2 = Availability('M31', AlwaysTrueComparator())
        target2.add_slot(self.slot1)
        target2.add_slot(self.slot2)
    
        assert self.schedule.add_target(target1)
        assert self.schedule.add_target(target2)
    
        expected_matrix = dict(FTN = [self.slot1])
        assert_equal(self.schedule.matrix, expected_matrix)


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
