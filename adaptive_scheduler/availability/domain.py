'''domain.py - when and where an observation may be scheduled.

This package provides the Availability() class, which stores when and at which
telescopes a single observation may be scheduled. It is thus a general 
constraint map.

Author: Eric Saunders (esaunders@lcogt.net)

May 2010
'''

# Required for true (non-integer) division
from __future__ import division     

from adaptive_scheduler.utils import dt_windows_intersect

# Standard library imports
from heapq import heappush, heappop


class Slot(object):

    def __init__(self, tel, start_time, end_time, priority=None):
        self.tel      = tel
        self.start    = start_time
        self.end      = end_time
        self.priority = priority


    def clashes_with(self, slot):
    
        # There's no clash if the slots are for different telescopes
        if ( self.tel != slot.tel ):
            return False

        # If the slots overlap, there's a clash
        if dt_windows_intersect(self.start, self.end, slot.start, slot.end):
            return True

        return False


    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)



class Availability(object):
    
    def __init__(self, name, comparator, priority=None, ordering=None):
        '''Constructor.
            'ordering' is a class that specifies the order in which to provide
            slots. The default is arbitrary.
        '''
        
        self.name     = name
        self.comparator = comparator
        self.priority = priority
        self.matrix   = {}
        self.bumped_list = []
        

    def add_slot(self, new_slot):    
    
        # Create a new entry for the slot's telescope, if not already present
        self.matrix.setdefault(new_slot.tel, [])


        # Determine how many slots on this telescope overlap in time, if any
        clashes = []
        clash_idx = []
        for n, old_slot in enumerate(self.matrix[new_slot.tel]):
            # If the slot overlaps
            if old_slot.clashes_with(new_slot):
                # Add the old slot to a temporary clash list
                clash_idx.append(n)
                clashes.append(old_slot)

        # For each existing slot which clashes
        for old_slot in clashes:
            # Check if the old slot takes priority
            if self.comparator.compare(old_slot, new_slot):
                # If so, the new slot can't be scheduled here - give up
                return False
            
        # If we get here, the new slot takes priority over all clashing slots
        # Move the existing slots to a bumped list
        self.bumped_list.extend(clashes)
        for idx in reversed(clash_idx):
            print 'idx', idx
            del self.matrix[new_slot.tel][idx]

        # Schedule the new slot
        self.matrix[new_slot.tel].append(new_slot)

        return True


    def has_bumped_targets(self):
        return len(self.bumped_list) > 0
        

    def get_slots(self):
        # TODO: Turn this into an iterator
        return self.matrix


    def add_target(self, target):

        # Iterate through the slots
        matrix = target.get_slots()
        for tel in matrix:
            for slot in matrix[tel]:

                # Place the slot if it doesn't clash
                if self.add_slot(slot):
                    return True

        # All slots clash - give up
        return False


    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)


    def __str__(self):
        string = ''
        string = string + 'Name: %s\n' % self.name
        if self.priority:
            string = string + 'Priority: %d\n' % self.priority
    
        for tel in sorted(self.matrix):
            string = string + "%s\n" % tel
            
            for slot in self.matrix[tel]:
                string = string + "    %s -> %s \n" % (slot.start, slot.end)

        return string


class Plan(object):

    def __init__(self):
        self.targets = []


    def add_target(self, target):
        heappush(self.targets, (target.priority, target))

    def pop(self):
        '''Return the highest priority target, deleting it from the plan.'''
        return heappop(self.targets)[1]

    def has_targets(self):
        if len(self.targets):
            return True

        return False


    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)
