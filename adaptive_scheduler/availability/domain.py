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
from adaptive_scheduler.maxheap import heappush, heappop


class Slot(object):

    def __init__(self, tel, start_time, end_time, **kwargs):
        self.tel      = tel
        self.start    = start_time
        self.end      = end_time
        self.metadata = kwargs


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
    
    def __init__(self, name, priority=None, ordering=None):
        '''Constructor.
            'ordering' is a class that specifies the order in which to provide
            slots. The default is arbitrary.
        '''
        
        self.name     = name
        self.priority = priority
        self.matrix   = {}
        

    def add_slot(self, new_slot):
    
        # Create a new entry for the slot's telescope, if not already present
        self.matrix.setdefault(new_slot.tel, [])

        # Don't add the slot if it clashes
        if self.slot_clashes(new_slot):
            return False

        # There's no clash - add the slot
        self.matrix[new_slot.tel].append(new_slot)
        return True


    def slot_clashes(self, new_slot):
        '''Return true if the proposed slot clashes with an existing slot.'''

        # Check for a clash with each existing slot in turn
        for old_slot in self.matrix[new_slot.tel]:
            if old_slot.clashes_with(new_slot):
                return True
            

        # If no existing slots clashed, then there is space for this new slot
        return False



    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)


    def __str__(self):
        string = ''
        string = string + 'Target: %s\n' % self.name
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
