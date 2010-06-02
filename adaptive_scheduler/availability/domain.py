'''domain.py - when and where an observation may be scheduled.

This package provides the Availability() class, which stores when and at which
telescopes a single observation may be scheduled. It is thus a general 
constraint map.

Author: Eric Saunders (esaunders@lcogt.net)

May 2010
'''

# Required for true (non-integer) division
from __future__ import division     



class Slot(object):

    def __init__(self, tel, start_time, end_time, **kwargs):
        self.tel      = tel
        self.start    = start_time
        self.end      = end_time
        self.metadata = kwargs



class Availability(object):
    
    def __init__(self, name, priority=None, ordering=None):
        '''Constructor.
            'ordering' is a class that specifies the order in which to provide
            slots. The default is arbitrary.
        '''
        
        self.name     = name
        self.priority = priority
        self.matrix   = {}
        

    def add_slot(self, slot):
        self.matrix.setdefault(slot.tel, [])
        self.matrix[slot.tel].append(slot) 


    def has_space_for(self, new_slot):

        # TODO: Deal with reverse case - new slot bigger than existing slots

        # Check for a clash with each existing slot in turn
        for old_slot in self.matrix[new_slot.tel]:
            # If a new slot time falls between the old slot boundaries
            if ( ( 
                    ( new_slot.start > old_slot.start )
                      and
                    ( new_slot.start < old_slot.end ) 
                  )
                  or
                    ( new_slot.end > old_slot.start )
                      and
                    ( new_slot.end < old_slot.end)            
                ):
                # They overlap - so the new slot clashes
                return False
                

        # If no existing slots clashed, then there is space for this new slot
        return True


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
