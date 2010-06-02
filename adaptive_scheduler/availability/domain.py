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
        self.tel        = tel
        self.start_time = start_time
        self.end_time   = end_time
        self.metadata   = kwargs



class Availability(object):
    
    def __init__(self, name, priority=None, ordering=None):
        '''Constructor.
            'ordering' is a class that specifies the order in which to provide
            slots. The default is arbitrary.
        '''
        
        self.name     = name
        self.priority = priority
        self.matrix = {}
        

    def add_slot(self, slot):
        self.matrix.setdefault(slot.tel, [])
        self.matrix[slot.tel].append(slot) 


    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)


    def __str__(self):
        string = ''
        string = string + 'Target: %s\n' % self.name
        string = string + 'Priority: %d\n' % self.priority
        for tel in sorted(self.matrix):
            string = string + "%s\n" % tel
            
            for slot in self.matrix[tel]:
                string = string + "    %s -> %s \n" % (slot.start_time, 
                                                       slot.end_time)

        return string
