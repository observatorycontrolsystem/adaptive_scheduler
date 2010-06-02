'''domain.py - when and where an observation may be scheduled.

This package provides the Availability() class, which stores when and at which
telescopes a single observation may be scheduled. It is thus a general 
constraint map.

Author: Eric Saunders (esaunders@lcogt.net)

May 2010
'''

# Required for true (non-integer) division
from __future__ import division     



class Availability(object):
    
    def __init__(self, ordering=None):
        '''Constructor.
            'ordering' is a class that specifies the order in which to provide
            slots. The default is arbitrary.
        '''
        
        self.matrix = {}
        

    def add_slot(self, tel, start_time, end_time):
        self.matrix.setdefault(tel, [])
        self.matrix[tel].append((start_time, end_time)) 


    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)


    def __str__(self):
        string = ''
        for tel in sorted(self.matrix):
            string = string + "%s\n" % tel
            
            for start, end in self.matrix[tel]:
                string = string + "    %s -> %s \n" % (start, end)


        return string
