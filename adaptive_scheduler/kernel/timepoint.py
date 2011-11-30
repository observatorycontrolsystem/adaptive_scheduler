#!/usr/bin/env python

'''
timepoint.py - Class for specifying generic timepoints.



Author: Sotiria Lampoudi
August 2011
'''

class Timepoint(object):
   def __init__(self, time, type):
      # type should be 'start' or 'end'
      self.time = time
      self.type = type


   def __lt__(self, other):
      if self.time == other.time:
         if self.type == other.type:
            return False
         elif self.type == 'end':
            return True
         else:
            return False
      else:
         return self.time < other.time
