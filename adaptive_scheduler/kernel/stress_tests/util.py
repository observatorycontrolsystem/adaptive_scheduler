#!/usr/bin/env python                                                             

'''                                               
util.py
Author: Sotiria Lampoudi    
May 2012
'''


from adaptive_scheduler.kernel.timepoint import *
from adaptive_scheduler.kernel.reservation_v2 import *

class Util(object):
   
   def get_coverage_count_plot(self, schedule):
      for resource in schedule.keys():
         print resource
         all_tps = []
         for r in schedule[resource]:
            all_tps.extend(r.scheduled_timepoints)
         all_tps.sort()
         current_height = 0
         for tp in all_tps:
            if tp.type == 'start':
               print tp.time, ' ', current_height
               current_height += 1
               print tp.time, ' ', current_height
            if tp.type == 'end':
               print tp.time, ' ', current_height
               current_height -= 1
               print tp.time, ' ', current_height


   def find_overlaps(self, schedule):
      overlap_counts = []
      for resource in schedule.keys():
         tps = []
         overlap_count = 0
         for r in schedule[resource]:
            tps.extend(r.scheduled_timepoints)
         tps.sort()
         current_height = 0
         for tp in tps:
            if tp.type == 'start':
                current_height += 1
                if current_height > 1:
                   overlap_count += 1
	    if tp.type == 'end':
		current_height -= 1
         if overlap_count:
            print 'ERROR: schedule with overlap for resource ', resource
            print schedule[resource]
            overlap_counts.append(overlap_count)
      oc_sum = 0
      for i in overlap_counts:
         oc_sum += i
      return oc_sum
