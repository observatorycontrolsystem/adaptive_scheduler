#!/usr/bin/env python

'''
Clustering class for clustering and ordering reservations

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
November 2011
May 2012 -- changed so higher priority results in lower order (evaluated earlier)
TODO: 
* fix clustering by priority so that hi prio -> low order
* add clustering by request duration

'''

from reservation_v2 import *
import sys
import math
import heapq

class Clustering(object):

    def __init__(self, reservation_list):
        self.reservation_list = reservation_list


    def cluster_and_order(self, n=3):
        # must return number of clusters
        number_of_clusters = self.cut_into_n_by_priority(n)
        return number_of_clusters


    def max_priority(self):
        priority = 0
        for r in self.reservation_list:
            if r.priority > priority:
                priority = r.priority
        return priority
        

    def min_priority(self):
        priority = sys.maxint
        for r in self.reservation_list:
            if r.priority < priority:
                priority = r.priority
        return priority
        

    def cut_into_n_by_priority(self, n):
        ''' Higher priority should translate to lower order'''
        if len(self.reservation_list) < n:
            print "ERROR: fewer elements in the list (%d) than requested clusters (%d)\n" % (len(self.reservation_list), n)
            return
        max_p    = self.max_priority()
        min_p    = self.min_priority()
        interval = math.ceil((float(max_p - min_p + 1))/float(n))
#         for r in self.reservation_list:
#             for i in range(1,n):
#                 if r.priority >= min_p + i*interval:
#                     r.order = i+1
        for r in self.reservation_list:
            r.order = 1
            for i in range(1,n):
                if r.priority <= max_p - i*interval:
                    r.order = i+1
        return n


    def cluster_into_n_by_priority(self, n):
        ''' TODO: fix this so that it orders by higher prio -> lower order '''
        # remember to fix test
        if len(self.reservation_list) < n:
            print "error: fewer elements in the list than requested clusters\n"
            return
        distances = []
        self.reservation_list.sort()
        # calculate distances between consecutive (in order of prio.) res's
        for i in range(0,n-1):
            distances.append(self.reservation_list[i+1].priority - 
                             self.reservation_list[i].priority)
        # find cut-points
        cutpoints = heapq.nlargest(n, distances)
        order    = 1
        previous = 1
        for cp in cutpoints:
            i = distances.index(cp)
            distances[i] = 0
            # fix the order of reservations to the left of i+1
            for j in range(previous, i+1):
                self.reservation_list[j].order = order
            order   += 1
            previous = i + 1
        # fix the order of the rightmost cluster of reservations
        for j in range(previous, len(self.reservation_list)):
            self.reservation_list[j].order = order
        return n
