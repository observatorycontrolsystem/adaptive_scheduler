#!/usr/bin/env python

'''
Clustering class for clustering and ordering reservations

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
November 2011
'''

from reservation_v2 import *
import sys
import math

class Clustering(object):

    def __init__(self, reservation_list):
        self.reservation_list = reservation_list


    def cluster_and_order(self, n=3):
        # must return number of clusters
        number_of_clusters = self.cluster_into_n_by_priority(n)
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
        

    def cluster_into_n_by_priority(self, n):
        if len(self.reservation_list) < n:
            print "error: fewer elements in the list than requested clusters\n"
            return
        max_p    = self.max_priority()
        min_p    = self.min_priority()
        interval = math.ceil((float(max_p - min_p + 1))/float(n))
        for r in self.reservation_list:
            for i in range(1,n):
                if r.priority >= min_p + i*interval:
                    r.order = i+1
        return n
