#!/usr/bin/env python

'''
Clustering class for clustering and ordering reservations

Author: Sotiria Lampoudi (slampoud@cs.ucsb.edu)
November 2011
May 2012 -- changed so higher priority results in lower order (evaluated earlier)
TODO: 
* add clustering by request duration
* add automatic cluster number detection
'''

from reservation_v3 import *
import sys
import math
import heapq

class Clustering(object):

    def __init__(self, reservation_list):
        self.reservation_list = reservation_list


    def cluster_and_order(self, n=3):
        # must return number of clusters
        #number_of_clusters = self.cut_into_n_by_priority(n)
        #number_of_clusters = self.cluster_into_n_by_priority(n)
        number_of_clusters = self.cluster_adaptively_by_what('priority')
#        number_of_clusters = self.cluster_adaptively_by_what('duration')
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
        ''' Higher priority translates to lower (earlier) order.
        Returns number of clusters. '''
        if len(self.reservation_list) < n:
            print "ERROR: fewer elements in the list (%d) than requested clusters (%d)\n" % (len(self.reservation_list), n)
            return
	if n == 1:
		return n
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
        return self.cluster_into_n_by_what(n, 'priority')
        

    def cluster_into_n_by_duration(self, n):
        return self.cluster_into_n_by_what(n, 'duration')


    def cluster_into_n_by_what(self, n, what):
        ''' Higher priority translates to lower (earlier) order.
        Longer duration translates to lower (earlier) order.
        Returns number of clusters. '''

        if len(self.reservation_list) < n:
            print "error: fewer elements in the list than requested clusters\n"
            return
        if n == 1:
            return n

        distances = []
        if what == 'priority':
            self.reservation_list.sort(key=lambda reservation: reservation.priority, reverse=True)
            # calculate distances between consecutive res's
            for i in range(0,len(self.reservation_list)-1):
                distances.append(self.reservation_list[i].priority - 
                                 self.reservation_list[i+1].priority)
        elif what == 'duration':
            # sort by duration
            self.reservation_list.sort(key=lambda reservation: reservation.duration, reverse=True)
            # calculate distances between consecutive res's
            for i in range(0,len(self.reservation_list)-1):
                distances.append(self.reservation_list[i].duration - 
                                 self.reservation_list[i+1].duration)

        # find cut-points
        cutpoints = heapq.nlargest(n-1, distances)
        # find cut-point indices
        cutpoints_idx = []
        for cp in cutpoints:
            i = distances.index(cp)
            cutpoints_idx.append(i)
            distances[i] = -1
        cutpoints_idx.sort()
        # walk through res's in prio order and set their order
        order    = 1
        previous = 0
        for i in cutpoints_idx:
            # fix the order of reservations to the left of i+1
            for j in range(previous, i+1):
                self.reservation_list[j].order = order
            order   += 1
            previous = i + 1
        # fix the order of the rightmost cluster of reservations
        for j in range(previous, len(self.reservation_list)):
            self.reservation_list[j].order = order
        return n


    def cluster_adaptively_by_what(self, what):
        ''' Higher priority translates to lower (earlier) order.
        Longer duration translates to lower (earlier) order.
        Returns number of clusters. '''
        
        distances = []
        if what == 'priority':
            self.reservation_list.sort(key=lambda reservation: reservation.priority, reverse=True)
            # calculate distances between consecutive res's
            for i in range(0,len(self.reservation_list)-1):
                distances.append(self.reservation_list[i].priority - 
                                 self.reservation_list[i+1].priority)
        elif what == 'duration':
            # sort by duration
            self.reservation_list.sort(key=lambda reservation: reservation.duration, reverse=True)
            # calculate distances between consecutive res's
            for i in range(0,len(self.reservation_list)-1):
                distances.append(self.reservation_list[i].duration - 
                                 self.reservation_list[i+1].duration)

        # find non-zero distances
        nzdcount = 0
        for i in range(len(distances)):
            if distances[i] > 0:
                nzdcount += 1
        # choose how many cutpoints
        num_cutpoints = min(nzdcount, int(len(self.reservation_list)/10))
        #num_cutpoints = nzdcount
        if num_cutpoints < 1:
            return 1
        # find cut-points
        cutpoints = heapq.nlargest(num_cutpoints, distances)

        # find cut-point indices -- this finds the first, if many
        # TODO: what we really want is uniform distribution of 
        # reservations, if using distance that appears many times
        cutpoints_idx = []
        for cp in cutpoints:
            i = distances.index(cp)
            cutpoints_idx.append(i)
            distances[i] = -1
        cutpoints_idx.sort()
        # walk through res's in prio order and set their order
        order    = 1
        previous = 0
        for i in cutpoints_idx:
            # fix the order of reservations to the left of i+1
            for j in range(previous, i+1):
                self.reservation_list[j].order = order
            order   += 1
            previous = i + 1
        # fix the order of the rightmost cluster of reservations
        for j in range(previous, len(self.reservation_list)):
            self.reservation_list[j].order = order
        return num_cutpoints
