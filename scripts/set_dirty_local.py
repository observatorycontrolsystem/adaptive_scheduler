#!/usr/bin/env python

'''
set_dirty_local.py - Set the Request DB to 'dirty' to force a schedule recompute.

Simple script to mark the Request DB as dirty, on a local dev box.

Author: Eric Saunders
July 2013
'''

from reqdb.client import SchedulerClient

sc = SchedulerClient('http://localhost:8001/')
sc.set_dirty_flag()

