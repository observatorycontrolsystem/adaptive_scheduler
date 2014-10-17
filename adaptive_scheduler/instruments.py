#!/usr/bin/env python

'''
instruments.py - Map generic instrument names to specific camera details

description

Author: Eric Saunders
November 2012
'''


import os, sys
PROJECT_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..')

instrument_file = os.path.join(PROJECT_PATH, 'camera_mappings.dat')

instrument_fh = open(instrument_file, 'r')

for line in instrument_fh:
    if line.startswith('#'):
        continue
    fields = line.split()

    location = "%s.%s.%s" % (fields[2]
