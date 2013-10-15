#!/usr/bin/env python

'''
set_path.py - Hack the path to allow the examples to run transparently.

Author: Eric Saunders
November 2012
'''
import os, sys
PROJECT_PATH = os.path.abspath(os.path.dirname(__file__)) + '/..'
sys.path.append(PROJECT_PATH)
