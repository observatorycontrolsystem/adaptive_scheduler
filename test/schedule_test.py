#!/usr/bin/env python
from __future__ import division

from nose.tools import eq_, assert_equal, raises

# Import the module to test
from adaptive_scheduler.schedule import Schedule


class test_schedule(object):
    '''Unit tests for the Schedule, a description of what observations
       will be observed when, and on which telescope.'''
       
    def setup(self):
        self.schedule = Schedule()
    
    def teardown(self):
        pass
        
        
    
    def test_(self):
        pass
