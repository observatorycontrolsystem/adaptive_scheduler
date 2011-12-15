#!/usr/bin/python
from __future__ import division

from nose.tools import raises
from datetime import datetime

# Import the modules to test
from adaptive_scheduler.model      import Request, CompoundRequest
from adaptive_scheduler.exceptions import InvalidRequestError


class TestRequest(object):
    '''Unit tests for the adaptive scheduler request object.'''

    def setup(self):
        self.target = {
                        'name'  : 'deneb',
                        'ra'    : '20 41 25.91',
                        'dec'   : '+45 16 49.22',
                        'epoch' : 2000,
                      }

        self.telescope = {
                           'name'      : 'maui',
                           'latitude'  : 20.7069444444,
                           'longitude' : -156.258055556,
                         }

        self.priority = 1
        self.duration = 60
        self.semester_start = datetime(2011, 11, 1, 0, 0, 0)
        self.semester_end   = datetime(2011, 11, 8, 0, 0, 0)


    @raises(InvalidRequestError)
    def test_invalid_request_type_raises_exception(self):
        junk_res_type = 'chocolate'
        windows = [self.semester_start, self.semester_end]
        request = Request(self.target, self.telescope, self.priority, self.duration)
        compound_request = CompoundRequest([request], junk_res_type, windows)


    def test_valid_request_type_does_not_raise_exception(self):
        valid_res_type = 'and'
        windows = [self.semester_start, self.semester_end]
        request = Request(self.target, self.telescope, self.priority, self.duration)
        compound_request = CompoundRequest([request], valid_res_type, windows)


    @raises(InvalidRequestError)
    def test_odd_number_of_window_bounds_raises_exception(self):
        res_type = 'and'
        windows = [self.semester_start, self.semester_end, self.semester_end]
        request = Request(self.target, self.telescope, self.priority, self.duration)
        compound_request = CompoundRequest([request], res_type, windows)
