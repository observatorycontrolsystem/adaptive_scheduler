#!/usr/bin/python
from __future__ import division

from nose.tools import raises
from datetime import datetime

# Import the modules to test
from adaptive_scheduler.model      import (Target, Telescope, Proposal, Molecule,
                                           Request, CompoundRequest)
from adaptive_scheduler.exceptions import InvalidRequestError


class TestRequest(object):
    '''Unit tests for the adaptive scheduler request object.'''

    def setup(self):
        self.target = Target(
                              name  = 'deneb',
                              ra    = '20 41 25.91',
                              dec   = '+45 16 49.22',
                              epoch = 2000,
                             )

        self.telescope = Telescope(
                                    name      = 'maui',
                                    latitude  = 20.7069444444,
                                    longitude = -156.258055556,
                                  )

        self.proposal = Proposal(
                                  proposal_name  = 'Scheduler Testing',
                                  user           = 'Eric Saunders',
                                  tag            = 'admin',
                                  time_remaining = 10,               # In hours
                                  priority       = 1
                                )

        self.molecule = Molecule(
                                  name            = 'expose_n default',
                                  type            = 'expose_n',
                                  count           = 1,
                                  binning         = 2,
                                  instrument_name = 'KB12',
                                  filter          = 'BSSL-UX-020'
                                )

        self.semester_start = datetime(2011, 11, 1, 0, 0, 0)
        self.semester_end   = datetime(2011, 11, 8, 0, 0, 0)
        self.windows = [self.semester_start, self.semester_end]

        self.duration = 60


    @raises(InvalidRequestError)
    def test_invalid_request_type_raises_exception(self):
        junk_res_type = 'chocolate'
        request = Request(self.target, self.telescope, self.molecule, self.windows,
                          self.duration)
        compound_request = CompoundRequest(junk_res_type, self.proposal, [request])


    def test_valid_request_type_does_not_raise_exception(self):
        valid_res_type = 'and'
        request = Request(self.target, self.telescope, self.molecule, self.windows,
                          self.duration)
        compound_request = CompoundRequest(valid_res_type, self.proposal, [request])
