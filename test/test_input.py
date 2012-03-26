#!/usr/bin/python
from __future__ import division

from nose.tools import assert_equal

from adaptive_scheduler.model import Telescope
from adaptive_scheduler.input import RequestProcessor



class TestRequestProcessor(object):

    def setup(self):
        self.telescopes = {
                            'maui' : Telescope(
                                                name      = 'maui',
                                                tel_class = '2m0',
                                                latitude  = 20.7069444444,
                                                longitude = -156.258055556,
                                              ),
                            'siding spring' : Telescope(
                                                name      = 'siding spring',
                                                tel_class = '2m0',
                                                latitude  = -31.273,
                                                longitude = 149.070593,
                                              ),
                            '1m0a.doma.bpl' : Telescope(
                                                name      = '1m0a.doma.bpl',
                                                tel_class = '1m0',
                                                latitude  = 34.433157,
                                                longitude = -119.86308,
                                              )
                            }


    def test_can_construct_class_mappings(self):

        rp = RequestProcessor()
        rp.set_telescope_class_mappings(self.telescopes)

        expected = {
                     '2m0' : [
                                 Telescope(
                                            name      = 'maui',
                                            tel_class = '2m0',
                                            latitude  = 20.7069444444,
                                            longitude = -156.258055556,
                                          ),
                                 Telescope(
                                            name      = 'siding spring',
                                            tel_class = '2m0',
                                            latitude  = -31.273,
                                            longitude = 149.070593,
                                          ),
                             ],
                     '1m0' : [
                                 Telescope(
                                            name      = '1m0a.doma.bpl',
                                            tel_class = '1m0',
                                            latitude  = 34.433157,
                                            longitude = -119.86308,
                                          )
                             ]
                    }

        assert_equal(rp.telescope_classes, expected)
