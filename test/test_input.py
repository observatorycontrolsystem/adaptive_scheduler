#!/usr/bin/python
from __future__ import division

from nose.tools import assert_equal

from adaptive_scheduler.model import Telescope, Request, CompoundRequest
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

        self.rp = RequestProcessor()
        self.rp.set_telescope_class_mappings(self.telescopes)


    def test_can_construct_class_mappings(self):

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

        assert_equal(self.rp.telescope_classes, expected)


    def test_doesnt_expand_tel_names(self):
        junk = 'junk'
        tel_name = '1m0a.doma.bpl'
        requests = [
                     Request(
                              target         = junk,
                              telescope_name = tel_name,
                              molecule       = junk,
                              windows        = junk,
                              duration       = junk
                            )
                   ]
        compound_request = CompoundRequest(res_type='single',
                                           proposal=junk,
                                           requests=requests)

        expected = CompoundRequest(
               res_type='single',
               proposal=junk,
               requests= [
                           Request(
                                    target    = junk,
                                    telescope = self.telescopes[tel_name],
                                    molecule  = junk,
                                    windows   = junk,
                                    duration  = junk
                                  )
                         ]
               )

        self.rp.expand_tel_class(compound_request)

        assert_equal(compound_request, expected)


    def test_can_expand_single_req_to_one_tel(self):
        junk = 'junk'
        requests = [
                     Request(
                              target    = junk,
                              telescope_name = '1m0',
                              molecule  = junk,
                              windows   = junk,
                              duration  = junk
                            )
                   ]
        compound_request = CompoundRequest(res_type='single',
                                           proposal=junk,
                                           requests=requests)

        expected = CompoundRequest(
               res_type='single',
               proposal=junk,
               requests= [
                           Request(
                                    target    = junk,
                                    telescope = self.telescopes['1m0a.doma.bpl'],
                                    molecule  = junk,
                                    windows   = junk,
                                    duration  = junk
                                  )
                         ]
               )

        self.rp.expand_tel_class(compound_request)

        assert_equal(compound_request, expected)




    def test_can_expand_single_req_to_two_tels(self):
        junk = 'junk'
        requests = [
                     Request(
                              target    = junk,
                              telescope_name = '2m0',
                              molecule  = junk,
                              windows   = junk,
                              duration  = junk
                            )
                   ]
        compound_request = CompoundRequest(res_type='single',
                                           proposal=junk,
                                           requests=requests)

        expected = CompoundRequest(
               res_type='oneof',    # Note that the res_type should change
               proposal=junk,
               requests= [
                           Request(
                                    target    = junk,
                                    telescope = self.telescopes['maui'],
                                    molecule  = junk,
                                    windows   = junk,
                                    duration  = junk
                                  ),
                           Request(
                                    target    = junk,
                                    telescope = self.telescopes['siding spring'],
                                    molecule  = junk,
                                    windows   = junk,
                                    duration  = junk
                                  )
                         ]
               )

        self.rp.expand_tel_class(compound_request)

        assert_equal(compound_request, expected)
