#!/usr/bin/env python

'''
test_orchestrator.py - Tests for the orchestrator module.

description

Author: Eric Saunders
August 2013
'''

from adaptive_scheduler.orchestrator import update_telescope_network_status
from adaptive_scheduler.model2       import Telescope

from nose.tools import assert_equal
import mock




class TestOrchestrator(object):

    def setup(self):
        pass


    @mock.patch('adaptive_scheduler.orchestrator.network_status')
    def test_update_telescope_network_status_no_events(self, mock_network_status):
        mock_network_status.return_value = {}
        tels = {
                 '1m0a.doma.lsc' : Telescope(),
                 '1m0a.doma.coj' : Telescope(),
               }

        update_telescope_network_status(tels)

        assert_equal(tels['1m0a.doma.lsc'].events, [])
        assert_equal(tels['1m0a.doma.coj'].events, [])


    @mock.patch('adaptive_scheduler.orchestrator.network_status')
    def test_update_telescope_network_status_one_event(self, mock_network_status):
        mock_network_status.return_value = {
                                             '1m0a.doma.lsc' : ['event1', 'event2'],
                                           }
        tels = {
                 '1m0a.doma.lsc' : Telescope(),
                 '1m0a.doma.coj' : Telescope(),
               }

        update_telescope_network_status(tels)

        assert_equal(tels['1m0a.doma.lsc'].events, ['event1', 'event2'])
        assert_equal(tels['1m0a.doma.coj'].events, [])
