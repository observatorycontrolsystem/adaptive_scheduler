#!/usr/bin/env python

'''
test_orchestrator.py - Tests for the orchestrator module.

description

Author: Eric Saunders
August 2013
'''

from adaptive_scheduler.orchestrator import (report_scheduling_outcome,
    combine_excluded_intervals, compute_optimal_combination, construct_value_function_dict, \
    preempt_running_blocks)
from adaptive_scheduler.model2       import Telescope
from adaptive_scheduler.kernel.reservation_v3 import CompoundReservation_v2 as CompoundReservation
from adaptive_scheduler.scheduler import update_network_model

from nose.tools import assert_equal
import mock
from datetime import datetime, timedelta
from adaptive_scheduler.kernel.intervals import Intervals
from adaptive_scheduler.kernel.timepoint import Timepoint
from mock import Mock, patch
import lcogtpond


class TestOrchestrator(object):

    def setup(self):
        pass


    def test_update_network_model_no_events(self):
        events = {}
        tels = {
                 '1m0a.doma.lsc' : Telescope(),
                 '1m0a.doma.coj' : Telescope(),
               }

        update_network_model(tels, events)

        assert_equal(tels['1m0a.doma.lsc'].events, [])
        assert_equal(tels['1m0a.doma.coj'].events, [])


    def test_update_network_model_one_event(self):
        events = {
                   '1m0a.doma.lsc' : ['event1', 'event2'],
                 }
        tels = {
                 '1m0a.doma.lsc' : Telescope(),
                 '1m0a.doma.coj' : Telescope(),
               }

        update_network_model(tels, events)

        assert_equal(tels['1m0a.doma.lsc'].events, ['event1', 'event2'])
        assert_equal(tels['1m0a.doma.coj'].events, [])



    def test_report_scheduling_outcome(self):

        class Request(object):
            def __init__(self, request_number):
                self.request_number = request_number

        class Reservation(object):
            def __init__(self, request, mock_cr):
                self.request = request
                self.compound_request = mock_cr


        mock_cr1 = mock.Mock()
        mock_cr2 = mock.Mock()

        res1 = Reservation(Request('0000000001'), mock_cr1)
        res2 = Reservation(Request('0000000002'), mock_cr2)
        reservation_list1 = [res1, res2]
        compound_reservation1 = CompoundReservation(reservation_list1, type='and')
        to_schedule = [compound_reservation1]

        scheduled_reservations = [res2]

        report_scheduling_outcome(to_schedule, scheduled_reservations)

        mock_cr1.emit_user_feedback.assert_called_with('This Request (request number=0000000001) was not scheduled (it clashed)', 'WasNotScheduled')
        mock_cr2.emit_user_feedback.assert_called_with('This Request (request number=0000000002) was scheduled', 'WasScheduled')

    def test_combine_running_and_too_requests(self):
        start = datetime(2012, 1, 1, 0, 0, 0)
        end = datetime(2012, 1, 2, 0, 0, 0)
        running = {
                   '0m4a.aqwb.coj' : Intervals([Timepoint(start, 'start'), Timepoint(end, 'end')])
                  }
        too = {
               '0m4a.aqwb.coj' : Intervals([Timepoint(start + timedelta(seconds=10), 'start'), Timepoint(end + timedelta(seconds=10), 'end')])
               }
        combined = combine_excluded_intervals(running, too)

        expected = {
                    '0m4a.aqwb.coj' : Intervals([Timepoint(start, 'start'), Timepoint(end + timedelta(seconds=10), 'end')])
                    }

        assert_equal(expected, combined)
        
    def test_optimal_schedule(self):
        telescope_request_dict = {
                                  ('tel1', 1) : 6,
                                  ('tel1', 2) : 7,
                                  ('tel2', 1) : 8,
                                  ('tel2', 2) : 10
                                  }

        requests = [1, 2];
        telescopes = ['tel1', 'tel2']

        combinations = compute_optimal_combination(telescope_request_dict, requests, telescopes)

        expected_combinations = [('tel1', 1), ('tel2', 2)]

        assert_equal(combinations, expected_combinations)


    def test_optimal_schedule_more_telescopes(self):
        telescope_request_dict = {
                                  ('tel1', 1) : 6,
                                  ('tel1', 2) : 7,
                                  ('tel2', 1) : 8,
                                  ('tel2', 2) : 10,
                                  ('tel3', 1) : 12,
                                  ('tel3', 2) : 14
                                  }

        requests = [1, 2];
        telescopes = ['tel1', 'tel2', 'tel3']

        combinations = compute_optimal_combination(telescope_request_dict, requests, telescopes)

        expected_combinations = [('tel2', 1), ('tel3', 2)]

        assert_equal(combinations, expected_combinations)

    def test_constructing_value_matrix(self):
        # tel2 is not used
        tels = ['tel1', 'tel2']
        
        too_ur1 = self.create_mock_too_ur(1, 20, tels)
        too_ur2 = self.create_mock_too_ur(2, 100, tels)

        too_urs = [too_ur1, too_ur2]

        normal_ur1 = self.create_mock_too_ur(30, 10)
        normal_urs = [normal_ur1]


        block = Mock()
        block.get_tracking_number_set.return_value = [normal_ur1.tracking_number]


        telescope_to_running_blocks = {
                                       'tel1' : [block]
                                       }

        matrix = construct_value_function_dict(too_urs, normal_urs, tels, telescope_to_running_blocks)

        expected = {
                    ('tel1', 1) : 2,
                    ('tel1', 2) : 10,
                    ('tel2', 1) : 20,
                    ('tel2', 2) : 100,
                    }

        assert_equal(matrix, expected)

    def configure_mock_get(self, func_mock, cutoff_dt, fake_block_dict):
        
        def mapping(**kwargs):
            mock_schedule = Mock(spec=lcogtpond.schedule.Schedule)
            mock_schedule.blocks = fake_block_dict[kwargs.get('site')]
            mock_schedule.end_of_overlap.return_value = cutoff_dt
            return mock_schedule
        
        func_mock.side_effect = mapping

        return

    @patch('adaptive_scheduler.orchestrator.cancel_schedule')
    @patch('lcogtpond.schedule.Schedule.get')
    def test_preempt_running_blocks(self, mocked_get, mocked_cancel_schedule):
        now = 'now'
        semester_end = 'end'
        dry_run = 'run'
        estimated_scheduler_end = "end"

        tel_mock = Mock()
        tel_mock.events = []
        tels = {
                '1m0a.doma.tel1' : tel_mock,
                '1m0a.doma.tel2' : tel_mock,
                '1m0a.doma.tel3' : tel_mock
                }

        too_ur1 = self.create_mock_too_ur(1, 20, tels)
        too_ur2 = self.create_mock_too_ur(2, 100, tels)
        too_ur3 = self.create_mock_too_ur(3, 100, tels)

        too_urs = [too_ur1, too_ur2]
        all_too_urs = [too_ur1, too_ur2, too_ur3]

        normal_ur1 = self.create_mock_too_ur(30, 10)
        normal_urs = [normal_ur1]

        block = Mock()
        block.get_tracking_number_set.return_value = [normal_ur1.tracking_number]

        too_block = Mock()
        too_block.get_tracking_number_set.return_value = [too_ur3.tracking_number]

        cutoff_dt = datetime(2013, 8, 18, 0, 0, 0)

        # tel2 is not used
        fake_block_list = {
                           'tel1' : [block],
                           'tel2' : [],
                           'tel3' : [too_block]
                           }
        self.configure_mock_get(mocked_get, cutoff_dt, fake_block_list)

        ends_after = datetime(2013, 8, 18, 0, 0, 0)
        running_if_starts_before = datetime(2013, 8, 18, 0, 0, 0)
        starts_before = datetime(2013, 8, 18, 0, 0, 0)

        preempt_running_blocks(too_urs, all_too_urs, normal_urs, tels, now, semester_end, estimated_scheduler_end, dry_run)

        tels_to_cancel = ['1m0a.doma.tel1']
        mocked_cancel_schedule.assert_called_with(tels_to_cancel, now, semester_end, dry_run)

    @patch('adaptive_scheduler.orchestrator.cancel_schedule')
    @patch('lcogtpond.schedule.Schedule.get')
    def test_preempt_running_blocks_no_preemption(self, mocked_get, mocked_cancel_schedule):
        now = 'now'
        semester_end = 'end'
        dry_run = 'run'
        estimated_scheduler_end = "end"

        # tel2, tel3 is not used
        tel_mock = Mock()
        tel_mock.events = []
        tels = {
                '1m0a.doma.tel1' : tel_mock,
                '1m0a.doma.tel2' : tel_mock,
                '1m0a.doma.tel3' : tel_mock
                }

        too_ur1 = self.create_mock_too_ur(1, 20, tels)
        too_ur2 = self.create_mock_too_ur(2, 100, tels)

        too_urs = [too_ur1, too_ur2]
        all_too_urs = [too_ur1, too_ur2]

        normal_ur1 = self.create_mock_too_ur(30, 10)
        normal_urs = [normal_ur1]

        block = Mock()
        block.get_tracking_number_set.return_value = [normal_ur1.tracking_number]

        cutoff_dt = datetime(2013, 8, 18, 0, 0, 0)

        # tel2 is not used
        fake_block_list = {
                           'tel1' : [block],
                           'tel2' : [],
                           'tel3' : []
                           }
        self.configure_mock_get(mocked_get, cutoff_dt, fake_block_list)

        ends_after = datetime(2013, 8, 18, 0, 0, 0)
        running_if_starts_before = datetime(2013, 8, 18, 0, 0, 0)
        starts_before = datetime(2013, 8, 18, 0, 0, 0)

        preempt_running_blocks(too_urs, all_too_urs, normal_urs, tels, now, semester_end, estimated_scheduler_end, dry_run)

        tels_to_cancel = []
        mocked_cancel_schedule.assert_called_with(tels_to_cancel, now, semester_end, dry_run)

    def create_mock_too_ur(self, tracking_number, priority, tels=[]):
        too_ur = Mock()
        too_ur.tracking_number = tracking_number
        too_ur.get_priority.return_value = priority
        
        request = Mock()
        too_ur.requests = [request]
        too_ur.n_requests = 1
        request.windows = []
        
        for tel in tels:
            window = Mock()
            window.resource = tel
            request.windows.append(window)
        
        return too_ur
