#!/usr/bin/env python
'''
test_report_outcome.py - Tests for the event_utils module report_scheduling_outcome.

description

Author: Eric Saunders
August 2013
'''

from adaptive_scheduler.event_utils import report_scheduling_outcome
from adaptive_scheduler.kernel.reservation_v3 import CompoundReservation_v2 as CompoundReservation

from mock import Mock


class TestReportOutcome(object):

    def setup(self):
        pass

    def test_report_scheduling_outcome(self):
        class Request(object):
            def __init__(self, request_id):
                self.id = request_id

        class Reservation(object):
            def __init__(self, request, mock_rg):
                self.request = request
                self.request_group = mock_rg

        mock_cr1 = Mock()
        mock_cr2 = Mock()

        res1 = Reservation(Request(1), mock_cr1)
        res2 = Reservation(Request(2), mock_cr2)
        reservation_list1 = [res1, res2]
        compound_reservation1 = CompoundReservation(reservation_list1, type='and')
        to_schedule = [compound_reservation1]

        scheduled_reservations = [res2]

        report_scheduling_outcome(to_schedule, scheduled_reservations)

        mock_cr1.emit_rg_feedback.assert_called_with('This Request (request id=1) was not scheduled (it clashed)',
                                                     'WasNotScheduled')
        mock_cr2.emit_rg_feedback.assert_called_with('This Request (request id=2) was scheduled', 'WasScheduled')
