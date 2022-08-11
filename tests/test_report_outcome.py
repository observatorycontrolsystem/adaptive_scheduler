#!/usr/bin/env python
'''
test_report_outcome.py - Tests for the event_utils module report_scheduling_outcome.

description

Author: Eric Saunders
August 2013
'''

from adaptive_scheduler.event_utils import report_scheduling_outcome
from adaptive_scheduler.kernel.reservation import CompoundReservation
from adaptive_scheduler.models import RequestGroup

from mock import patch


class TestReportOutcome(object):

    def setup(self):
        pass

    @patch.object(RequestGroup, 'emit_request_group_feedback')
    def test_report_scheduling_outcome(self, mock_emit):
        class Request(object):
            def __init__(self, request_id):
                self.id = request_id

        class Reservation(object):
            def __init__(self, request, request_group_id):
                self.request = request
                self.request_group_id = request_group_id

        res1 = Reservation(Request(1), 10)
        res2 = Reservation(Request(2), 20)
        reservation_list1 = [res1, res2]
        compound_reservation1 = CompoundReservation(reservation_list1, cr_type='and')
        to_schedule = [compound_reservation1]

        scheduled_reservations = [res2]

        report_scheduling_outcome(to_schedule, scheduled_reservations)

        mock_emit.assert_any_call(10, 'This Request (request id=1) was not scheduled (it clashed)',
                                                                 'WasNotScheduled')
        mock_emit.assert_any_call(20, 'This Request (request id=2) was scheduled', 'WasScheduled')
