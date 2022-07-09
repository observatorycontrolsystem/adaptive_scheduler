#!/usr/bin/env python
from __future__ import division

from mock import patch

from adaptive_scheduler.eventbus import get_eventbus
from adaptive_scheduler.feedback import UserFeedbackLogger


class TestUserFeedbackLogger(object):

    def setup(self):
        self.timestamp = 'time'
        self.originator = 'me'
        self.msg = 'I am the msg'
        self.tag = 'OutOfDonutsError'
        self.request_group_id = 5

    @patch('adaptive_scheduler.feedback.rg_log.info')
    def test_userfeedbacklogger(self, mock_func):
        event_bus = get_eventbus()
        listener = UserFeedbackLogger()
        event_bus.add_listener(listener)

        event = UserFeedbackLogger.create_event(
            timestamp=self.timestamp,
            originator=self.originator,
            msg=self.msg,
            tag=self.tag,
            request_group_id=self.request_group_id
        )

        event_bus.fire_event(event)

        mock_func.assert_called_with(str(event), event.request_group_id)

    def test_events_have_equality(self):
        event1 = UserFeedbackLogger.create_event(
            timestamp=self.timestamp,
            originator=self.originator,
            msg=self.msg,
            tag=self.tag,
            request_group_id=self.request_group_id
        )
        event2 = UserFeedbackLogger.create_event(
            timestamp=self.timestamp,
            originator=self.originator,
            msg=self.msg,
            tag=self.tag,
            request_group_id=self.request_group_id
        )

        assert event1 == event2
