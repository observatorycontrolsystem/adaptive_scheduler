#!/usr/bin/env python

from __future__ import division

from nose.tools import assert_equal, assert_not_equal, raises
from mock       import patch, Mock

from adaptive_scheduler.eventbus import get_eventbus
from adaptive_scheduler.feedback import UserFeedbackLogger

class TestUserFeedbackLogger(object):

    def setup(self):
        self.timestamp='time'
        self.originator='me'
        self.msg='I am the msg'
        self.tag='OutOfDonutsError'
        self.tracking_number='0000000005'


    @patch('adaptive_scheduler.feedback.ur_log.info')
    def test_userfeedbacklogger(self, mock_func):
        event_bus = get_eventbus()
        listener  = UserFeedbackLogger()
        event_bus.add_listener(listener)

        event = UserFeedbackLogger.create_event(
                                                 timestamp=self.timestamp,
                                                 originator=self.originator,
                                                 msg=self.msg,
                                                 tag=self.tag,
                                                 tracking_number=self.tracking_number
                                                )

        event_bus.fire_event(event)

        mock_func.assert_called_with(str(event), event.tracking_number)


    def test_events_have_equality(self):
        event1 = UserFeedbackLogger.create_event(
                                                  timestamp=self.timestamp,
                                                  originator=self.originator,
                                                  msg=self.msg,
                                                  tag=self.tag,
                                                  tracking_number=self.tracking_number
                                                 )
        event2 = UserFeedbackLogger.create_event(
                                                  timestamp=self.timestamp,
                                                  originator=self.originator,
                                                  msg=self.msg,
                                                  tag=self.tag,
                                                  tracking_number=self.tracking_number
                                                 )

        assert_equal(event1, event2)
