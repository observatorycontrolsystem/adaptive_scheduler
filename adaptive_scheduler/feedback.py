#!/usr/bin/env python

'''
feedback.py - User feedback on scheduling decisions.

description

Author: Eric Saunders
October 2013
'''

from adaptive_scheduler.eventbus import BaseListener, Event
from adaptive_scheduler.utils    import EqualityMixin
from adaptive_scheduler.log      import UserRequestLogger
import logging

multi_ur_log = logging.getLogger('ur_logger')
ur_log = UserRequestLogger(multi_ur_log)

class UserFeedbackLogger(BaseListener):

    def __init__(self):
        pass

    @classmethod
    def create_event(cls, timestamp, originator, msg, tag, tracking_number):
        return cls._Event(timestamp, originator, msg, tag, tracking_number)


    def on_update(self, event):
        logged_msg = str(event)
        ur_log.info(logged_msg, event.tracking_number)


    class _Event(Event, EqualityMixin):
        def __init__(self, timestamp, originator, msg, tag, tracking_number):
            self.timestamp       = timestamp
            self.originator      = originator
            self.msg             = msg
            self.tag             = tag
            self.tracking_number = tracking_number


        def dispatch(self, listener):
            listener.on_update(self)


        def __repr__(self):
            return "%s <%s [%s] %s>" % ('UserFeedbackEvent', self.timestamp, self.tag, self.msg)
