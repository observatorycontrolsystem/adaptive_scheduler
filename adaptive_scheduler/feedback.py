#!/usr/bin/env python

'''
feedback.py - User feedback on scheduling decisions.

description

Author: Eric Saunders
October 2013
'''

from adaptive_scheduler.eventbus import BaseListener, Event
from adaptive_scheduler.log      import UserRequestLogger
from adaptive_scheduler.utils    import EqualityMixin
import os.path
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



class TimingLogger(BaseListener):

    def __init__(self):
        pass

    @classmethod
    def create_start_event(cls, timestamp):
        return cls._StartEvent(timestamp)


    @classmethod
    def create_end_event(cls, timestamp):
        return cls._EndEvent(timestamp)


    def on_start(self, event):
        self.start = event.timestamp

        return


    def on_end(self, event):
        self.end = event.timestamp
        duration = self.end - self.start

        # 2013-12-01_timings.log
        out_filename = '%s-%s-%s_timings.log' % (self.start.year,
                                                 self.start.month,
                                                 self.start.day)

        if not os.path.exists(out_filename):
            with open(out_filename, 'w') as out_fh:
                header = '#Start Duration\n'
                out_fh.write(header)

        with open(out_filename, 'a') as out_fh:
            msg = '%s %s\n' % (self.start, duration.total_seconds())
            out_fh.write(msg)

        return


    class _StartEvent(Event, EqualityMixin):
        def __init__(self, timestamp):
            self.timestamp = timestamp

        def dispatch(self, listener):
            listener.on_start(self)

        def __repr__(self):
            return "%s <%s>" % ('StartEvent', self.timestamp)

    class _EndEvent(Event, EqualityMixin):
        def __init__(self, timestamp):
            self.timestamp = timestamp

        def dispatch(self, listener):
            listener.on_end(self)

        def __repr__(self):
            return "%s <%s>" % ('EndEvent', self.timestamp)
