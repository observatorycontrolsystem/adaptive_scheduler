#!/usr/bin/env python

'''
feedback.py - User feedback on scheduling decisions.

description

Author: Eric Saunders
October 2013
'''


class UserFeedbackEvent(object):
    def __init__(self, timestamp, originator, msg, tag):
        self.timestamp  = timestamp
        self.originator = originator
        self.msg        = msg
        self.tag        = tag

    def __repr__(self):
        return "%s <%s [%s] %s>" % (self.__class__.__name__, self.timestamp, self.tag, self.msg)
