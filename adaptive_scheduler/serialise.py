#!/usr/bin/env python

'''
serialise.py - Serialise scheduler objects

description

Author: Eric Saunders
June 2013
'''

import json

class ScheduleEncoder(json.JSONEncoder):
    def default(self, o):
        return repr(o)
