''' scheduler/log.py - Module for wrapping scheduler logging.

This module is used for configuring and returning named loggers.

Author: Martin Norbury (mnorbury@lcogt.net)
August 2012
'''
import logging

# Default logging format
_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=_FORMAT)

# TODO: Hack to overcome logging configuration in rise_set
class _FakeLogger(object):
    def __init__(self, name):
        self.name = name
    def info(self, msg):
        import datetime
        now = datetime.datetime.now()
        print '%s %s INFO %s' % ( now, self.name, msg )

def create_logger(name):
    ''' Create a named logger instance. '''
    #logger = logging.getLogger(name)
    #logger.setLevel(logging.DEBUG)
    #return logger
    return _FakeLogger(name)
