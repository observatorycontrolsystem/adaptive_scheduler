'''
util.py - Utility module.

description

Author: Martin Norbury
May 2013
'''
from functools import wraps
import logging

log = logging.getLogger(__name__)

def debug(mylogger):
    ''' Decorator for printing debug message. '''
    def wrap(func):
        ''' Inner function wrapper. '''
        @wraps(func)
        def wrapper(*args,**kwargs):
            ''' Inner argument wrapper. '''
            mylogger.debug("Calling %s with args=%s and kwargs=%s" %
                          (func.__name__,args,kwargs))
            result = func(*args, **kwargs)
            mylogger.debug("Return from %s with result=%s" %
                          (func.__name__,result))
            return result
        return wrapper
    return wrap
