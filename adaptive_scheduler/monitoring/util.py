'''
util.py - Utility module.

description

Author: Martin Norbury
May 2013
'''
from functools import wraps
import datetime as dt
import requests
import logging
import socket
import collections

DATE_FORMATTER = '%Y-%m-%d %H:%M:%S'
ES_ENDPOINT = 'http://es1.lco.gtn:9200/telescope_events/document/'
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


def construct_event_dict(telescope_name, event):
    split_name = telescope_name.split('.')
    event_dict = {'type': event.type.replace(' ', '_'),
                  'reason': event.reason,
                  'start_time': event.start_time.strftime(DATE_FORMATTER),
                  'end_time': event.end_time.strftime(DATE_FORMATTER),
                  'name': telescope_name,
                  'telescope': split_name[0],
                  'enclosure': split_name[1],
                  'site': split_name[2],
                  'timestamp': dt.datetime.utcnow().strftime(DATE_FORMATTER),
                  'hostname': socket.gethostname()}

    return event_dict


def construct_available_event_dict(telescope_name):
    event = collections.namedtuple('Event',['type', 'reason', 'start_time', 'end_time'])(type= 'AVAILABLE',
                  reason= 'Available for scheduling',
                  start_time= dt.datetime.utcnow(),
                  end_time= dt.datetime.utcnow())

    return construct_event_dict(telescope_name, event)


def send_event_to_es(event_dict):
    try:
        requests.post(ES_ENDPOINT + event_dict['name'] + '_' + event_dict['type'] + '_' + event_dict['scheduler_time'],
                  json=event_dict).raise_for_status()
    except Exception as e:
        log.error('Exception storing telescope status event in elasticsearch: {}'.format(repr(e)))
