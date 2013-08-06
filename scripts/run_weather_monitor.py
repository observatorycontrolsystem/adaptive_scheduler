#!/usr/bin/env python

'''
app.py - Prototype network state harness

description

Author: Martin Norbury
May 2013
'''

import logging
logger = logging.getLogger(__name__)

import argparse

from adaptive_scheduler.monitoring.network_status import network_status

def parse_command_line():

    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-l",dest='log_level',type=str,default='INFO')
    result = parser.parse_args()

    # Configure logger
    log_level = getattr(logging,result.log_level.upper())
    handler   = logging.StreamHandler()
    formatter = '%(asctime)s.%(msecs).03d %(levelname)7s: %(module)15s: %(message)s'
    datefmt   = '%Y-%m-%d %H:%M:%S'
    logging.basicConfig(level=log_level,format=formatter,datefmt=datefmt)

if __name__ == '__main__':

    parse_command_line()

    logger.info("Starting test harness")
    events = network_status()
    for k,v in sorted(events.iteritems()):
        logger.info("%s %s" % (k,v))
