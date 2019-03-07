#!/usr/bin/env python

'''
Tests logging features

Austin Riba
Sep 2015
'''

import logging
from adaptive_scheduler.log import RequestGroupLogger
import logger_config


class TestURLogging(object):
    '''
    The output of this test will be found in logs/0000000123.log
    Unfortunatly logcapture does not seem to work with the custom classes.
    '''
    def setup(self):
        self.multi_rg_log = logging.getLogger('rg_logger')
        self.rg_log = RequestGroupLogger(self.multi_rg_log)

    def test_logmessage(self):
        self.rg_log.info('info Message!', '0000000123')
        self.rg_log.warn('Warning, stuff is broke', '0000000123')
