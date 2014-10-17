#!/usr/bin/env python

'''
logging.py - Scheduler-specific logging classes

This module provides
    * MultiFileHandler   - write to multiple files using a single logger
    * UserRequestHandler - write information specific to URs to individual files
    * UserRequestLogger  - convenience wrapper for logging specific URs

Author: Eric Saunders
April 2013
'''

import logging
import os.path


class MultiFileHandler(logging.FileHandler):

    def __init__(self, filename, mode, encoding=None, delay=0):
        logging.FileHandler.__init__(self, filename, mode, encoding, delay)

    def emit(self, record):
        if self.should_change_file(record):
            self.change_file(record.file_id)
        logging.FileHandler.emit(self, record)

    def should_change_file(self, record):
        if not hasattr(record, 'file_id') or record.file_id == self.baseFilename:
             return False
        return True

    def change_file(self, file_id):
        self.stream.close()

        self.baseFilename = file_id
        self.stream = self._open()


class UserRequestHandler(MultiFileHandler):
    def __init__(self, tracking_number, mode='a', logdir = '.', encoding=None, delay=0):
        filename = os.path.join(logdir, tracking_number + '.log')
        MultiFileHandler.__init__(self, filename, mode, encoding, delay)
        self.logdir = logdir
        self.tracking_number  = tracking_number

    def emit(self, record):
        if hasattr(record, 'tracking_number'):
            record.file_id = os.path.join(self.logdir, record.tracking_number + '.log')
        MultiFileHandler.emit(self, record)


class UserRequestLogger(object):

    def __init__(self, logger):
        self.logger = logger

    def debug(self, msg, tracking_number):
        self.logger.debug(msg, extra={'tracking_number':tracking_number})

    def info(self, msg, tracking_number):
        self.logger.info(msg, extra={'tracking_number':tracking_number})

    def warn(self, msg, tracking_number):
        self.logger.warn(msg, extra={'tracking_number':tracking_number})

    def critical(self, msg, tracking_number):
        self.logger.critical(msg, extra={'tracking_number':tracking_number})

    def error(self, msg, tracking_number):
        self.logger.error(msg, extra={'tracking_number':tracking_number})


if __name__ == '__main__':

    logger = logging.getLogger('request_logger')
    logger.setLevel(logging.DEBUG)

    handler = UserRequestHandler(tracking_number='0000000244', mode='a')
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    # These messages are logged to the original UR tracking number
    logger.debug('debug message')
    logger.info('info message')

    # These messages are logged to a different UR tracking number
    logger.debug('debug message',       extra={'tracking_number':'0000000300'})
    logger.info('info message',         extra={'tracking_number':'0000000300'})
    logger.warn('warn message',         extra={'tracking_number':'0000000300'})
    logger.error('error message',       extra={'tracking_number':'0000000300'})
    logger.critical('critical message', extra={'tracking_number':'0000000300'})

    ur_logger = UserRequestLogger(logger)
    ur_logger.critical('critical message 2', '0000000300')

