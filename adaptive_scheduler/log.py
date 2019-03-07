#!/usr/bin/env python

'''
logging.py - Scheduler-specific logging classes

This module provides
    * MultiFileHandler   - write to multiple files using a single logger
    * RequestGroupHandler - write information specific to RGs to individual files
    * RequestGroupLogger  - convenience wrapper for logging specific RGs

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


class RequestGroupHandler(MultiFileHandler):
    def __init__(self, request_group_id, mode='a', logdir = '.', encoding=None, delay=0):
        filename = os.path.join(logdir, str(int(request_group_id)) + '.log')
        MultiFileHandler.__init__(self, filename, mode, encoding, delay)
        self.logdir = logdir
        self.request_group_id  = request_group_id

    def emit(self, record):
        if hasattr(record, 'id'):
            record.file_id = os.path.join(self.logdir, str(int(record.id)) + '.log')
            record.tags = {'request_group_id': int(record.id)}  # For JSON tags
        MultiFileHandler.emit(self, record)


class RequestGroupLogger(object):

    def __init__(self, logger):
        self.logger = logger

    def debug(self, msg, request_group_id):
        self.logger.debug(msg, extra={'request_group_id': int(request_group_id)})

    def info(self, msg, request_group_id):
        self.logger.info(msg, extra={'request_group_id': int(request_group_id)})

    def warn(self, msg, request_group_id):
        self.logger.warn(msg, extra={'request_group_id': int(request_group_id)})

    def critical(self, msg, request_group_id):
        self.logger.critical(msg, extra={'request_group_id': int(request_group_id)})

    def error(self, msg, request_group_id):
        self.logger.error(msg, extra={'request_group_id': int(request_group_id)})


if __name__ == '__main__':

    logger = logging.getLogger('request_logger')
    logger.setLevel(logging.DEBUG)

    handler = RequestGroupHandler(request_group_id=24, mode='a')
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    # These messages are logged to the original UR tracking number
    logger.debug('debug message')
    logger.info('info message')

    # These messages are logged to a different UR tracking number
    logger.debug('debug message',       extra={'request_group_id':'300'})
    logger.info('info message',         extra={'request_group_id':'300'})
    logger.warn('warn message',         extra={'request_group_id':'300'})
    logger.error('error message',       extra={'request_group_id':'300'})
    logger.critical('critical message', extra={'request_group_id':'300'})

    rg_logger = RequestGroupLogger(logger)
    rg_logger.critical('critical message 2', '300')

