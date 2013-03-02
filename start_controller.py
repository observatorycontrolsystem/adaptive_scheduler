#!/usr/bin/env python
'''
start_controller - Run the main scheduling control thread

Author: Martin Norbury
August 2012
'''
from optparse import OptionParser

from adaptive_scheduler import log
from adaptive_scheduler import controller

# Module logger
logger = log.create_logger('main')

if __name__ == '__main__':
    logger.info("Starting application")

    # Parse the command line arguments
    parser = OptionParser(description="Scheduler controller")
    parser.add_option("-r", "--request_polling", dest="request_polling",
                      help="Request polling interval (s)",
                      default=10,
                      type="int")
    parser.add_option("-s", "--syncronization_interval", dest="syncronization_interval",
                      help="DB Syncronization interval (s)",
                      default=10,
                      type="int")
    options, args = parser.parse_args()

    # Run controller
    request_db_url = 'http://zwalker-linux:8000/'
    controller.run_controller(options.request_polling,
                              options.syncronization_interval,
                              request_db_url)

