#!/usr/bin/env python
'''
as.py - Run the adaptive scheduler on continuous loop.

Example usage (testing)
    python as.py --requestdb='http://localhost:8001/'
    python as.py  -r http://localhost:8001/ -s 10 --dry-run --now '2013-10-01 00:00:00' --telescopes=telescopes-simulated_full_1m_network.dat

Example usage (production)
    python as.py --requestdb='http://scheduler-dev.lco.gtn/requestdb/'

Author: Eric Saunders
July 2012
'''
from __future__ import division


from adaptive_scheduler.eventbus         import get_eventbus
from adaptive_scheduler.feedback         import UserFeedbackLogger, TimingLogger
from adaptive_scheduler.interfaces       import NetworkInterface, PondScheduleInterface, RequestDBInterface
from adaptive_scheduler.scheduler        import LCOGTNetworkScheduler, SchedulerRunner, SchedulerParameters
from adaptive_scheduler.monitoring.network_status   import Network
from adaptive_scheduler.kernel.fullscheduler_gurobi import FullScheduler_gurobi as FullScheduler

import argparse
import logging
import sys

VERSION = '1.0.1'

# Set up and configure an application scope logger
# import logging.config
# logging.config.fileConfig('logging.conf')
# import logger_config
log = logging.getLogger('adaptive_scheduler')

# Set up signal handling for graceful shutdown
run_flag = True

def ctrl_c_handler(signal, frame):
    global run_flag
    print 'Received Ctrl+C - terminating on loop completion.'
    run_flag = False

def kill_handler(signal, frame):
    global run_flag
    print 'Received SIGTERM (kill) - terminating on loop completion.'
    run_flag = False

# import signal
#signal.signal(signal.SIGINT, ctrl_c_handler)
#signal.signal(signal.SIGTERM, kill_handler)

# TODO: Write unit tests for these methods
    
        
class RequestDBSchedulerParameters(SchedulerParameters):
    
    def __init__(self, requestdb_url, **kwargs):
        SchedulerParameters.__init__(self, **kwargs)
        self.requestdb_url = requestdb_url


def parse_args(argv):
    arg_parser = argparse.ArgumentParser(
                                formatter_class=argparse.RawDescriptionHelpFormatter,
                                description=__doc__)

    arg_parser.add_argument("-l", "--timelimit", type=float, default=None,
                            help="The time limit of the scheduler kernel, in seconds; negative implies no limit")
    arg_parser.add_argument("-i", "--horizon", type=float, default=7,
                            help="The scheduler's horizon, in days")
    arg_parser.add_argument("-z", "--slicesize", type=int, default=300,
                            help="The discretization size of the scheduler, in seconds")
    arg_parser.add_argument("-s", "--sleep", type=int, default=60,
                            help="Sleep period between scheduling runs, in seconds")
    arg_parser.add_argument("-r", "--requestdb", type=str, required=True,
                            help="Request DB endpoint URL")
    arg_parser.add_argument("-d", "--dry-run", action="store_true",
                            help="Perform a trial run with no changes made")
    arg_parser.add_argument("-n", "--now", type=str,
                            help="Alternative datetime to use as 'now', for running simulations (%%Y-%%m-%%d %%H:%%M:%%S)")
    arg_parser.add_argument("-t", "--telescopes", type=str, default='telescopes.dat',
                            help="Available telescopes file (default=telescopes.dat)")
    arg_parser.add_argument("-c", "--cameras", type=str, default='camera_mappings.dat',
                            help="Instrument description file (default=camera_mappings.dat)")
    arg_parser.add_argument("-w", "--noweather", action="store_true",
                            help="Disable weather checking")
    arg_parser.add_argument("--nosingles", action="store_true",
                                help="Ignore the 'single' Request type")
    arg_parser.add_argument("--nocompounds", action="store_true",
                                help="Ignore the 'and', 'oneof' and 'many' Request types")
    arg_parser.add_argument("--notoo", action="store_true",
                                help="Treat Target of Opportunity Requests like Normal Requests")
    arg_parser.add_argument("-o", "--run-once", action="store_true",
                            help="Only run the scheduling loop once, then exit")

    # Handle command line arguments
    args = arg_parser.parse_args(argv)

    if args.dry_run:
        log.info("Running in simulation mode - no DB changes will be made")
    log.info("Using available telescopes file '%s'", args.telescopes)
    log.info("Sleep period between scheduling runs set at %ds" % args.sleep)
    
    sched_params = RequestDBSchedulerParameters(**args)

    return sched_params





def main(argv):
    sched_params = parse_args(argv)

    log.info("Starting Adaptive Scheduler, version {v}".format(v=VERSION))

    if sched_params.dry_run:
        import lcogtpond
        lcogtpond._service_host = 'localhost'

    event_bus = get_eventbus()
    user_feedback_logger = UserFeedbackLogger()
    timing_logger        = TimingLogger()
    event_bus.add_listener(user_feedback_logger, persist=True)
    event_bus.add_listener(timing_logger, persist=True,
                           event_type=TimingLogger._StartEvent)
    event_bus.add_listener(timing_logger, persist=True,
                           event_type=TimingLogger._EndEvent)
    
    schedule_interface = PondScheduleInterface()
    requestdb_client = SchedulerClient()
    user_request_interface = RequestDBInterface(requestdb_client)
    network_state_interface = Network()
    network_interface = NetworkInterface(schedule_interface, user_request_interface, network_state_interface)
    
    kernel_class = FullScheduler
    scheduler = LCOGTNetworkScheduler(kernel_class, sched_params, event_bus)
    network_model = sched_params.get_model_builder().tel_network.telescopes
    scheduler_runner = SchedulerRunner(sched_params, scheduler, network_interface, network_model)
    scheduler_runner.run()


if __name__ == '__main__':
    main(sys.argv[1:])

