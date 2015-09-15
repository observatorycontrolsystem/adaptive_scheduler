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
from adaptive_scheduler.interfaces       import NetworkInterface, CachedInputNetworkInterface
from adaptive_scheduler.pond             import PondScheduleInterface
from adaptive_scheduler.requestdb        import RequestDBInterface 
from adaptive_scheduler.scheduler        import LCOGTNetworkScheduler, SchedulerRunner
from adaptive_scheduler.scheduler_input  import SchedulerParameters, SchedulingInputFactory, SchedulingInputProvider, FileBasedSchedulingInputProvider, RequestDBSchedulerParameters
from adaptive_scheduler.monitoring.network_status   import Network
# from adaptive_scheduler.kernel.fullscheduler_gurobi import FullScheduler_gurobi as FullScheduler
from adaptive_scheduler.kernel.fullscheduler_v6 import FullScheduler_v6 as FullScheduler

from reqdb.client import SchedulerClient

import argparse
import logging
import sys

VERSION = '1.0.1'

# Set up and configure an application scope logger
# import logging.config
# logging.config.fileConfig('logging.conf')
import logger_config
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
    
        



def parse_args(argv):
    defaults = RequestDBSchedulerParameters(None)
    arg_parser = argparse.ArgumentParser(
                                formatter_class=argparse.RawDescriptionHelpFormatter,
                                description=__doc__)

    arg_parser.add_argument("-l", "--timelimit", type=float, default=defaults.timelimit_seconds, dest='timelimit_seconds',
                            help="The time limit of the scheduler kernel, in seconds; negative implies no limit")
    arg_parser.add_argument("-i", "--horizon", type=float, default=defaults.horizon_days, dest='horizon_days',
                            help="The scheduler's horizon, in days")
    arg_parser.add_argument("-z", "--slicesize", type=int, default=defaults.slicesize_seconds, dest='slicesize_seconds',
                            help="The discretization size of the scheduler, in seconds")
    arg_parser.add_argument("-s", "--sleep", type=int, default=defaults.sleep_seconds, dest='sleep_seconds',
                            help="Sleep period between scheduling runs, in seconds")
    arg_parser.add_argument("-r", "--requestdb", type=str, required=True,
                            help="Request DB endpoint URL")
    arg_parser.add_argument("-d", "--dry-run", action="store_true",
                            help="Perform a trial run with no changes made")
    arg_parser.add_argument("-n", "--now", type=str, dest='simulate_now',
                            help="Alternative datetime to use as 'now', for running simulations (%%Y-%%m-%%d %%H:%%M:%%S)")
    arg_parser.add_argument("-t", "--telescopes", type=str, default=defaults.telescopes_file, dest='telescopes_file',
                            help="Available telescopes file")
    arg_parser.add_argument("-c", "--cameras", type=str, default=defaults.cameras_file, dest='cameras_file',
                            help="Instrument description file")
    arg_parser.add_argument("-w", "--noweather", action="store_true", dest='no_weather',
                            help="Disable weather checking")
    arg_parser.add_argument("--nosingles", action="store_true", dest='no_singles',
                                help="Ignore the 'single' Request type")
    arg_parser.add_argument("--nocompounds", action="store_true", dest='no_compounds',
                                help="Ignore the 'and', 'oneof' and 'many' Request types")
    arg_parser.add_argument("--notoo", action="store_true", dest='no_too',
                                help="Treat Target of Opportunity Requests like Normal Requests")
    arg_parser.add_argument("-o", "--run-once", action="store_true",
                            help="Only run the scheduling loop once, then exit")
    arg_parser.add_argument("-k", "--kernel", type=str, default=defaults.kernel,
                            help="Options are v5, v6, gurobi, mock. Default is gurobi")
    arg_parser.add_argument("-f", "--fromfile", type=str, dest='input_file_name', default=defaults.input_file_name,
                            help="Filenames for scheduler input. Example: -f too_input.in,normal_input.in")
    arg_parser.add_argument("--no-pickle", action="store_true", dest='no_pickle',
                                help="Disable storing pickled files of scheduling run input")
    arg_parser.add_argument("--pondport", type=int, dest='pond_port',
                                help="Port for POND communication", default=defaults.pond_port)
    arg_parser.add_argument("--pondhost", type=str, dest='pond_host',
                                help="Hostname for POND communication", default=defaults.pond_host)
    arg_parser.add_argument("--profiling_enabled", type=bool, dest='profiling_enabled',
                                help="Enable profiling output", default=defaults.profiling_enabled)
    arg_parser.add_argument("--reservation_save_time_seconds", type=float, dest='avg_reservation_save_time_seconds',
                                help="Initial estimate for time needed to save a new scheduler reservation", default=defaults.avg_reservation_save_time_seconds)
    arg_parser.add_argument("--normal_runtime_seconds", type=float, dest='normal_runtime_seconds',
                                help="Initial estimate for the normal loop runtime", default=defaults.normal_runtime_seconds)
    arg_parser.add_argument("--too_runtime_seconds", type=float, dest='too_runtime_seconds',
                                help="Initial estimate for the ToO loop runtime", default=defaults.too_runtime_seconds)
    
    # Handle command line arguments
    args = arg_parser.parse_args(argv)

    if args.dry_run:
        log.info("Running in simulation mode - no DB changes will be made")
    log.info("Using available telescopes file '%s'", args.telescopes_file)
    log.info("Sleep period between scheduling runs set at %ds" % args.sleep_seconds)
    
    sched_params = RequestDBSchedulerParameters(**vars(args))

    return sched_params



def get_kernel_class(sched_params):
    kernel_class = None
    if sched_params.kernel == 'v5':
        from adaptive_scheduler.kernel.fullscheduler_v5 import FullScheduler_v5
        kernel_class = FullScheduler_v5
        # Use -1 for no timelimit
        if sched_params.timelimit_seconds == None:
            sched_params.timelimit_seconds = -1
    elif sched_params.kernel == 'v6':
        from adaptive_scheduler.kernel.fullscheduler_v6 import FullScheduler_v6
        kernel_class = FullScheduler_v6
        # Use -1 for no timelimit
        if sched_params.timelimit_seconds == None:
            sched_params.timelimit_seconds = -1
    elif sched_params.kernel == 'gurobi':
        from adaptive_scheduler.kernel.fullscheduler_gurobi import FullScheduler_gurobi
        kernel_class = FullScheduler_gurobi
    elif sched_params.kernel == 'mock':
        from mock import Mock
        kernel_mock = Mock()
        kernel_mock.schedule_all = Mock(return_value={})
        kernel_class = Mock(return_value=kernel_mock)
    else:
        raise Exception("Unknown kernel version %s" % sched_params.kernel)
    
    return kernel_class



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
    
    schedule_interface = PondScheduleInterface(port=sched_params.pond_port, host=sched_params.pond_host)
    requestdb_client = SchedulerClient(sched_params.requestdb_url)
    user_request_interface = RequestDBInterface(requestdb_client)
    network_state_interface = Network()
    network_interface = NetworkInterface(schedule_interface, user_request_interface, network_state_interface)
#     network_interface = CachedInputNetworkInterface('/data/adaptive_scheduler/input_states/scheduler_input.pickle')
    
    kernel_class = get_kernel_class(sched_params)
    network_model = sched_params.get_model_builder().tel_network.telescopes
    scheduler = LCOGTNetworkScheduler(kernel_class, sched_params, event_bus, network_model)
    if sched_params.input_file_name:
        too_infile, normal_infile = sched_params.input_file_name.split(',')
        input_provider = FileBasedSchedulingInputProvider(too_infile, normal_infile, is_too_mode=True)
    else:
        input_provider = SchedulingInputProvider(sched_params, network_interface, network_model, is_too_input=True)
    input_factory = SchedulingInputFactory(input_provider)
    scheduler_runner = SchedulerRunner(sched_params, scheduler, network_interface, network_model, input_factory)
    scheduler_runner.run()


if __name__ == '__main__':
    main(sys.argv[1:])

