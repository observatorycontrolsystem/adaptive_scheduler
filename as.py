#!/usr/bin/env python
'''
as.py - Run the adaptive scheduler on continuous loop.

Author: Eric Saunders
July 2012
'''
from __future__ import division

from adaptive_scheduler.eventbus import get_eventbus
from adaptive_scheduler.feedback import UserFeedbackLogger, TimingLogger
from adaptive_scheduler.interfaces import NetworkInterface
from adaptive_scheduler.observations import ObservationScheduleInterface
from adaptive_scheduler.observation_portal_connections import ObservationPortalInterface
from adaptive_scheduler.configdb_connections import ConfigDBInterface
from adaptive_scheduler.scheduler import LCOGTNetworkScheduler, SchedulerRunner
from adaptive_scheduler.scheduler_input import SchedulingInputFactory, SchedulingInputProvider, \
    FileBasedSchedulingInputProvider, SchedulerParameters
from adaptive_scheduler.monitoring.network_status import Network
from adaptive_scheduler.kernel.fullscheduler_ortoolkit import FullScheduler_ortoolkit, ALGORITHMS

import argparse
import logging
import sys

VERSION = '1.1.0'

# Set up and configure an application scope logger
import logger_config

log = logging.getLogger('adaptive_scheduler')
rg_logger = logging.getLogger('rg_logger')

# Set up signal handling for graceful shutdown
run_flag = True


def parse_args(argv):
    defaults = SchedulerParameters()
    arg_parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=__doc__)

    arg_parser.add_argument("-l", "--timelimit", type=float, default=defaults.timelimit_seconds,
                            dest='timelimit_seconds',
                            help="The time limit of the scheduler kernel, in seconds; negative implies no limit")
    arg_parser.add_argument("-i", "--horizon", type=float, default=defaults.horizon_days, dest='horizon_days',
                            help="The scheduler's horizon, in days")
    arg_parser.add_argument("-z", "--slicesize", type=int, default=defaults.slicesize_seconds, dest='slicesize_seconds',
                            help="The discretization size of the scheduler, in seconds")
    arg_parser.add_argument("-s", "--sleep", type=int, default=defaults.sleep_seconds, dest='sleep_seconds',
                            help="Sleep period between scheduling runs, in seconds")
    arg_parser.add_argument("-p", "--observation_portal_url", type=str, required=True, dest='observation_portal_url',
                            help="Observation Portal base URL")
    arg_parser.add_argument("-c", "--configdb_url", type=str, dest='configdb_url', default=defaults.configdb_url,
                            help="ConfigDB endpoint URL")
    arg_parser.add_argument("-d", "--dry-run", action="store_true",
                            help="Perform a trial run with no changes made")
    arg_parser.add_argument("-n", "--now", type=str, dest='simulate_now',
                            help="Alternative datetime to use as 'now', for running simulations (in isoformat: %%Y-%%m-%%dT%%H:%%M:%%SZ)")
    arg_parser.add_argument("-w", "--noweather", action="store_true", dest='no_weather',
                            help="Disable weather checking")
    arg_parser.add_argument("--nosingles", action="store_true", dest='no_singles',
                            help="Ignore the 'single' Request type")
    arg_parser.add_argument("--nocompounds", action="store_true", dest='no_compounds',
                            help="Ignore the 'and', 'oneof' and 'many' Request types")
    arg_parser.add_argument("--no_rr", action="store_true", dest='no_rr',
                            help="Treat Rapid Response Requests like Normal Requests")
    arg_parser.add_argument("-o", "--run-once", action="store_true",
                            help="Only run the scheduling loop once, then exit")
    arg_parser.add_argument("-k", "--kernel", type=str, default=defaults.kernel, choices=ALGORITHMS.keys(),
                            help="Options are GUROBI, CBC, or GLPK. Default is CBC")
    arg_parser.add_argument("-f", "--fromfile", type=str, dest='input_file_name', default=defaults.input_file_name,
                            help="Filename for scheduler input. Example: -f scheduling_input_20180101.pickle")
    arg_parser.add_argument("--pickle", action="store_true", dest='pickle',
                            help="Enable storing pickled files of scheduling run input")
    arg_parser.add_argument("--save_output", action="store_true", dest='save_output',
                            help="Enable storing scheduling run output in a json file")
    arg_parser.add_argument("--request_logs", action="store_true", dest='request_logs',
                            help="Enable saving the per-request log files")
    arg_parser.add_argument("--downtime_url", type=str, dest='downtime_url',
                            help="Downtime endpoint url", default=defaults.downtime_url)
    arg_parser.add_argument("--elasticsearch_url", type=str, dest='elasticsearch_url',
                            help="Elasticsearch telemetry endpoint url", default=defaults.elasticsearch_url)
    arg_parser.add_argument("--elasticsearch_index", type=str, dest='elasticsearch_index',
                            help="Elasticsearch telemetry index name", default=defaults.elasticsearch_index)
    arg_parser.add_argument("--elasticsearch_excluded_observatories", type=str,
                            dest='elasticsearch_excluded_observatories',
                            help="Elasticsearch telemetry observatories to exclude (comma delimited)",
                            default=defaults.elasticsearch_excluded_observatories)
    arg_parser.add_argument("--profiling_enabled", type=bool, dest='profiling_enabled',
                            help="Enable profiling output", default=defaults.profiling_enabled)
    arg_parser.add_argument("--reservation_save_time_seconds", type=float, dest='avg_reservation_save_time_seconds',
                            help="Initial estimate for time needed to save a new scheduler reservation",
                            default=defaults.avg_reservation_save_time_seconds)
    arg_parser.add_argument("--normal_runtime_seconds", type=float, dest='normal_runtime_seconds',
                            help="Initial estimate for the normal loop runtime",
                            default=defaults.normal_runtime_seconds)
    arg_parser.add_argument("--rr_runtime_seconds", type=float, dest='rr_runtime_seconds',
                            help="Initial estimate for the Rapid Response loop runtime",
                            default=defaults.rr_runtime_seconds)
    arg_parser.add_argument("--ignore_ipp", action="store_true", dest='ignore_ipp',
                            help="Ignore intra-proposal priority when computing request priority",
                            default=defaults.ignore_ipp)
    arg_parser.add_argument("--debug", action="store_true", dest='debug',
                            help="Sets debug mode in the requestdb client calls to save error output to a file.",
                            default=defaults.debug)

    # Handle command line arguments
    args, unknown = arg_parser.parse_known_args(argv)

    if args.dry_run:
        log.info("Running in simulation mode - no DB changes will be made")
    log.info("Sleep period between scheduling runs set at %ds" % args.sleep_seconds)

    rg_logger.disabled = not args.request_logs

    sched_params = SchedulerParameters(**vars(args))

    return sched_params


def get_kernel_class(sched_params):
    kernel_class = None
    if sched_params.kernel == 'mock':
        from mock import Mock
        kernel_mock = Mock()
        kernel_mock.schedule_all = Mock(return_value={})
        kernel_class = Mock(return_value=kernel_mock)
    else:
        kernel_class = FullScheduler_ortoolkit
    return kernel_class


def main(argv):
    sched_params = parse_args(argv)
    log.info("Starting Adaptive Scheduler, version {v}".format(v=VERSION))

    event_bus = get_eventbus()
    user_feedback_logger = UserFeedbackLogger()
    timing_logger = TimingLogger()
    event_bus.add_listener(user_feedback_logger, persist=True)
    event_bus.add_listener(timing_logger, persist=True,
                           event_type=TimingLogger._StartEvent)
    event_bus.add_listener(timing_logger, persist=True,
                           event_type=TimingLogger._EndEvent)

    schedule_interface = ObservationScheduleInterface(host=sched_params.observation_portal_url)
    observation_portal_interface = ObservationPortalInterface(sched_params.observation_portal_url,
                                                              debug=sched_params.debug)
    configdb_interface = ConfigDBInterface(configdb_url=sched_params.configdb_url)
    network_state_interface = Network(configdb_interface, sched_params)
    network_interface = NetworkInterface(schedule_interface, observation_portal_interface, network_state_interface,
                                         configdb_interface)

    kernel_class = get_kernel_class(sched_params)
    network_model = configdb_interface.get_telescope_info()
    scheduler = LCOGTNetworkScheduler(kernel_class, sched_params, event_bus, network_model)
    if sched_params.input_file_name:
        input_provider = FileBasedSchedulingInputProvider(sched_params.input_file_name, network_interface,
                                                          is_rr_mode=True)
    else:
        input_provider = SchedulingInputProvider(sched_params, network_interface, network_model, is_rr_input=True)
    input_factory = SchedulingInputFactory(input_provider)
    scheduler_runner = SchedulerRunner(sched_params, scheduler, network_interface, network_model, input_factory)
    scheduler_runner.run()


if __name__ == '__main__':
    main(sys.argv[1:])
