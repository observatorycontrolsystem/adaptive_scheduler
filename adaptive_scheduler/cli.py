'''
Run the adaptive scheduler on continuous loop.

Author: Eric Saunders
July 2012
'''
from __future__ import division

from adaptive_scheduler.eventbus import get_eventbus
from adaptive_scheduler.feedback import UserFeedbackLogger, TimingLogger
from adaptive_scheduler.interfaces import NetworkInterface
from adaptive_scheduler.monitoring.seeing import DummySeeingMonitor, OpenSearchSeeingMonitor
from adaptive_scheduler.observations import ObservationScheduleInterface
from adaptive_scheduler.observation_portal_connections import ObservationPortalInterface
from adaptive_scheduler.configdb_connections import ConfigDBInterface
from adaptive_scheduler.scheduler import LCOGTNetworkScheduler, SchedulerRunner
from adaptive_scheduler.scheduler_input import (
  SchedulingInputFactory, SchedulingInputProvider,
  FileBasedSchedulingInputProvider, SchedulerParameters
)
from adaptive_scheduler.monitoring.network_status import Network
from adaptive_scheduler.kernel.fullscheduler_ortoolkit import FullScheduler_ortoolkit, ALGORITHMS
from adaptive_scheduler.log import RequestGroupHandler

from lcogt_logging import LCOGTFormatter

import argparse
import logging
import sys
import os


VERSION = '2.0.0'

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
    arg_parser.add_argument("-p", "--observation_portal_url", type=str, dest='observation_portal_url',
                            help="Observation Portal base URL", default=defaults.observation_portal_url)
    arg_parser.add_argument("-c", "--configdb_url", type=str, dest='configdb_url', default=defaults.configdb_url,
                            help="ConfigDB endpoint URL")
    arg_parser.add_argument("-d", "--dry-run", type=bool, default=defaults.dry_run,
                            help="Perform a trial run with no changes made")
    arg_parser.add_argument("-n", "--now", type=str, dest='simulate_now',
                            help="Alternative datetime to use as 'now', for running simulations (in isoformat: %%Y-%%m-%%dT%%H:%%M:%%SZ)")
    arg_parser.add_argument("-w", "--noweather", type=bool, default=defaults.no_weather, dest='no_weather',
                            help="Disable weather checking")
    arg_parser.add_argument("--nosingles", type=bool, default=defaults.no_singles, dest='no_singles',
                            help="Ignore the 'single' Request type")
    arg_parser.add_argument("--nocompounds", type=bool, default=defaults.no_compounds, dest='no_compounds',
                            help="Ignore the 'and', 'oneof' and 'many' Request types")
    arg_parser.add_argument("--no_rr", type=bool, default=defaults.no_rr, dest='no_rr',
                            help="Treat Rapid Response Requests like Normal Requests")
    arg_parser.add_argument("--warm_starts", type=bool, default=defaults.warm_starts, dest='warm_starts',
                            help="Enable using warm start solutions in the scheduling kernel")
    arg_parser.add_argument("-o", "--run-once", type=bool, default=defaults.run_once,
                            help="Only run the scheduling loop once, then exit")
    arg_parser.add_argument("-k", "--kernel", type=str, default=defaults.kernel, choices=ALGORITHMS.keys(),
                            help="Options are GUROBI, CBC, or SCIP. Default is SCIP")
    arg_parser.add_argument("--kernel_params", type=str, default=defaults.kernel_params,
                            help="Set kernel specific parameters within ORTools. Only set this if you know what you are doing")
    arg_parser.add_argument("-f", "--fromfile", type=str, dest='input_file_name', default=defaults.input_file_name,
                            help="Filename for scheduler input. Example: -f scheduling_input_20180101.pickle")
    arg_parser.add_argument("-g", "--mip_gap", type=float, default=defaults.mip_gap,
                            help="The acceptable MIP GAP threshold used in the solver. Defaults to 0.01 (1%%). Recommended range 0.01-0.0001")
    arg_parser.add_argument("--pickle", type=bool, default=defaults.pickle, dest='pickle',
                            help="Enable storing pickled files of scheduling run input")
    arg_parser.add_argument("--save_output", type=bool, default=defaults.save_output, dest='save_output',
                            help="Enable storing scheduling run output in a json file")
    arg_parser.add_argument("--request_logs", type=bool, default=defaults.request_logs, dest='request_logs',
                            help="Enable saving the per-request log files")
    arg_parser.add_argument("--request_logs_dir", type=str, default=defaults.request_logs_dir, dest='request_logs_dir',
                            help="Where to save the per-request log files")
    arg_parser.add_argument("--telescope_classes", type=str, default=','.join(defaults.telescope_classes),
                            help="Only schedule observations on the specified telescope_classes. Expects 3 character telescope classes comma delimited. If not specified, default is all classes.")
    arg_parser.add_argument("--downtime_url", type=str, dest='downtime_url',
                            help="Downtime endpoint url", default=defaults.downtime_url)
    arg_parser.add_argument("--opensearch_url", type=str, dest='opensearch_url',
                            help="OpenSearch telemetry endpoint url", default=defaults.opensearch_url)
    arg_parser.add_argument("--opensearch_index", type=str, dest='opensearch_index',
                            help="OpenSearch telemetry index name", default=defaults.opensearch_index)
    arg_parser.add_argument("--opensearch_seeing_index", type=str, dest='opensearch_seeing_index',
                            help="OpenSearch Seeing index name", default=defaults.opensearch_seeing_index)
    arg_parser.add_argument("--opensearch_excluded_observatories", type=str,
                            dest='opensearch_excluded_observatories',
                            help="OpenSearch telemetry observatories to exclude (comma delimited)",
                            default=','.join(defaults.opensearch_excluded_observatories))
    arg_parser.add_argument("--seeing_valid_time_period", type=float, default=defaults.seeing_valid_time_period, dest='seeing_valid_time_period',
                            help="The number of minutes in the future a seeing value should be used for to apply the seeing constraint on request windows")
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
    arg_parser.add_argument("--ignore_ipp", type=bool, dest='ignore_ipp',
                            help="Ignore intra-proposal priority when computing request priority",
                            default=defaults.ignore_ipp)

    # Handle command line arguments
    args, _ = arg_parser.parse_known_args(argv)

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


def setup_logging(sched_params):
    log = logging.getLogger('adaptive_scheduler')
    log.setLevel(logging.INFO)
    log.propagate = False

    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)

    formatter = LCOGTFormatter()

    sh.setFormatter(formatter)
    log.addHandler(sh)

    if sched_params.request_logs:
        # create the rg logs directory
        os.makedirs(sched_params.request_logs_dir, exist_ok=True)

        multi_rg_log = logging.getLogger('rg_logger')
        multi_rg_log.setLevel(logging.DEBUG)
        multi_rg_log.propagate = False

        uh = RequestGroupHandler(request_group_id=1, logdir=sched_params.request_logs_dir)
        uh.setLevel(logging.DEBUG)

        uh.setFormatter(formatter)
        multi_rg_log.addHandler(uh)


def main(argv=None):
    sched_params = parse_args(argv)

    # Set up and configure an application scope logger
    setup_logging(sched_params)

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
    observation_portal_interface = ObservationPortalInterface(sched_params.observation_portal_url)
    configdb_interface = ConfigDBInterface(configdb_url=sched_params.configdb_url, telescope_classes=sched_params.telescope_classes)
    network_state_interface = Network(configdb_interface, sched_params)

    if sched_params.opensearch_url and sched_params.opensearch_seeing_index:
        seeing_monitor = OpenSearchSeeingMonitor(
            sched_params.seeing_valid_time_period, configdb_interface, sched_params.opensearch_url,
            sched_params.opensearch_seeing_index, sched_params.opensearch_excluded_observatories
        )
    else:
        seeing_monitor = DummySeeingMonitor()
    network_interface = NetworkInterface(schedule_interface, observation_portal_interface, network_state_interface,
                                         configdb_interface, seeing_monitor)

    kernel_class = get_kernel_class(sched_params)
    network_model = configdb_interface.get_telescope_info()
    scheduler = LCOGTNetworkScheduler(kernel_class, sched_params, event_bus, network_model, seeing_monitor)
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
