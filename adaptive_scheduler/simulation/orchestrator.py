'''
Entrypoint to run the scheduler in simulation mode.

This orchestrator will setup the simulation from environment variables specified, 
run the scheduling loop one or more times, and record metrics about a single run or
the full simulation within an OpenSearch index. The orchestrator will also handle
advancing time and input when simulating over a period of time.
'''

import logging
import sys
import os
import json
from urllib.parse import urljoin

import requests
from datetime import datetime, timedelta

from lcogt_logging import LCOGTFormatter
from dateutil.parser import parse

from adaptive_scheduler.eventbus import get_eventbus
from adaptive_scheduler.kernel.fullscheduler_ortoolkit import FullScheduler_ortoolkit
from adaptive_scheduler.monitoring.network_status import Network
from adaptive_scheduler.interfaces import NetworkInterface
from adaptive_scheduler.observations import ObservationScheduleInterface
from adaptive_scheduler.observation_portal_connections import ObservationPortalInterface
from adaptive_scheduler.configdb_connections import ConfigDBInterface
from adaptive_scheduler.scheduler import LCOGTNetworkScheduler, SchedulerRunner
from adaptive_scheduler.scheduler_input import (
  SchedulingInputFactory, SchedulingInputProvider, SchedulerParameters
)
from adaptive_scheduler.simulation.metrics import MetricCalculator
from adaptive_scheduler.utils import timeit


log = logging.getLogger('adaptive_scheduler')

# Some Environment Variable settings for the simulation
RUN_ID = os.getenv("SIMULATION_RUN_ID", "1")
START_TIME = parse(os.getenv("SIMULATION_START_TIME", "2022-06-23"))
END_TIME = parse(os.getenv("SIMULATION_END_TIME", "2022-07-07"))
TIME_STEP = float(os.getenv("SIMULATION_TIME_STEP_MINUTES", "60"))
AIRMASS_WEIGHTING_COEFFICIENT = os.getenv("SIMULATION_AIRMASS_COEFFICIENT", 0.1)


def setup_logging():
    log = logging.getLogger('adaptive_scheduler')
    log.setLevel(logging.INFO)
    log.propagate = False

    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)

    formatter = LCOGTFormatter()

    sh.setFormatter(formatter)
    log.addHandler(sh)


def setup_input(current_time):
    # This will eventually call endpoint in configdb and the observation portal to setup the input state of those
    # source based on the current timestamp of the scheduling run. For configdb, this involves playing the records
    # backwards until the time is reached. For the observation portal, it involves pulling over all requests
    # created and PENDING at a certain point in time for the semester, which should be doable by looking at the created
    # and modified timestamps and state.
    log.info(f"Placeholder for setting up input for time {current_time.isoformat}")
    pass


def increment_input(current_time, time_step):
    # This will eventually call endpoints in configdb and the observation portal to increment the state of them forward
    # by the time step specified. Incrementing time forward is slightly different then the initial setup of a starting time.
    # This will be called as you step forward in time to make sure these data sources contain the right input data.
    # For configdb, this involves moving the records back forwards a bit. For the observation portal, it involves pulling
    # down newer requests as well as cleaning up the state of old ones between time steps (completing/expiring as appropriate).
    # This also means that we should complete and fail the right percentages of observations that should have ended within the last
    # time_step, and set ones that are in progress to ATTEMPTED state.
    log.info(f"Placeholder for incrementing input by {time_step} to time {current_time.isoformat}")
    pass


@timeit
def send_to_opensearch(os_url, os_index, metrics):
    # Send the json metrics to the opensearch index
    if os_url and os_index:
        doc_name = f"{metrics['simulation_id']}_{metrics['record_time']}"
        try:
            requests.post(
                urljoin(os_url, f'{os_index}/_doc/{doc_name}'), json=metrics
            ).raise_for_status()
        except Exception as ex:
            log.warning(f"Failed to save metrics to Opensearch at {os_url} in index {os_index}: {repr(ex)}")

        log.info(f"Successfully saved metrics for {metrics['simulation_id']}")
    else:
        log.warning("Not configured to save metrics in opensearch. Please set OPENSEARCH_URL and SIMULATION_OPENSEARCH_INDEX.")


def record_metrics(normal_scheduler_result, rr_scheduler_result, scheduler, scheduler_runner):
    log.info("Recording metrics for scheduler simulation run")

    metrics = MetricCalculator(normal_scheduler_result, rr_scheduler_result, scheduler, scheduler_runner)
    sched_params = scheduler_runner.sched_params
    airmass_metrics = metrics.airmass_metrics()
    sched_priorities, unsched_priorities = metrics.get_priority_data()
    sched_durations, unsched_durations = metrics.get_duration_data()
    binned_tac_priority_metrics = metrics.binned_tac_priority_metrics()

    metrics = {
        'simulation_id': RUN_ID,
        'simulation_start_time': sched_params.simulate_now,
        'horizon_days': sched_params.horizon_days,
        'slicesize_seconds': sched_params.slicesize_seconds,
        'kernel': sched_params.kernel,
        'mip_gap': sched_params.mip_gap,
        'record_time': datetime.utcnow().isoformat(),
        'airmass_weighting_coefficient': AIRMASS_WEIGHTING_COEFFICIENT,

        'total_effective_priority': metrics.total_scheduled_eff_priority()[0],
        'total_scheduled_count': metrics.count_scheduled()[0],
        'total_request_count': metrics.count_scheduled()[1],
        'percent_requests_scheduled': metrics.percent_reservations_scheduled(),
        'total_scheduled_seconds': sum(sched_durations),
        'total_available_seconds': metrics.total_available_seconds(),
        'percent_time_utilization': metrics.percent_time_utilization(),
        'airmass_metrics': airmass_metrics,
        'scheduled_req_by_priority': [binned_tac_priority_metrics['sched_histogram']],
        'scheduled_seconds_by_priority': [binned_tac_priority_metrics['sched_durations']],
        'total_req_by_priority': [binned_tac_priority_metrics['full_histogram']],
        'total_seconds_by_priority': [binned_tac_priority_metrics['all_durations']],
        'percent_sched_by_priority': [binned_tac_priority_metrics['percent_count']],
        'percent_duration_by_priority': [binned_tac_priority_metrics['percent_duration']],
        'raw_scheduled_durations': sched_durations,
        'raw_unscheduled_durations': unsched_durations,
        'raw_scheduled_priorities': sched_priorities,
        'raw_unscheduled_priorities': unsched_priorities,
    }
    send_to_opensearch(sched_params.opensearch_url, sched_params.simulation_opensearch_index, metrics)


def main(argv=None):
    # Get all scheduler params from environment variables
    sched_params = SchedulerParameters()

    # Set up and configure an application scope logger
    setup_logging()

    log.info(f"Starting Scheduler Simulator with id {RUN_ID} and time range {START_TIME.isoformat()} to {END_TIME.isoformat()}")

    # All this setup is the same as the normal scheduling run - things will be setup based on the
    # scheduler environment variables set.
    event_bus = get_eventbus()
    schedule_interface = ObservationScheduleInterface(host=sched_params.observation_portal_url)
    observation_portal_interface = ObservationPortalInterface(sched_params.observation_portal_url)
    # TODO: If there is a configuration override file detected then incorporate that into the configdb_interface
    overrides = None
    if os.path.exists('/app/data/simulation_overrides.json'):
        with open('/app/data/simulation_overrides.json', 'r') as fp:
            overrides = json.load(fp)
    configdb_interface = ConfigDBInterface(configdb_url=sched_params.configdb_url, telescope_classes=sched_params.telescope_classes, overrides=overrides)
    network_state_interface = Network(configdb_interface, sched_params)
    network_interface = NetworkInterface(schedule_interface, observation_portal_interface, network_state_interface,
                                         configdb_interface)
    kernel_class = FullScheduler_ortoolkit
    network_model = configdb_interface.get_telescope_info()

    scheduler = LCOGTNetworkScheduler(kernel_class, sched_params, event_bus, network_model)
    input_provider = SchedulingInputProvider(sched_params, network_interface, network_model, is_rr_input=True)
    input_factory = SchedulingInputFactory(input_provider)

    # Set the scheduler to run once each time it is invoked.
    sched_params.run_once = True

    # Basic orchestrator loop here: setup input, run scheduler, record metrics, step forward time, repeat
    current_time = START_TIME
    # Setup the input from configdb and observation portal using the current time
    setup_input(current_time)
    while current_time <= END_TIME:
        log.info(f"Simulating with current time {current_time.isoformat()}")
        sched_params.simulate_now = f"{current_time.isoformat()}Z"

        # Scheduler run is invoked in the normal way, but it will just run a single time
        scheduler_runner = SchedulerRunner(sched_params, scheduler, network_interface, network_model, input_factory)
        scheduler_runner.run()
        # Output scheduled requests are available within the runner after it completes a run
        # These are used to seed a warm start solution for the next run in the normal scheduler, but can be used to generate metrics here
        sched_params.metric_effective_horizon = 5  # days

        record_metrics(
            scheduler_runner.normal_scheduler_result,
            scheduler_runner.rr_scheduler_result,
            scheduler_runner.scheduler,
            scheduler_runner,
        )

        current_time += timedelta(minutes=TIME_STEP)
        increment_input(current_time, TIME_STEP)

    log.info(f"Finished running simulation {RUN_ID}, exiting")


if __name__ == '__main__':
    main(sys.argv[1:])
