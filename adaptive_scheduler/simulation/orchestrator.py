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
from datetime import datetime, timedelta

from opensearchpy import OpenSearch
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
from adaptive_scheduler.simulation.metrics import *

log = logging.getLogger('adaptive_scheduler')

# Some Environment Variable settings for the simulation
RUN_ID = os.getenv("SIMULATION_RUN_ID", "1")
START_TIME = parse(os.getenv("SIMULATION_START_TIME", "2022-06-23"))
END_TIME = parse(os.getenv("SIMULATION_END_TIME", "2022-07-07"))
TIME_STEP = os.getenv("SIMULATION_TIME_STEP_MINUTES", "60")


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
    pass

def increment_input(current_time, time_step):
    # This will eventually call endpoints in configdb and the observation portal to increment the state of them forward 
    # by the time step specified. Incrementing time forward is slightly different then the initial setup of a starting time.
    # This will be called as you step forward in time to make sure these data sources contain the right input data.
    # For configdb, this involves moving the records back forwards a bit. For the observation portal, it involves pulling 
    # down newer requests as well as cleaning up the state of old ones between time steps (completing/expiring as appropriate).
    # This also means that we should complete and fail the right percentages of observations that should have ended within the last
    # time_step, and set ones that are in progress to ATTEMPTED state.
    pass

def send_to_opensearch(metrics):
    # Send the json metrics to the opensearch index
    pass

    
def record_metrics(normal_scheduled_requests_by_rg_id, rr_scheduled_requests_by_rg_id):
    # Derive whatever metrics we want using the supplied scheduled requests and send them to opensearch here

    # maybe we should just pass in the scheduler result instead and get the normal and rr requests somewhere else
    
    # For aggregating across all requests, but not sure if this is the best method
    combined_scheduled_requests_by_rg_id = combine_normal_and_rr_requests_by_rg_id(
        normal_scheduled_requests_by_rg_id, rr_scheduled_requests_by_rg_id)
    
    metrics = {
        'simulation_id': RUN_ID,
        'total_scheduled_time': total_scheduled_time(combined_scheduled_requests_by_rg_id)
        'total_scheduled_count': total_scheduled_count(combined_scheduled_requests_by_rg_id)
        'percent_scheduled': percent_of_requests_scheduled(combined_scheduled_requests_by_rg_id)
    }
    send_to_opensearch(metrics)


def main(argv=None):
    # Get all scheduler params from environment variables
    sched_params = SchedulerParameters()

    # Set up and configure an application scope logger
    setup_logging()

    log.info(f"Starting Scheduler Simulator with id {RUN_ID}")

    # All this setup is the same as the normal scheduling run - things will be setup based on the
    # scheduler environment variables set.
    event_bus = get_eventbus()
    schedule_interface = ObservationScheduleInterface(host=sched_params.observation_portal_url)
    observation_portal_interface = ObservationPortalInterface(sched_params.observation_portal_url)
    # TODO: If there is a configuration override file detected then incorporate that into the configdb_interface
    configdb_interface = ConfigDBInterface(configdb_url=sched_params.configdb_url, telescope_classes=sched_params.telescope_classes)
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
        sched_params.simulate_now = current_time.isoformat()

        # Scheduler run is invoked in the normal way, but it will just run a single time
        scheduler_runner = SchedulerRunner(sched_params, scheduler, network_interface, network_model, input_factory)
        scheduler_runner.run()

        # Output scheduled requests are available within the runner after it completes a run
        # These are used to seed a warm start solution for the next run in the normal scheduler, but can be used to generate metrics here
        rr_scheduled_requests_by_rg_id = scheduler_runner.rr_scheduled_requests_by_rg
        normal_scheduled_requests_by_rg_id = scheduler_runner.normal_scheduled_requests_by_rg
        record_metrics(normal_scheduled_requests_by_rg_id, rr_scheduled_requests_by_rg_id)

        current_time += timedelta(minutes=TIME_STEP)
        increment_input(current_time, TIME_STEP)


if __name__ == '__main__':
    main(sys.argv[1:])
