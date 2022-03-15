# Adaptive Scheduler

![Build](https://github.com/observatorycontrolsystem/adaptive_scheduler/workflows/Build/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/observatorycontrolsystem/adaptive_scheduler/badge.svg?branch=master)](https://coveralls.io/github/observatorycontrolsystem/adaptive_scheduler?branch=master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/c41dca92a76f4ea9a284698d26772e91)](https://app.codacy.com/gh/observatorycontrolsystem/adaptive_scheduler?utm_source=github.com&utm_medium=referral&utm_content=observatorycontrolsystem/adaptive_scheduler&utm_campaign=Badge_Grade_Dashboard)

The adaptive scheduler works with the [Observation Portal](https://github.com/observatorycontrolsystem/observation-portal) 
to get the set of schedulable requests, and cancel and submit sets of observations on sites. It solves an optimization
problem using a mixed integer programming solver to maximize the priority of scheduled observations. It supports the 
three types of observations within the Observation Portal (Normal, Rapid Request, and Time Critical). It has optional 
support to query OpenSearch to incorporate telescope telemetry into it's decision to schedule on a telescope. 
It connects to the [Configuration Database](https://github.com/observatorycontrolsystem/configdb) to get the set 
of schedulable instruments.

[Google's OR-Tools](https://developers.google.com/optimization) is now used for the scheduling kernel. The Dockerfile is configured to support the SCIP, CBC and GLPK free algorithms, and should support the latest GUROBI if you have a license. The docker-compose file is an example of how to run the container - the environment variables will need to be set to point to the proper services for it to function correctly. The default kernel algorithm is now SCIP, but CBC or GLPK can be specified as well using the '-k {ALGORITHM}' argument when invoking the scheduler.

## Prerequisites

Optional prerequisites can be skipped for reduced functionality.

-   Python == 3.6.x
-   A running [Configuration Database](https://github.com/observatorycontrolsystem/configdb)
-   A running [Observation Portal](https://github.com/observatorycontrolsystem/observation-portal) 
-   (Optional) A running [Downtime Database](https://github.com/observatorycontrolsystem/downtime)
-   (Optional) A running OpenSearch with index for telescope telemetry
-   (Optional) A running Redis instance - used for caching rise-set values

## Environment Variables

| Category               | Variable                | Description                                                         | Default                                                 |
| ---------------------- | ----------------------- | ------------------------------------------------------------------- | ------------------------------------------------------- |
| Gurobi                 | `GRB_LICENSE_FILE`      | The location of the gurobi license file within your container       | _`Empty string`_                                                      |
| Metrics                | `OPENTSDB_HOSTNAME`     | The host name for an opentsdb server, for metrics                   | _`Empty string`_                                                      |
|                        | `OPENTSDB_PYTHON_METRICS_TEST_MODE`| Set to any value to turn off metrics collection                   | `False`                                                       |
| External Services      | `CONFIGDB_URL`          | The url to the configuration database                               | `http://127.0.0.1:7500`                                 |
|                        | `DOWNTIME_URL`        | The url to the downtime database                                    | `http://127.0.0.1:7000`                                 |
|                        | `OPENSEARCH_URL`     | The url to the OpenSearch cluster                                | _`Empty string`_                                                      |
|                        | `OPENSEARCH_INDEX`     | The OpenSearch index for telescope telemetry                                | `live-telemetry`                                                      |
|                        | `OPENSEARCH_EXCLUDED_OBSERVATORIES`| Comma delimited list of enclosure codes to ignore telemetry from                                | _`Empty string`_                                                      |
|                        | `OBSERVATION_PORTAL_URL`| The url to the observation portal                                   | `http://127.0.0.1:8000`                                 |
|                        | `OBSERVATION_PORTAL_API_TOKEN`| The API Token for an admin of the observation-portal                                   | _`Empty string`_                                 |
|                        | `REDIS_URL`             | The url of the redis cache (or the linked container name)           | `redis`                                                 |
| Kernel Settings       | `KERNEL_ALGORITHM`     | Algorithm code for ORTools to use. Options are `CBC`, `SCIP`, `GLPK`, and `GUROBI`      | `SCIP`                                                 |
| Kernel Settings       | `KERNEL_FALLBACK_ALGORITHM`     | Fallback algorithm in case main choice fails or throws an exception. Options are `CBC`, `SCIP`, `GLPK`, and `GUROBI`      | `SCIP`                                                 |
|                       | `KERNEL_TIMELIMIT`     | Max amount of time for the kernel to try to find an optimal solution      | _None_                                                 |
|                       | `KERNEL_MIPGAP`     | MIP Gap tolerance for kernel to optimize to.      | 0.01                                                 |
|                       | `MODEL_SLICESIZE`     | Size of time chunks to discretize window starts into for the solver in whole seconds      | 300                                                 |
|                       | `MODEL_HORIZON`     | Number of days in the future to generate the schedule for      | 7.0                                                 |
| General Settings       | `DRY_RUN`             | If True, scheduler will run but no output will be saved to the Observation Portal          | `False`                                                 |
|                        | `RUN_ONCE`             | Terminate after running a single scheduling loop          | `False`                                                 |
|                        | `NO_WEATHER`             | Ignore any telescope telemetry - assume all telescopes are available          | `False`                                                 |
|                        | `NO_SINGLES`             | Do not consider `SINGLE` type RequestGroups for scheduling          | `False`                                                 |
|                        | `NO_COMPOUNDS`             | Do not consider `MANY` type RequestGroups for scheduling          | `False`                                                 |
|                        | `NO_RAPID_RESPONSE`             | Skip the Rapid Response scheduling loop (only perform Normal scheduling)          | `False`                                                 |
|                        | `TIME_BETWEEN_RUNS`             | Seconds to sleep between each scheduling loop          | 60.0                                                 |
|                        | `IGNORE_IPP_VALUES`             | If True, ignore IPP values when considering request priority          | `False`                                                   |
|                        | `INITIAL_PER_RESERVATION_SAVE_TIME`             | Initial estimate of time taken per reservation to save to the web portal          | 60.0                                                 |
|                        | `TELESCOPE_CLASSES`             | Restrict the scheduler to only operate on the specified telescope classes (comma delimited) (e.g. `1m0,2m0`). Default empty string means all telescope classes.       | ``                                                 |
|                        | `INITIAL_NORMAL_RUNTIME`             | Initial estimate of duration of normal scheduling cycle in seconds         | 360.0                                                 |
|                        | `INITIAL_RAPID_RESPONSE_RUNTIME`  | Initial estimate of duration of rapid response scheduling cycle in seconds      | 120.0                                                 |
| Debugging Settings     | `SAVE_PICKLE_INPUT_FILES`     | If True, stores pickled scheduler input files each run in `/data/adaptive_scheduler/input_states` | `False`                                                   |
|                        | `SAVE_JSON_OUTPUT_FILES`      | If True, stores json scheduler output files each run in `/data/adaptive_scheduler/output_schedule` | `False`                                                   |
|                        | `SAVE_PER_REQUEST_LOGS`      | If True, stores a log file for each Request considered for scheduling in `/logs/` | `False`                                                   |
|                        | `SAVE_PER_REQUEST_LOGS`      | If True, stores a log file for each Request considered for scheduling in `/logs/` | `False`                                                   |
|                        | `SCHEDULER_INPUT_FILE`      | Full path to scheduler pickle input file. If present, scheduler will run on the input file rather than getting current requests. | _`Empty string`_                                                 |
|                        | `CURRENT_TIME_OVERRIDE`      | Overrides the current time during scheduling. Useful for debugging things in the past | _None_                                                 |

## How to Run

The scheduler can either be run directly on a machine, or in the provided Docker container

### Native Run

First install the requirements into a python3.6 virtualenv. There are a large number of input arguments, with defaults 
defined in the *SchedulerParameters* class in **adaptive_scheduler/scheduler_input.py**.

`python as.py --help`

### Docker Run 

You can build the **Dockerfile** locally with local changes by running

`docker build -t name_of_my_container .`

You can then update the container name in the supplied **docker-compose.yml** and run the scheduler using

`cd deploy; docker-compose up`

To run the unit tests instead of the actual scheduler, change the commented out command in the **deploy/docker-compose.yml** file to run nosetests with the desired arguments.

Note you will likely want to change many of the environment variables to point to your services and adjust the settings of the scheduler.
