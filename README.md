# Adaptive Scheduler

[![Build Status](https://travis-ci.com/observatorycontrolsystem/adaptive_scheduler.svg?branch=master)](https://travis-ci.com/observatorycontrolsystem/adaptive_scheduler)
[![Coverage Status](https://coveralls.io/repos/github/observatorycontrolsystem/adaptive_scheduler/badge.svg?branch=master)](https://coveralls.io/github/observatorycontrolsystem/adaptive_scheduler?branch=master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/c41dca92a76f4ea9a284698d26772e91)](https://app.codacy.com/gh/observatorycontrolsystem/adaptive_scheduler?utm_source=github.com&utm_medium=referral&utm_content=observatorycontrolsystem/adaptive_scheduler&utm_campaign=Badge_Grade_Dashboard)

The adaptive scheduler works with the [Observation Portal](https://github.com/observatorycontrolsystem/observation-portal) 
to get the set of schedulable requests, and cancel and submit sets of observations on sites. It solves an optimization
problem using a mixed integer programming solver to maximize the priority of scheduled observations. It supports the 
three types of observations within the Observation Portal (Normal, Rapid Request, and Time Critical). It has optional 
support to query elasticsearch to incorporate telescope telemetry into it's decision to schedule on a telescope. 
It connects to the [Configuration Database](https://github.com/observatorycontrolsystem/configdb) to get the set 
of schedulable instruments.

[Google's OR-Tools](https://developers.google.com/optimization) is now used for the scheduling kernel. The Dockerfile is configured to support the CBC and GLPK free algorithms, and version 6.x of GUROBI if you have a license. The default kernel algorithm is now CBC, but GLPK or GUROBI can be specified as well using the '-k {ALGORITHM}' argument when invoking the scheduler.

## Prerequisites

Optional prerequisites can be skipped for reduced functionality.

-   Python == 3.6.x
-   A running [Configuration Database](https://github.com/observatorycontrolsystem/configdb)
-   A running [Observation Portal](https://github.com/observatorycontrolsystem/observation-portal) 
-   (Optional) A running [Downtime Database](https://github.com/observatorycontrolsystem/downtime)
-   (Optional) A running Elasticsearch with index for telescope telemetry
-   (Optional) A running Redis instance - used for caching rise-set values

## Environment Variables

|                        | Variable                | Description                                                         | Default                                                 |
| ---------------------- | ----------------------- | ------------------------------------------------------------------- | ------------------------------------------------------- |
| Gurobi                 | `GRB_LICENSE_FILE`      | The location of the gurobi license file within your container       | _`Empty string`_                                                      |
| Metrics                | `OPENTSDB_HOSTNAME`     | The host name for an opentsdb server, for metrics                   | _`Empty string`_                                                      |
| External Services      | `CONFIGDB_URL`          | The url to the configuration database                               | `http://127.0.0.1:7500`                                 |
|                        | `DOWNTIMEDB_URL`        | The url to the downtime database                                    | `http://127.0.0.1:7000`                                 |
|                        | `ELASTICSEARCH_URL`     | The url to the elasticsearch cluster                                | _`Empty string`_                                                      |
|                        | `OBSERVATION_PORTAL_URL`| The url to the observation portal                                   | `http://127.0.0.1:8000`                                 |
|                        | `REDIS_URL`             | The url of the redis cache (or the linked container name)           | `redis`                                                 |

## How to Run

The scheduler can either be run directly on a machine, or in the provided Docker container

### Native Run

First install the requirements into a python3.6 virtualenv. There are a large number of input arguments, with defaults 
defined in the *SchedulerParameters* class in **adaptive_scheduler/scheduler_input.py**.

`python as.py --help`

### Docker Run 

You can build the **Dockerfile** locally with local changes by running

`docker build -t name_of_my_container .`

You can then update the container name in the supplied **docker-compose.yml** and run the tests using

`docker-compose up`

To run the actual scheduler instead of its unit tests, change the entrypoint in the **docker-compose.yml** file to 
invoke the main entrypoint `python as.py` with the desired arguments. Currently this will only work if you have a Gurobi 
license for Gurobi 6.0.2 mounted into the container and linked appropriately, or switch the code to not use Gurobi.
