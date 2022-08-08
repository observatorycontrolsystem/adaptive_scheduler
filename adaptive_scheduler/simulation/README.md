# Adaptive Scheduler Simulator Orchestrator

The orchestrator allows for running the adaptive scheduler in a simulated environment in order to facilitate testing.
It allows the user to dump input request data to the [Configuration Database](https://github.com/observatorycontrolsystem/configdb)
which is then passed to the scheduler. The orchestrator runs the scheduler and passes off the scheduler result to a
metric calculation file, which calculates metrics to send to an OpenSearch database. Work is still being done to enable the
orchestrator to step through a time range and run the scheduler repeatedly on different points of the input data.

## Overview of Metrics
The available metrics center around priority distributions, utilization, and miscellaneous data including airmass data
and slew distance. Certain metrics sent to OpenSearch are pre-binned by priority level. To get the best understanding of
the data structures, inspect the raw JSON in OpenSearch directly.

## Prerequisites
* Python 3.9
* A running [Configuration Database](https://github.com/observatorycontrolsystem/configdb)
* A running OpenSearch with index for scheduler simulations

## Environment Variables
Consult the adaptive scheduler README for general environment variables related to the scheduler. Additional environment
variables specific to the orchestrator are as follows:
| Variable                         | Description                                                                                 | Default                 |
|----------------------------------|---------------------------------------------------------------------------------------------|-------------------------|
| `SIMULATION_RUN_ID`              | The run ID of the scheduler. This will be saved as `simulation_id` in OpenSearch            | `1`                     |
| `SIMULATION_START_TIME`          | The simulation start time, which allows the orchestrator to step through a time range (WIP) | `2022-06-23`            |
| `SIMULATION_END_TIME`            | The end time of the time range (WIP)                                                        | `2022-07-07`            |
| `SIMULATION_TIME_STEP_MINUTES`   | The time step in minutes for the time range (WIP)                                           | `60`                    |
| `SIMULATION_AIRMASS_COEFFICIENT` | The airmass optimization weighting value                                                    | `0.1`                   |
| `SIMULATION_OPENSEARCH_INDEX`    | The index to save OpenSearch metrics to                                                     | `scheduler-simulations` |
| `OPENSEARCH_URL`                 | OpenSearch endpoint (needed for the plotting interface)                                     | _`Empty_string`_        |
|                                  |                                                                                             |                         |

## How to Run
When running in a Docker container, the entry point can be modified to point to the orchestrator instead of the scheduler,
e.g. `sh -c "sleep 20s; simulation-orchestrator"`. The twenty second wait time is to ensure all the relevant services (configdb, redis, etc.) are
spun up and available. Otherwise, run the orchestrator on a machine as you would run the scheduler. 

## Simulation Process
The general workflow for running a scheduler simulation is as follows:
1. Make changes to the adaptive scheduler. If running with Docker, build the image using the suggested build command in the adaptive scheduler README.
2. If necessary, adjust the `metrics.py` file to conform with the tests you are running, such as adjusting binning for priority values.
3. Modify environment variables accordingly, making sure to set and verify the run ID.
4. Run the orchestrator.

## Plotting
A plotting interface is included with the simulator to facilitate data visualization. The interface features OpenSearch searching by
either OpenSearch ID or `simulation_id`, the ability to save plots in various formats, and zsh-style TAB autocompletion. 
Note that the environment variable `OPENSEARCH_URL` must be set on whatever machine you are running the plots from.
To use the plotting interface, run `python -m adaptive_scheduler.simulation.plots`
(`-h` to show the available command line arguments).

## Creating Your Own Plots
The plotting framework provides a `Plot` class defined in `plotutils.py` to help initialize plots and get data from OpenSearch. 
`Plot` is initialized with a user-defined plotting function to generate the plot, the plot title, and either a single string or a list
of strings. It searches the `_id` or `simulation_id` field in OpenSearch for the strings and plots the data. To write your own plotting
functions, follow the example functions in `plotfuncs.py`. Plotting functions should take in either a list of datasets
or a single dataset (to match the initialization in `plots.py`). The plot title should be passed into the plotting function as well. 
This title is used to generate the descriptions for the command-line interface of the plotting framework.

The plot creation process is as simple as:
1. Creating a function (e.g. `plot_my_plot`) in `plotfuncs.py`
2. Adding the plot to the list of plots in `plots.py`, e.g. `Plot(plotfuncs.plot_my_plot, 'My Plot Title', 'some-data-id')`
