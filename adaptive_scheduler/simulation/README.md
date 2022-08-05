# Adaptive Scheduler Simulator Orchestrator

The orchestrator allows for running the adaptive scheduler in a simulated environment in order to facilitate testing.
It allows the user to dump input request data to the [Configuration Database](https://github.com/observatorycontrolsystem/configdb)
which is then passed to the scheduler. The orchestrator runs the scheduler and passes off the scheduler result to a
metric calculation file, which calculates metrics to send to an OpenSearch database. 