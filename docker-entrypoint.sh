#!/bin/sh

# Set sensible defaults for these parameters (overwritten if the env variables are set in docker-compose.yml)
SCHEDULER_SLEEP=${SCHEDULER_SLEEP:-60}
SCHEDULER_TIMELIMIT=${SCHEDULER_TIMELIMIT:-900}
SCHEDULER_HORIZON=${SCHEDULER_HORIZON:-7}
SCHEDULER_SLICESIZE=${SCHEDULER_SLICESIZE:-300}
OBSERVATION_PORTAL_URL=${OBSERVATION_PORTAL_URL:-http://127.0.0.1:8000}
CONFIGDB_URL=${CONFIGDB_URL:-http://127.0.0.1:7000}
DOWNTIMEDB_URL=${DOWNTIMEDB_URL:-http://127.0.0.1:7500}
ELASTICSEARCH_URL=${ELASTICSEARCH_URL:-http://elasticsearch-path:9200}

python3.6 as.py -p ${OBSERVATION_PORTAL_URL} -c ${CONFIGDB_URL} --downtime_url=${DOWNTIMEDB_URL} --elasticsearch_url=${ELASTICSEARCH_URL} --elasticsearch_index='live-telemetry' --elasticsearch_excluded_observatories='igla' -s ${SCHEDULER_SLEEP} -l ${SCHEDULER_TIMELIMIT} -i ${SCHEDULER_HORIZON} -z ${SCHEDULER_SLICESIZE} ${SCHEDULER_EXTRA_VARS} "${SCHEDULER_NOW}"
