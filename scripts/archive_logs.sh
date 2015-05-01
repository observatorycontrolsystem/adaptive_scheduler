#!/bin/bash
# Wrapper for archive_logs.py - compress scheduler UR log files that
#                               aren't being updated anymore
# Eric Saunders
# January 2015

SCHEDULER_ENV=/lco/env/scheduler
SCHEDULER_HOME=/lco/adaptive_scheduler

MTIME=60
UR_LOG_DIR=${SCHEDULER_HOME}/logs
ARCHIVE_LOG_DIR=${SCHEDULER_HOME}/archived_logs

source ${SCHEDULER_ENV}/bin/activate
date
echo "Archiving old UR log files"
exec python ${SCHEDULER_HOME}/scripts/archive_logs.py --mtime=${MTIME} --log_dir=${UR_LOG_DIR} --archive_dir=${ARCHIVE_LOG_DIR}
