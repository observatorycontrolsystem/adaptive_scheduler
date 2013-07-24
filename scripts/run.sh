#!/bin/sh
rsync -av eng@scheduler-dev:/var/log/adaptive_scheduler.err ../logs/
python2.7 plot_timings.py ../logs/adaptive_scheduler.err
