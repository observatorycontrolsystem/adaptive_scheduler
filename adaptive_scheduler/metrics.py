'''
metrics.py - Interface to scheduler performance metrics.

This module provides wrappers and infrastructure to translate metrics generated
from the scheduling kernel and elsewhere into output that can be served by the
scheduler viewing service.

Author: Eric Saunders
June 2012
'''

# Required for true (non-integer) division
from __future__ import division

from adaptive_scheduler.utils import ( increment_dict_by_value, datetime_to_epoch,
                                       normalised_epoch_to_datetime )

from datetime import datetime
import os.path


def convert_coverage_to_dmy(coverage, semester_start):
    ''' Given a coverage vector of the form:
            (start, end, value)
        convert the start/end times from kernel time to datetimes, and
        return the new structure. '''

    epoch_start = datetime_to_epoch(semester_start)

    dt_coverage = []
    for interval in coverage:
        dt_start = normalised_epoch_to_datetime(interval[0], epoch_start)
        dt_end   = normalised_epoch_to_datetime(interval[1], epoch_start)

        dt_interval = (dt_start, dt_end, interval[2])

        dt_coverage.append(dt_interval)

    return dt_coverage


def sum_contended_datetimes(dt_coverage):
    ''' Given a coverage vector of (start, end, value) tuples, sum the total amount
        of value associated with each individual UT day. Return this as a dictionary,
        keyed on the string representation of each date (e.g. "2011-11-02"). '''

    contended_dts = {}
    for interval in dt_coverage:
        contended = interval[2]
        if contended:
            increment_dict_by_value(contended_dts, str(interval[0].date()), contended)
            increment_dict_by_value(contended_dts, str(interval[1].date()), contended)

    return contended_dts


def dump_metric(metric, metric_name, dump_dir):
    ''' Write the metric datastructure to file, wrapped with a timestamp indicating
        when this was generated. '''

    dump_file = metric_name + '.dat'
    dump_path = os.path.join(dump_dir, dump_file)

    dump_fh = open(dump_path, 'w')

    timestamped_wrapper = {
                            'generated_at' : str(datetime.utcnow()),
                             metric_name   : metric
                          }
    print >> dump_fh, timestamped_wrapper

    dump_fh.close()

    return
