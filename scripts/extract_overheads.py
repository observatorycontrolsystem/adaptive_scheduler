#!/usr/bin/env python

'''
extract_overheads.py - Determine the overheads incurred by a set of observations.

Information about each performed observation is stored in an ObservationReport, an
XML document generated at the telescope. These documents are later stored in the
POND and the telemetry database.

This code takes the output of a MySQL query generating a set of such documents, one
per line. It parses these documents to deduce the overhead time.

* We assume that only molecules of type EXPOSE and STANDARD are interesting.
* We assume that only observations which completed successfully represent normal
operation.
* We calculate the overhead by finding the time when the observation first begins to
be processed by the sequencer, and the time when the sequencer finally calls the
command to expose. We define the overhead as all the preparation time between these
two events.
* Note that subsequent to the exposure completing, there is some readout time, and
this is not captured here.

Author: Eric Saunders
June 2012
'''
from __future__ import division

import sys
from xml.sax import handler, parseString

from datetime import datetime
import re
import calendar


class ObservationReportHandler(handler.ContentHandler):
    '''Handler for observationreport XML documents.'''

    def __init__(self, debug=False):
        self.reset_state()
        self.debug = debug

        handler.ContentHandler.__init__(self)


    def reset_state(self):
        self.info = { 'useful_type'  : False,
                      'useful_state' : False,
                      'start'        : None,
                      'end'          : None  }

        self.not_seen_a_state_yet = True
        self.inside_state_elem    = False
        self.state_elem_contents  = None

        self.not_seen_a_start_yet = True
        self.inside_start_elem    = False

        self.not_seen_an_end_yet  = True
        self.inside_end_elem      = False

        self.not_seen_an_exposure_event_yet = True
        self.inside_exposure_event_elem = False
        self.inside_exposure_event_time = False



    def startElement(self, name, attrs):
        '''Called whenever a new element is encountered.'''

        if name == 'ns2:observationreport':
            mol_type = attrs.getValue('type')
            self.info['mol_type'] = mol_type

            if self.debug:
                print "Found molecule type:", mol_type

            if (mol_type == 'STANDARD') or (mol_type == 'EXPOSE'):
                self.info['useful_type'] = True
            else:
                self.info['useful_type'] = False

        if (name == 'state'):
            self.inside_state_elem = True
        else:
            self.inside_state_elem = False

        if (name == 'start'):
            self.inside_start_elem = True
        else:
            self.inside_start_elem = False

        if (name == 'end'):
            self.inside_end_elem = True
        else:
            self.inside_end_elem = False

        if (name == 'event' and
            attrs.getValue('command') == "org.lcogt.sequencer.command.instrument.StartExposureCommand" ):

            self.inside_exposure_event_elem = True

        if (self.inside_exposure_event_elem and name == 'time'):
            self.inside_exposure_event_time = True
        else:
            self.inside_exposure_event_time = False


    def characters(self, data):
        '''Called whenever raw data is found inside a tag.'''

        if self.not_seen_a_state_yet:

            if self.inside_state_elem:
                self.state_elem_contents = data
                if self.debug:
                    print "Found state of", data

                if data == 'DONE':
                    self.info['useful_state'] = True

                self.not_seen_a_state_yet = False


        if self.not_seen_a_start_yet:

            if self.inside_start_elem:
                self.info['start'] = data
                if self.debug:
                    print "Found start time of", data

                self.not_seen_a_start_yet = False


        if self.not_seen_an_end_yet:

            if self.inside_end_elem:
                self.info['end'] = data
                if self.debug:
                    print "Found end time of", data

                self.not_seen_an_end_yet = False


        if self.not_seen_an_exposure_event_yet:

            if self.inside_exposure_event_time:
                self.info['exposure_started'] = data
                if self.debug:
                    print "Found exposure start at", data

                self.inside_exposure_event_time = False
                self.not_seen_an_exposure_event_yet = False


    def is_useful(self):

        return self.info['useful_type'] and self.info['useful_state']


def in_seconds(td):
    '''Timedelta objects don't have any way to return their size in seconds, prior
    to Python 2.7. This implementation is taken from the docs.'''
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6


def determine_datetime_format(dt):
    datetime_format_millis    = '%Y-%m-%dT%H:%M:%S.%fZ'
    datetime_format_no_millis = '%Y-%m-%dT%H:%M:%SZ'

    regex_millis    = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z'
    regex_no_millis = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z'
    if re.match(regex_millis, dt):
        return datetime_format_millis
    elif re.match(regex_no_millis, dt):
        return datetime_format_no_millis
    else:
        return ''


def parse_datetime(dt):
    datetime_format = determine_datetime_format(dt)
    return datetime.strptime(dt, datetime_format)


def increment_dict(dictionary, key):
    if key in dictionary:
        dictionary[key] += 1
    else:
        dictionary[key]  = 1


def print_stats(n_useful, n_reports, totals, useful_totals, unique_loc_instr):
    print "Final stats (%d/%d useful observing reports):" % (n_useful, n_reports)
    for mol_type in totals:
        print "%14s: %d/%d" % (
                                mol_type,
                                useful_totals.get(mol_type, 0),
                                totals[mol_type]
                              )
    print
    print "Reports come from %d unique location-instrument combinations:" % (
                                                         len(unique_loc_instr.keys())
                                                        )

    for loc_instr in sorted(unique_loc_instr):
        print loc_instr
    print


def print_summary_line(n, summary, out_fh, in_unix_time=None):

    start = summary[n]['start']
    if in_unix_time:
        start = calendar.timegm(start.timetuple())

    print >> out_fh, "%-11d %-30s %-16s %-12s %s" % (
                                                 n,
                                                 start,
                                                 summary[n]['overhead'],
                                                 in_seconds(summary[n]['overhead']),
                                                 in_seconds(summary[n]['non-overhead'])
                                              )
    return


def write_overheads_to_file(loc_instr, n, summary, out_fh, in_unix_time=False):

    start_header = "Start (datetime)"
    if in_unix_time:
        start_header = "Start(unixepoch)"


    print >> out_fh, "Calculated %d overhead times:" % n
    print >> out_fh, ("Report line " + start_header +
                     "               Overhead (h:m:s) Overhead (s) Non-overhead (s)")

    # Tabulate the overheads
    for n in sorted(summary):
        if ( summary[n]['location'] == loc_instr ):
            print_summary_line(n, summary, out_fh, in_unix_time)

    return



if __name__ == '__main__':

    debug = False
    xml_filename = sys.argv[1]
    if len(sys.argv) > 2:
        debug = True

    # Toggle for datetime/unix time output for start times
    in_unix_time = True

    xml_fh = open(xml_filename, 'r')

    error_log = 'errors.out'
    error_fh  = open(error_log, 'w')

    handler = ObservationReportHandler(debug)

    totals             = {}
    useful_totals      = {}
    unique_loc_instr   = {}
    summary            = {}
    n_useful  = 0
    n_reports = 0
    for i, line in enumerate(xml_fh):

        # Enumerate starts at 0, but line numbers start at 1
        line_number = i + 1

        if debug:
            print "\nHandling line number:", line_number

        # Skip comments
        if line.startswith('#'):
            continue

        # Chop up the meta data about the observing report
        cols = line.split()
        history_data = {
                         'history_id'   : cols[0],
                         'sb_id'        : cols[1],
                         'location'     : cols[2],
                         'overhead'     : None,
                         'non-overhead' : None
                       }

        # We split on spaces earlier, so need to stick the XML back together
        observing_report = " ".join(cols[3:])

        # Initialise the handler, and parse the XML of the observing report
        handler.reset_state()
        parseString(observing_report, handler)

        # Summarise the types of observation recorded
        increment_dict(totals, handler.info['mol_type'])
        increment_dict(unique_loc_instr, history_data['location'])

        # For successful observations of the right type, calculate the overhead time
        if handler.is_useful():
            n_useful += 1

            increment_dict(useful_totals, handler.info['mol_type'])

            # Pull out the datetime, which could be in a couple of different formats
            try:
                exp_start = parse_datetime(handler.info['exposure_started'])
                start     = parse_datetime(handler.info['start'])
                end       = parse_datetime(handler.info['end'])

            # We can still fail on timezones, broken/missing times, etc.
            except ValueError as e:
                print >> error_fh, "Input line %d: Couldn't parse datetime" % line_number
                print >> error_fh, "Error was:", e

            # Calculate the duration of the overhead, and the exposure
            history_data['start']        = start
            history_data['overhead']     = exp_start - start
            history_data['non-overhead'] = end - exp_start
            summary[i] = history_data

        n_reports += 1


    # Summarise what we read
    print_stats(n_useful, n_reports, totals, useful_totals, unique_loc_instr)

    for loc_instr in unique_loc_instr:
        out_filename = loc_instr + '_overheads.dat'
        n = len([i for n in summary if summary[n]['location'] == loc_instr])
        if n > 0:
            out_fh = open(out_filename, 'w')
            print "Writing overheads to file:", out_filename
            write_overheads_to_file(loc_instr, n, summary, out_fh, in_unix_time)
            out_fh.close()



    # Clean up
    xml_fh.close()
    error_fh.close()
