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

import sys
from xml.sax import handler, parseString

from datetime import datetime


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


        if self.inside_exposure_event_time:
            self.info['exposure_started'] = data
            if self.debug:
                print "Found exposure start at", data

            self.inside_exposure_event_time = False


    def is_useful(self):

        return self.info['useful_type'] and self.info['useful_state']


def in_seconds(td):
    '''Timedelta objects don't have any way to return their size in seconds, prior
    to Python 2.7. This is taken from the docs.'''
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6


if __name__ == '__main__':

    debug = False
    xml_filename = sys.argv[1]

    xml_fh = open(xml_filename, 'r')

    handler = ObservationReportHandler(debug)

    totals         = {}
    overhead_times = {}
    non_overhead_times = {}
    useful = 0
    for i, report in enumerate(xml_fh):
        if debug:
            print "\nHandling report number:", i
        handler.reset_state()
        parseString(report, handler)

        # Summarise the types of observation recorded
        if handler.info['mol_type'] in totals:
            totals[handler.info['mol_type']] += 1
        else:
            totals[handler.info['mol_type']] = 1

        # For successful observations of the right type, calculate the overhead time
        if handler.is_useful():
            useful += 1
            time_format = '%Y-%m-%dT%H:%M:%S.%fZ'
            exp_start   = datetime.strptime(handler.info['exposure_started'], time_format)
            start = datetime.strptime(handler.info['start'], time_format)
            end   = datetime.strptime(handler.info['end'], time_format)
            print end.time(), exp_start.time(), start.time()
            overhead_times[i] = exp_start - start
            non_overhead_times[i] = end - exp_start


    # Summarise what we read
    print "Final stats (%d/%d useful observing reports):" % (useful, i)
    for mol_type, number in totals.iteritems():
        print "%14s: %d" % ( mol_type, number )
    print

    # Tabulate the overheads
    print "Calculated %d overhead times:" % len(overhead_times.keys())
    print "Report line Overhead (h:m:s) Overhead (s) Non-overhead(s)"
    for report_number in sorted(overhead_times):
        print "%-11d %-16s %-12s %s" % (report_number,
                            overhead_times[report_number],
                            in_seconds(overhead_times[report_number]),
                            in_seconds(non_overhead_times[report_number]))
