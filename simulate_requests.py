#!/usr/bin/env python

'''
simulate_requests.py - Fill the Request DB with simulated Requests

description

Author: Eric Saunders
May 2013
'''
import argparse
import ast
from datetime import datetime, timedelta
import random

from rise_set.astrometry import calc_apparent_sidereal_time
from rise_set.sky_coordinates import RightAscension, Declination
from reqdb.requests import Request, UserRequest
from reqdb.client import SchedulerClient


def file_to_dicts(filename):
    fh = open(filename, 'r')
    data = fh.read()

    return ast.literal_eval(data)


def get_proposal():

    proposal = {
                 'proposal_id'   : 'LCOSchedulerTest',
                 'user_id'       : 'esaunders@lcogt.net',
                 'tag_id'        : 'LCOGT',
                 'priority'      : 20               # 'TAC priority': LARGER means MORE IMPORTANT
               }

    return proposal


def random_date(start, end):
    """
    This function will return a random datetime between two datetime
    objects, with one second precision.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + timedelta(seconds=random_second)


class RequestBuilder(object):

    def __init__(self, global_start, global_end):

        self.location = {
                      'telescope_class' : '1m0',
                      }

        self.molecule = {
             # Required fields
             'exposure_time'   : 600,         # Exposure time, in secs
             'exposure_count'  : 2,           # The number of consecutive exposures
             'filter'          : 'V',         # The generic filter name
           }
        self.global_start = global_start
        self.global_end   = global_end


    def build(self, start, end, random_target=False):

        req = Request()
        req.set_location(self.location)
        req.add_molecule(self.molecule)

        if random_target:
            req.set_target(self.build_random_target())

        req.add_window(self.build_window(start, end))

        return req


    def build_window(self, start, end):
        if start < self.global_start:
            start = self.global_start

        if end > self.global_end:
            end = self.global_end

        window = {
                    'start'    : str(start),
                    'end'      : str(end),
                  }

        return window


    def build_random_target(self):
        random_ra  = RightAscension(degrees=random.uniform(0, 359.9999999))
        random_dec = Declination(degrees=random.uniform(-89.99999999, 89.9999999))

        target = {
               # Required fields
               'name'              : 'Simulated target',
               'ra'                : random_ra.in_degrees(),            # In decimal degrees
               'dec'               : random_dec.in_degrees(),           # In decimal degrees
        }

        return target


    def build_zenith_target(self, dt):
        ra  = calc_apparent_sidereal_time(dt)
        dec = self.tel_info['latitude']

        target = {
               # Required fields
               'name'              : 'Simulated target',
               'ra'                : ra.in_degrees(),            # In decimal degrees
               'dec'               : dec,           # In decimal degrees
        }

        return target


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Fill the Request DB with simulated Requests")
    parser.add_argument("n", type=int, help="Number of User Requests to create")
    parser.add_argument("--random-target", help="Generate fully randomised RA/Decs",
                    action="store_true")

    args = parser.parse_args()

    TWO_WEEKS  = timedelta(days=14)
    TWO_MONTHS = timedelta(days=60)
    SIX_MONTHS = timedelta(days=180)

    # We expect RA range 1-13 to be visible
#    SEMESTER_2013B_START = datetime(2013, 10, 1)
    SEMESTER_2013B_START = datetime(2013, 9, 4)

#    start_date = datetime.utcnow() + TWO_MONTHS
    start_date = SEMESTER_2013B_START
#    end_date   = start_date + SIX_MONTHS
    end_date   = start_date + TWO_WEEKS

#    locations = file_to_dicts('telescopes.dat.max_1ms')

    print "Making %d User Requests, starting between %s and %s" % (args.n, start_date, end_date)

    if args.random_target:
        print "Constructing fully randomised RA/Decs"
    else:
        print "Placing targets at approximate zenith"


    r_builder = RequestBuilder(start_date, end_date)
    for i in range(args.n):

        print "%d) %s" % (i, start_date)
        req = r_builder.build(start_date, end_date, random_target=args.random_target)

        ur = UserRequest(group_id='Simulated Request')
        ur.add_request(req)
        ur.operator = 'single'
        ur.set_proposal(get_proposal())

#        client = SchedulerClient('http://localhost:8001/requestdb/')
        client = SchedulerClient('http://localhost:8001/')
        response_data = client.submit(ur, debug=True)
        client.print_submit_response()
