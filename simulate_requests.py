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
from random import randrange

from rise_set.astrometry import calc_apparent_sidereal_time
from reqdb.requests import Request, UserRequest
from reqdb.client import SchedulerClient


def file_to_dicts(filename):
    fh = open(filename, 'r')
    data = fh.read()

    return ast.literal_eval(data)


def get_proposal():

    proposal = {
                 'proposal_id'   : 'LCOSchedulerTest',
                 'user_id'       : 'eric.saunders',
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
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)


class RequestBuilder(object):

    def __init__(self, telescope, global_start, global_end):
        self.tel_info = telescope
        tel_name, obs, site = telescope['name'].split('.')
        self.location = {
                          'telescope_class' : telescope['tel_class'],
                          'site'            : site,
                          'observatory'     : obs,
                          'telescope'       : tel_name
                        }
        self.molecule = {
             # Required fields
             'exposure_time'   : 600,         # Exposure time, in secs
             'exposure_count'  : 2,           # The number of consecutive exposures
             'filter'          : 'V',         # The generic filter name
           }
        self.global_start = global_start
        self.global_end   = global_end


    def build(self, dt):
        req = Request()
        req.set_location(self.location)
        req.add_molecule(self.molecule)
        req.set_target(self.build_zenith_target(dt))
        req.add_window(self.build_window(dt))

        return req


    def build_window(self, dt):
        ONE_DAY = timedelta(days=1)
        start = dt - TWO_HOURS
        end   = dt + TWO_HOURS

        if start < self.global_start:
            start = self.global_start

        if end > self.global_end:
            end = self.global_end

        window = {
                    'start'    : str(start),
                    'end'      : str(end),
                  }

        return window


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

    args = parser.parse_args()

    TWO_WEEKS  = timedelta(days=14)
    TWO_MONTHS = timedelta(days=60)

    start_date = datetime.utcnow() + TWO_MONTHS
    end_date   = start_date + TWO_WEEKS

    locations = file_to_dicts('telescopes.dat')

    print "Making %d User Requests, starting between %s and %s" % (args.n, start_date, end_date)

    for i in range(args.n):
        r_builder = RequestBuilder(locations[0], start_date, end_date)

        random_start = random_date(start_date, end_date)
        print "%d) %s" % (i, random_start)
        req = r_builder.build(random_start)

        ur = UserRequest(group_id='Simulated Request')
        ur.add_request(req)
        ur.operator = 'single'
        ur.set_proposal(get_proposal())

        client = SchedulerClient('http://localhost:8001/requestdb/')
        response_data = client.submit(ur, debug=True)
        client.print_submit_response()
