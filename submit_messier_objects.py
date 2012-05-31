#!/usr/bin/env python

'''
submit_messier_objects.py - Fill the scheduler Request DB with Messier objects

This script submits Messier objects to the scheduler Request DB.

Author: Eric Saunders
May 2012
'''

import ast
from datetime import datetime
import random

from adaptive_scheduler.input import get_telescope_network
from adaptive_scheduler.model import ( Target, Proposal, Molecule,
                                       Request, CompoundRequest )
from adaptive_scheduler.utils import datetime_to_epoch, epoch_to_datetime

import client.requests
import client.request_parts
from client.client import SchedulerClient



def load(path_to_messier):

    fh = open(path_to_messier, 'r')

    messier_objects = ast.literal_eval(fh.read())

    return messier_objects


def make_sexegesimal_hr_min_sec_string(hr, min, sec):
    return "%s %s %s" % (hr, min, sec)


def build_targets(messier_objects):

    targets = []
    for mo in messier_objects.values():

        print mo['target']['name']

        ra_sex_string = make_sexegesimal_hr_min_sec_string(
                                                            mo['ra']['h'],
                                                            mo['ra']['m'],
                                                            mo['ra']['s']
                                                          )

        dec_sex_string = make_sexegesimal_hr_min_sec_string(
                                                             mo['dec']['d'],
                                                             mo['dec']['m'],
                                                             mo['dec']['s']
                                                           )

        target = Target(
                         name  = mo['target']['name'],
                         ra    = ra_sex_string,
                         dec   = dec_sex_string,
                         epoch = mo['equinox']
                       )

        targets.append(target)


    return targets


def build_random_proposal():

    proposal_names = (
                       'Messier relationship',
                       'Messier painting',
                       'Messier room',
                       'Messier hair'
                     )

    user_names = (
                   'esaunders',
                   'mbecker',
                   'mnorbury',
                   'tlister'
                 )

    return Proposal(
                     proposal_name = random.choice(proposal_names),
                     user_name     = random.choice(user_names)
                   )


def build_test_molecule():

    return Molecule(
                     type            = 'expose_n',
                     count           = 1,
                     binning         = '2',
                     instrument_name = 'KB12',
                     filter          = 'BSSL-UX-020',
                    )


def build_request(target, molecule, windows, duration, telescope_class):

    return Request(
                    target         = target,
                    molecule       = molecule,
                    windows        = windows,
                    duration       = duration,
                    telescope_name = telescope_class
                  )


def req_to_client_req(request):


    loc = client.request_parts.Location(
                                         telescope_class = request.telescope_name,
                                       )
    target = client.request_parts.SiderealTarget(
                                          name = request.target.name,
                                          ra   = request.target.ra.in_sexegesimal(),
                                          dec  = request.target.dec.in_sexegesimal()
                                        )
    molecule = client.request_parts.Molecule(
                                              description = request.molecule.type,
                                              filter      = request.molecule.filter,
                                              binning     = request.molecule.binning,
                                              duration    = request.duration
                                            )
    window = client.request_parts.Window(
                                          start = str(request.windows[0]),
                                          end   = str(request.windows[1])
                                        )


    client_req = client.requests.Request()
    client_req.set_location(loc)
    client_req.set_target(target)
    client_req.add_molecule(molecule)
    client_req.add_window(window)

    return client_req


def c_req_to_client_c_req(compound_request):

    client_c_req = client.requests.CompoundRequest()
    for request in compound_request.requests:
        client_req = req_to_client_req(request)
        client_c_req.add_request(client_req)

    client_c_req.compound(compound_request.res_type)
    return client_c_req


def make_random_window(start, end, min_duration=None, max_duration=None):
    epoch_start = datetime_to_epoch(start)
    epoch_end   = datetime_to_epoch(end)

    if not min_duration:
        min_duration = 0

    if not max_duration:
        max_duration = epoch_end - epoch_start

    # Guarantee our window will be at least min_duration in size
    # And not bigger than max_duration
    window_length = available_space = epoch_end - epoch_start
    while ( available_space < min_duration ) or ( window_length > max_duration ):
        random_start = random.randrange(epoch_start, epoch_end)
        random_end   = random.randrange(random_start, epoch_end)
        available_space = epoch_end - random_start
        window_length   = random_end - random_start

    window_start = epoch_to_datetime(random_start)
    window_end   = epoch_to_datetime(random_end)

    return window_start, window_end


def select_random_telescope_class():
    possible_telescope_classes = (
                                   '0m4',
                                   '1m0',
                                   '2m0'
                                 )

    return random.choice(possible_telescope_classes)


if __name__ == '__main__':

    tel_file        = 'telescopes.dat'
    path_to_messier = 'messier_catalog.dict'
    req_db_url      = 'http://mbecker-linux2.lco.gtn:8001/submit/'

    semester_start  = datetime(2012, 5, 1)
    semester_end    = datetime(2012, 5, 31)

    messier_objects = load(path_to_messier)


    telescopes = get_telescope_network(tel_file)
    targets    = build_targets(messier_objects)
    molecule   = build_test_molecule()


    duration        = '01:00:00'

    WIN_MIN_DURATION = 30 * 60            # Windows must be at least 30 minutes wide
    WIN_MAX_DURATION  = 3 * 24 * 60 * 60  # Windows can't be longer than 3 days

    requests = []
    for target in targets:
        window = make_random_window(semester_start, semester_end,
                                    WIN_MIN_DURATION, WIN_MAX_DURATION)
        telescope_class = select_random_telescope_class()
        request = build_request(target, molecule, window, duration, telescope_class)

        requests.append(request)


    # For simplicity, wrap each Request in a 'single' Compound Request
    compound_requests = []
    for request in requests:
        proposal   = build_random_proposal()
        compound_request = CompoundRequest(
                                            res_type = 'single',
                                            proposal = proposal,
                                            requests = [request]
                                          )
        compound_requests.append(compound_request)

    # Map to client request objects
    client_c_reqs = []
    for compound_request in compound_requests:
        client_c_req = c_req_to_client_c_req(compound_request)
        client_c_reqs.append(client_c_req)

    # Now submit
    for i, client_c_req in enumerate(client_c_reqs):
        proposal = compound_requests[i].proposal
        client   = SchedulerClient(req_db_url, client_c_req)
        client.submit_credentials(
                                   proposal_name = proposal.proposal_name,
                                   user_name     = proposal.user_name
                                 )

        url = client.build_request_url()
        #print url
        client.submit_request(url)

        #print client_c_req.requests