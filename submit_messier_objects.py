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

    proposal_info = (  #  Proposal Name          Proposal ID   Group ID
                       ( 'Messier relationship', 'MESSIER-01', 'do stuff1' ),
                       ( 'Messier painting',     'MESSIER-02', 'do stuff2' ),
                       ( 'Messier room',         'MESSIER-03', 'do stuff3' ),
                       ( 'Messier hair',         'MESSIER-04', 'do stuff4' )
                     )

    user_info = (  # User name    User ID
                   ( 'esaunders', '1' ),
                   ( 'slampoud',  '2' ),
                   ( 'mbecker',   '3' ),
                   ( 'mnorbury',  '4' ),
                   ( 'tlister',   '5' )
                 )

    selected_proposal = random.choice(proposal_info)
    selected_user     = random.choice(user_info)

    return Proposal(
                     proposal_name = selected_proposal[0],
                     proposal_id   = selected_proposal[1],
                     user_name     = selected_user[0],
                     user_id       = selected_user[1],
                     group_id      = selected_proposal[2],
                     obs_id         = 'face',   # TODO: THIS IS DUE TO CHANGE
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
                                              type        = request.molecule.type,
                                              filter      = request.molecule.filter,
                                              binning     = request.molecule.binning,
                                              exposure_time = request.duration
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


def build_single_compound_requests(requests):
    ''' The simplest thing we can do. Wrap each Request in a 'single'
        Compound Request and return it.'''
    compound_requests = []
    for request in requests:
        proposal   = build_random_proposal()
        compound_request = CompoundRequest(
                                            res_type = 'single',
                                            proposal = proposal,
                                            requests = [request]
                                          )
        compound_requests.append(compound_request)

    return compound_requests


def build_nested_compound_requests(requests):
    '''Given a set of Requests, construct a set of Compound Requests, with one
       level of nesting, randomly distributing the Requests.'''

    possible_types = (
                       'single',
                       'and',
                       'oneof'
                     )

    # We use the request list as a stack, pulling off to populate our CRs
    compound_requests = []
    while requests:
        proposal = build_random_proposal()

        # We can't choose ANDs or ONEOFs if we only have one request left
        if len(requests) >= 2:
            chosen_type = random.choice(possible_types)
        elif len(requests) == 1:
            chosen_type = 'single'

        # SINGLEs are straighforward...
        if chosen_type == 'single':
            chosen_request = requests.pop(0)
            compound_request = CompoundRequest(
                                                res_type = chosen_type,
                                                proposal = proposal,
                                                requests = [chosen_request]
                                              )

        # We've got a more interesting compound request...
        else:
            # Set up the first two requests...
            chosen_requests = list((requests.pop(0), requests.pop(0)))

            # Randomly decide whether to add further requests...
            while requests:
                if random.random() > 0.5:
                    chosen_requests.append(requests.pop(0))
                else:
                    break

            # Package it all up
            compound_request = CompoundRequest(
                                                res_type = chosen_type,
                                                proposal = proposal,
                                                requests = chosen_requests
                                              )

        compound_requests.append(compound_request)


    return compound_requests



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

    duration = 60

    WIN_MIN_DURATION = 30 * 60            # Windows must be at least 30 minutes wide
    WIN_MAX_DURATION  = 3 * 24 * 60 * 60  # Windows can't be longer than 3 days

    requests = []
    for target in targets:
        window = make_random_window(semester_start, semester_end,
                                    WIN_MIN_DURATION, WIN_MAX_DURATION)
        telescope_class = select_random_telescope_class()
        request = build_request(target, molecule, window, duration, telescope_class)

        requests.append(request)

    print "%d requests before compounding..." % len(requests)

    #compound_requests = build_single_compound_requests(requests)
    compound_requests = build_nested_compound_requests(requests)

    tally = 0
    for cr in compound_requests:
        print cr.res_type, len(cr.requests)
        tally += len(cr.requests)

    print "%d requests after compounding..." % tally

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
                                   proposal_id   = proposal.proposal_id,
                                   user_name     = proposal.user_name,
                                   user_id       = proposal.user_id,
                                   group_id      = proposal.group_id,
                                   obs_id        = 'your face is dumb'
                                 )

        url = client.build_request_url()
        #print url
        client.submit_request(url)

        #print client_c_req.requests
