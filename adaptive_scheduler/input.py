#!/usr/bin/env python

'''
input.py - Read and marshal scheduling input (targets, telescopes, etc)

description

Author: Eric Saunders
November 2011
'''

from adaptive_scheduler.model    import ( Telescope, Target, Proposal, Molecule,
                                          Request, CompoundRequest )
from adaptive_scheduler.utils    import iso_string_to_datetime, EqualityMixin
import ast


class RequestProcessor(EqualityMixin):

    def __init__(self):
        pass


    def set_telescope_class_mappings(self, telescopes):
        '''Given a set of telescopes, determine the mappings between classes and
           telescope instances, and store them.'''

        tel_classes = {}

        for tel_name, cur_tel in telescopes.items():

            # If we've not seen this class of telescope before...
            if cur_tel.tel_class not in tel_classes:
                # ...create a new entry
                tel_classes[cur_tel.tel_class] = []

            # ...append this telescope to the appropriate class
            tel_classes[cur_tel.tel_class].append(cur_tel)

        self.telescope_classes = tel_classes

        return


    def expand_tel_class(self, compound_request):
        ''' Parse the provided CompoundRequest. If the provided telescope is actually
            a telescope class, expand the Request to an OR across all telescopes of
            that class. '''

        return compound_request




def file_to_dicts(filename):
    fh = open(filename, 'r')
    data = fh.read()

    return ast.literal_eval(data)


def build_telescopes(filename):
    telescopes = {}
    tel_dicts  = file_to_dicts(filename)

    for d in tel_dicts:
        telescopes[ d['name'] ] = Telescope(d)

    return telescopes


def build_targets(filename):
    targets = {}
    target_dicts  = file_to_dicts(filename)

    for d in target_dicts:
        targets[ d['name'] ] = Target(d)

    return targets


def build_proposals(filename):
    proposals = {}
    proposal_dicts = file_to_dicts(filename)

    for d in proposal_dicts:
        proposals[ d['proposal_name'] ] = Proposal(d)

    return proposals


def build_molecules(filename):
    molecules = {}
    molecule_dicts = file_to_dicts(filename)

    for d in molecule_dicts:
        molecules[ d['name'] ] = Molecule(d)

    return molecules


def build_requests(req_list, targets, telescopes, molecules, semester_start,
                   semester_end):
    '''
        This one is a little different from the other build methods, because
        Requests are always intended to be sub-components of a CompoundRequest
        object (even if there is only one Request (type single)).
    '''

    requests = []


    for d in req_list:

        dt_windows = []
        if 'window' in d:
            for str_start_date, str_end_date in d['window']:
                dt_windows.append(
                                   ( iso_string_to_datetime(str_start_date),
                                     iso_string_to_datetime(str_end_date) )
                                 )

        # If no windows are provided, default to the semester bounds
        else:
            dt_windows = [semester_start, semester_end]

        req = Request(
                       target    = targets[ d['target_name'] ],
                       telescope = telescopes[ d['telescope_name'] ],
                       molecule  = molecules[ d['molecule_name'] ],
                       windows   = dt_windows,
                       duration  = d['duration'],
                     )

        # Store the requested duration directly in the molecule
        req.molecule.duration = req.duration

        # Add the completed request to the list
        requests.append(req)

    return requests


def build_compound_requests(filename, targets, telescopes, proposals, molecules,
                            semester_start, semester_end):

    compound_requests = []
    request_dicts = file_to_dicts(filename)

    for d in request_dicts:
        requests = build_requests(d['requests'], targets, telescopes, molecules,
                                  semester_start, semester_end)

        compound_requests.append(
                                 CompoundRequest(
                                          res_type  = d['res_type'],
                                          proposal  = proposals[ d['proposal_name'] ],
                                          requests  = requests,
                                        )
                                )

    return compound_requests
