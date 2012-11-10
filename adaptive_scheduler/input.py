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
import cPickle


class RequestProcessor(object):

    def __init__(self):
        pass


    def set_telescope_class_mappings(self, telescopes):
        '''Given a set of telescopes, determine the mappings between classes and
           telescope instances, and store them. This makes it easy to find all
           telescopes of a particular class.'''

        tel_classes = {}

        for tel_name, cur_tel in telescopes.items():

            # If we've not seen this class of telescope before...
            if cur_tel.tel_class not in tel_classes:
                # ...create a new entry
                tel_classes[cur_tel.tel_class] = []

            # ...append this telescope to the appropriate class
            tel_classes[cur_tel.tel_class].append(cur_tel)

        self.telescope_classes = tel_classes
        self.telescopes = telescopes

        return


    def expand_tel_class(self, compound_request):
        ''' Parse the provided CompoundRequest. If the provided telescope is actually
            a telescope class, expand the CompoundRequest to a ONEOF (OR) across all
            telescopes of that class. '''

        processed_requests = []

        # Track the number of requests we expand
        n_expanded = 0

        # Go through each Request in turn...
        for request in compound_request.requests:

            # If the requested telescope is a class we know about...
            if request.telescope_name in self.telescope_classes:

                # ...then create a new request for each telescope in the class
                for telescope in self.telescope_classes[request.telescope_name]:

                    # Assume copying isn't needed, since originals won't be modified
                    new_request = Request(
                                           target    = request.target,
                                           telescope = telescope,
                                           molecule  = request.molecule,
                                           windows   = request.windows,
                                           duration  = request.duration
                                         )

                    processed_requests.append(new_request)

                    # Note that we just created a new Request
                    n_expanded += 1

            # Otherwise...
            else:
                #...resolve the name to a telescope instance, then store
                request.telescope = self.telescopes[request.telescope_name]
                processed_requests.append(request)

        # If we expanded more than one request...
        if n_expanded >= 2:
            # ...then this must be a oneof now
            compound_request.res_type = 'oneof'

        # TODO: Store pre-processed requests? Need careful copying!
        compound_request.requests = processed_requests

        return


def get_telescope_network(tel_file):
    ''' This function is a placeholder for the real thing, which will look up
        the current installed network from the config DB.'''

    tels = build_telescopes(tel_file)

    return tels


def get_requests_from_files(tel_file, target_file, proposal_file, molecule_file,
                            request_file, semester_start, semester_end):

    # Create telescopes, targets, proposals and requests from input files
    tels      = build_telescopes(tel_file)
    targets   = build_targets(target_file)
    proposals = build_proposals(proposal_file)
    molecules = build_molecules(molecule_file)

    # Configure a preprocessor to handle telescope classes in requests
    request_processor = RequestProcessor()
    request_processor.set_telescope_class_mappings(tels)


    # Combine the input information to reconstitute the actual compound requests
    compound_requests = build_compound_requests(request_file, targets, tels,
                                                proposals, molecules, semester_start,
                                                semester_end)

    # Expand all telescope classes into OR'd Requests
    for cr in compound_requests:
        request_processor.expand_tel_class(cr)

    return compound_requests


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

    #TODO: These are not complete Request objects, because telescope hasn't
    #      yet been resolved. Clean this up with the RequestProcessor.
    requests = []


    for d in req_list:

        # Construct the Request windows
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

        # Build the Request
        req = Request(
                       target         = targets[ d['name'] ],
                       molecule       = molecules[ d['name'] ],
                       windows        = dt_windows,
                       duration       = d['duration'],
                       telescope_name = d['telescope_name'],
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
                                          res_type = d['res_type'],
                                          proposal = proposals[ d['proposal_name'] ],
                                          requests = requests,
                                        )
                                )

    return compound_requests



def dump_scheduler_input(pickle_file, to_schedule, resource_windows,
                         contractual_obligation_list):

    dump_fh = open(pickle_file, 'w')
    cPickle.dump(to_schedule, dump_fh)
    cPickle.dump(resource_windows, dump_fh)
    cPickle.dump(contractual_obligation_list, dump_fh)
    dump_fh.close()

    return


def load_scheduler_input(pickle_file):
    in_fh = open(pickle_file, 'r')

    to_schedule                 = cPickle.load(in_fh)
    resource_windows            = cPickle.load(in_fh)
    contractual_obligation_list = cPickle.load(in_fh)

    return to_schedule, resource_windows, contractual_obligation_list
