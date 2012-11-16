#!/usr/bin/env python

'''
model2.py - summary line

description

Author: Eric Saunders
July 2012
'''

# Required for true (non-integer) division
from __future__ import division
from rise_set.sky_coordinates import RightAscension, Declination
from adaptive_scheduler.utils import ( iso_string_to_datetime, EqualityMixin,
                                       DefaultMixin )
from adaptive_scheduler.kernel.reservation_v2 import CompoundReservation_v2 as CompoundReservation
from adaptive_scheduler.exceptions import InvalidRequestError

import ast


def file_to_dicts(filename):
    fh = open(filename, 'r')
    data = fh.read()

    return ast.literal_eval(data)


def dict_to_model(dict_repr):

    user_request = build_user_request(dict_repr)



class DataContainer(DefaultMixin):
    def __init__(self, *initial_data, **kwargs):
        for dictionary in initial_data:
            for key in dictionary:
                setattr(self, key, dictionary[key])
        for key in kwargs:
            setattr(self, key, kwargs[key])



class Target(DataContainer):

    def list_missing_fields(self):
        req_fields = ('name', 'ra', 'dec')
        missing_fields = []

        for field in req_fields:
            try:
                getattr(self, field)
            except:
                missing_fields.append(field)

        return missing_fields

    # Use accessors to ensure we always have valid coordinates
    def get_ra(self):
        return self._ra

    def set_ra(self, ra):
        #TODO: Check units are accurate
        print 'Setting RA in target:', ra
        self._ra = RightAscension(degrees=ra)

    def set_dec(self, dec):
        #TODO: Check units are accurate
        print 'Setting Dec in target:', dec
        self._dec = Declination(dec)

    def get_dec(self):
        return self._dec

    ra  = property(get_ra, set_ra)
    dec = property(get_dec, set_dec)



class Molecule(DataContainer):
    #TODO: This is really an expose_n molecule, so should be specialised
    #TODO: Specialisation will be necessary once other molecules are scheduled

    def list_missing_fields(self):
        req_fields = ('type', 'exposure_count', 'bin_x', 'bin_y',
                      'instrument_name', 'filter', 'exposure_time')
        missing_fields = []

        for field in req_fields:
            try:
                getattr(self, field)
            except:
                missing_fields.append(field)

        return missing_fields



class Window(DefaultMixin):
    def __init__(self, window_dict):
        self.start = iso_string_to_datetime(window_dict['start'])
        self.end   = iso_string_to_datetime(window_dict['end'])



class Proposal(DataContainer):
    def list_missing_fields(self):
        req_fields = ('proposal_name', 'proposal_id',
                      'user_name', 'user_id',
                      'tag_id')
        missing_fields = []

        for field in req_fields:
            try:
                getattr(self, field)
            except:
                missing_fields.append(field)

        return missing_fields



class Telescope(DataContainer):
    pass



class Request(DefaultMixin):
    '''
        Represents a single valid configuration where an observation could take
        place. These are combined within a CompoundRequest to allow AND and OR
        semantics ("do this and this and this", "do this or this").

        target    - a Target object (pointing information)
        molecules - a list of Molecule objects (detailed observing information)
        windows   - a list of start/end datetimes, representing when this observation
                    is eligible to be performed. For user observations with no
                    time constraints, this should be the planning window of the
                    scheduler (e.g. the semester bounds).
        duration  - exposure time of each observation. TODO: Clarify what this means.
        telescope - a Telescope object (lat/long information)
    '''

    def __init__(self, target, molecules, windows, telescope, request_number):

        self.target         = target
        self.molecules      = molecules
        self.windows        = windows
        self.telescope      = telescope
        self.request_number = request_number

    def get_duration(self):
        '''This is a placeholder for a more sophisticated duration function, that
           does something clever with overheads. For now, it just sums the exposure
           times of the molecules, and adds an almost arbitrary overhead.'''

        #TODO: Placeholder for more sophisticated overhead scheme

        # Pick a sensible sounding overhead, in seconds
        overhead_per_molecule = 20
        duration = 0
        for mol in self.molecules:
            duration += mol.exposure_count * mol.exposure_time
            duration += overhead_per_molecule

        return duration

    # Define properties
    duration = property(get_duration)


class CompoundRequest(DefaultMixin):
    '''
        A user-level request for an observation. This will be translated into the
        Reservation/CompoundReservation of the scheduling kernel.

        operator - the type of compound request (single, and, oneof)
        proposal - proposal meta information associated with this request
        requests - a list of Request objects. There must be at least one.
    '''

    valid_types = CompoundReservation.valid_types

    def __init__(self, operator, requests):
        self.operator  = self._validate_type(operator)
        self.requests  = requests


    def _validate_type(self, provided_operator):
        '''Check the operator type being asked for matches a valid type
           of CompoundObservation.'''

        if provided_operator not in CompoundRequest.valid_types:

            error_msg = ("You've asked for a type of request that doesn't exist. "
                         "Valid operator types are:\n")

            for res_type, help_txt in CompoundRequest.valid_types.iteritems():
                error_msg += "    %9s - %s\n" % (res_type, help_txt)

            raise InvalidRequestError(error_msg)

        return provided_operator


    def get_duration(self):
        '''The duration of a CompoundRequest is just the sum of the durations of
           its sub-requests.'''

        duration = 0
        for req in self.requests:
            duration += req.duration()

        return duration


    # Define properties
    duration = property(get_duration)



class UserRequest(CompoundRequest, DefaultMixin):
    '''UserRequests are just top-level CompoundRequests. They differ only in having
       access to proposal and expiry information.'''

    def __init__(self, operator, requests, proposal, expires, tracking_number):
        CompoundRequest.__init__(self, operator, requests)

        self.proposal = proposal
        self.expires  = expires
        self.tracking_number = tracking_number


    def get_priority(self):
        '''This is a placeholder for a more sophisticated priority function. For now,
           it is just a pass-through to the proposal (i.e. TAC-assigned) priority.'''

        #TODO: Placeholder for more sophisticated priority scheme
        return self.proposal.priority

    # Define properties
    priority = property(get_priority)




class TelescopeNetwork(object):

    def __init__(self, tel_file):
        self.tels = self.get_telescope_network(tel_file)

        valid_locations = []
        for tel_name, tel in self.tels.iteritems():
            valid_locations.append(tel_name + '.' + tel.tel_class)

        self.le = LocationExpander(valid_locations)


    def get_telescope_network(self, tel_file):
        ''' This function is a placeholder for the real thing, which will look up
            the current installed network from the config DB.'''

        if not hasattr(self, 'tels'):
            self.tels = self.build_telescopes_from_file(tel_file)

        return self.tels


    def build_telescopes_from_file(self, tel_file):
        telescopes = {}
        tel_dicts  = file_to_dicts(tel_file)

        # Key the telescopes on <name.tel_class>
        for d in tel_dicts:
            telescopes[ d['name'] ] = Telescope(d)

        return telescopes


    def get_telescope(self, tel_name):
        return self.tels.get(tel_name, None)


    def get_telescopes_at_location(self, location):
        locations = self.le.expand_locations(location)

        telescopes = []
        for l in locations:
            tel = self.get_telescope(l)
            telescopes.append(tel)

        return telescopes



class ModelBuilder(object):

    def __init__(self, tel_file):
        self.tel_network = TelescopeNetwork(tel_file)


    def build_user_request(self, cr_dict):
        requests  = self.build_requests(cr_dict['requests'])
        proposal  = Proposal(cr_dict['proposal'])
        expiry_dt = iso_string_to_datetime(cr_dict['expires'])

        user_request = UserRequest(
                                    operator = cr_dict['operator'],
                                    requests = requests,
                                    proposal = proposal,
                                    expires  = expiry_dt,
                                    tracking_number = cr_dict['tracking_number']
                                  )

        return user_request


    def build_requests(self, req_dicts):
        requests = []
        for req_dict in req_dicts:
            expanded_requests = self.build_and_expand_request(req_dict)

            # if there is more than one request after expansion, they need to be
            # wrapped in a ONEOF CompoundRequest
            if len(expanded_requests) >= 2:
                req = CompoundRequest(
                                       operator = 'oneof',
                                       requests = expanded_requests,
                                     )
            else:
                req = expanded_requests[0]

            requests.append(req)

        return requests


    def build_and_expand_request(self, req_dict):
        target = Target(req_dict['target'])

        molecules = []
        for mol_dict in req_dict['molecules']:
            molecules.append(Molecule(mol_dict))

        windows = []
        for window_dict in req_dict['windows']:
            windows.append(Window(window_dict))


        telescopes = self.tel_network.get_telescopes_at_location(req_dict['location'])

        # Build a Request for each expanded location
        requests = []
        for telescope in telescopes:
            req = Request(
                           target     = target,
                           molecules  = molecules,
                           windows    = windows,
                           telescope  = telescope,
                           request_number = req_dict['request_number'],
                         )
            requests.append(req)


        return requests



class LocationExpander(object):
    def __init__(self, locations):
        self.set_locations(locations)


    def set_locations(self, locations):
        ''' Expects a tuple of fully qualified names, with the telescope class
            prepended to the front. Example:

            location =  (
                          '0m4a.aqwa.bpl.0m4',
                          '0m4b.aqwa.bpl.0m4',
                          '1m0a.doma.elp.1m0',
                        )
        '''

        self.telescopes = [ location.split('.') for location in locations ]

        return


    def expand_locations(self, dict_repr):

        # We don't accept sub-filters unless all previous filters have been populated
        filters = []
        if dict_repr['telescope_class']:
            filters.append(dict_repr['telescope_class'])

            if dict_repr['site']:
                filters.append(dict_repr['site'])

                if dict_repr['observatory']:
                    filters.append(dict_repr['observatory'])

                    if dict_repr['telescope']:
                        filters.append(dict_repr['telescope'])


        # Now run the filters to pick out only matches
        filtered_subset = self.telescopes
        for f in filters:
            new_filtered_subset = []
            for location in filtered_subset:
                if f in location:
                    new_filtered_subset.append(location)
            filtered_subset = new_filtered_subset

        # Reconstruct the location string, but drop the telescope class
        locations = [ '.'.join(location[:-1]) for location in filtered_subset ]

        return locations
