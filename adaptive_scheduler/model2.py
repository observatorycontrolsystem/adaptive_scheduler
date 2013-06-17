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
from adaptive_scheduler.kernel.reservation_v3 import CompoundReservation_v2 as CompoundReservation
from adaptive_scheduler.exceptions import InvalidRequestError
from adaptive_scheduler import semester_service

import math
import ast
import logging
log = logging.getLogger(__name__)

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
        self._ra = RightAscension(degrees=float(ra))

    def set_dec(self, dec):
        #TODO: Check units are accurate
        self._dec = Declination(float(dec))

    def get_dec(self):
        return self._dec

    ra  = property(get_ra, set_ra)
    dec = property(get_dec, set_dec)


class Constraints(DataContainer):
    pass


class Molecule(DataContainer):
    #TODO: This is really an expose_n molecule, so should be specialised
    #TODO: Specialisation will be necessary once other molecules are scheduled

    def list_missing_fields(self):
        req_fields = ('type', 'exposure_count', 'bin_x', 'bin_y',
                      'instrument_name', 'filter', 'exposure_time',
                      'priority')
        missing_fields = []

        for field in req_fields:
            try:
                getattr(self, field)
            except:
                missing_fields.append(field)

        return missing_fields



class Window(DefaultMixin):
    '''Accepts start and end times as datetimes or ISO strings.'''
    def __init__(self, window_dict, resource):
        try:
            self.start  = iso_string_to_datetime(window_dict['start'])
            self.end    = iso_string_to_datetime(window_dict['end'])
        except TypeError:
            self.start = window_dict['start']
            self.end   = window_dict['end']

        self.resource = resource

    def get_resource_name(self):
        return self.resource.name



class Windows(DefaultMixin):
    def __init__(self):
        self.windows_for_resource = {}

    def append(self, window):
        if window.get_resource_name() in self.windows_for_resource:
            self.windows_for_resource[window.get_resource_name()].append(window)
        else:
            self.windows_for_resource[window.get_resource_name()] = [window]

        return

    def at(self, resource_name):
        return self.windows_for_resource[resource_name]

    def has_windows(self):
        return bool(self.size())

    def size(self):
        all_windows = []
        for resource_name, windows in self.windows_for_resource.iteritems():
            all_windows += windows

        return len(all_windows)



class Proposal(DataContainer):
    def list_missing_fields(self):
        req_fields = ('proposal_id', 'user_id', 'tag_id', 'observer_name', 'priority')
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
    # TODO: Update docstring to match new signature
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

    def __init__(self, target, molecules, windows, constraints, request_number):

        self.target         = target
        self.molecules      = molecules
        self.windows        = windows
        self.constraints    = constraints
        self.request_number = request_number

    def get_duration(self):
        '''This is a placeholder for a more sophisticated duration function, that
           does something clever with overheads. For now, it just sums the exposure
           times of the molecules, and adds an almost arbitrary overhead.'''

        #TODO: Placeholder for more sophisticated overhead scheme

        # Pick sensible sounding overheads, in seconds
        readout_per_exp        = 60    # Unbinned readout time per frame
        fixed_overhead_per_exp = 0.5   # Camera-query overhead (binning independent)
        filter_change_time     = 15    # Time to change a filter
        front_padding          = 90    # Guesstimate of sequencer/site agent set-up time
                                       # (upper bound)

        duration = 0

        # Find number of filter changes, and calculate total filter overhead
        prev_filter = None
        n_filter_changes = 0
        for i, mol in enumerate(self.molecules):
            if mol.filter != prev_filter:
                n_filter_changes += 1

            prev_filter = mol.filter

        filter_overhead = n_filter_changes * filter_change_time

        for mol in self.molecules:
            binned_overhead_per_exp = readout_per_exp / (mol.bin_x * mol.bin_y)
            total_overhead_per_exp  = binned_overhead_per_exp + fixed_overhead_per_exp
            mol_duration  = mol.exposure_count * (mol.exposure_time + total_overhead_per_exp)

            duration     += mol_duration

        # Add per-block overheads
        duration += front_padding
        duration += filter_overhead

        duration = math.ceil(duration)

        return duration


    def has_windows(self):
        return self.windows.has_windows()

    def n_windows(self):
        return self.windows.size()


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
            duration += req.duration

        return duration


    def filter_requests(self, filter_test):
        for r in self.requests:
            n_before = 0
            n_after  = 0
            for resource_name, windows in r.windows.windows_for_resource.items():
                n_before += len(windows)
                r.windows.windows_for_resource[resource_name] = [w for w in windows if filter_test(w, self)]
                n_after += len(r.windows.windows_for_resource[resource_name])

            log.debug("Windows before = %s", n_before)
            log.debug("Windows after  = %s", n_after)


    def is_schedulable(self):
#        return self._is_schedulable_hard()
        return self._is_schedulable_easy()


    def _is_schedulable_easy(self):
            if self.operator == 'and':
                is_ok_to_return = True
                for r in self.requests:
                    if not r.has_windows():
                        return False

            elif self.operator == 'oneof' or self.operator == 'single':
                is_ok_to_return = False
                for r in self.requests:
                    if r.has_windows():
                        return True

            return is_ok_to_return

    def _is_schedulable_hard(self):
        is_ok_to_return = {
                            'and'    : (False, lambda r: not r.has_windows()),
                            'oneof'  : (True,  lambda r: r.has_windows()),
                            'single' : (True,  lambda r: r.has_windows())
                          }

        for r in self.requests:
            if is_ok_to_return[self.operator][1](r):
                return is_ok_to_return[self.operator][0]

        return not is_ok_to_return[self.operator][0]


    # Define properties
    duration = property(get_duration)



class UserRequest(CompoundRequest, DefaultMixin):
    '''UserRequests are just top-level CompoundRequests. They differ only in having
       access to proposal and expiry information.'''

    def __init__(self, operator, requests, proposal, expires,
                 tracking_number, group_id):
        CompoundRequest.__init__(self, operator, requests)

        self.proposal = proposal
        self.expires  = expires
        self.tracking_number = tracking_number
        self.group_id = group_id


    def get_priority(self):
        '''This is a placeholder for a more sophisticated priority function. For now,
           it is just a pass-through to the proposal (i.e. TAC-assigned) priority.'''

        #TODO: Placeholder for more sophisticated priority scheme
        return self.proposal.priority

    # Define properties
    priority = property(get_priority)

    def scheduling_horizon(self):
        sem_end = semester_service.get_semester_end()
        if self.expires and self.expires < sem_end:
            return self.expires
        return sem_end



def build_telescope_network(tel_file=None, tel_dicts=None):
    '''Factory for TelescopeNetwork objects.'''

    # TODO: Raise exception if at least one argument isn't passed

    if tel_file:
        tel_dicts = file_to_dicts(tel_file)

    # Key the telescopes on <name.tel_class>
    telescopes = {}
    for d in tel_dicts:
        telescopes[ d['name'] ] = Telescope(d)

    return TelescopeNetwork(telescopes)


class TelescopeNetwork(object):
    def __init__(self, telescopes):
        self.telescopes = telescopes

        valid_locations = []
        for tel_name, tel in self.telescopes.iteritems():
            valid_locations.append(tel_name + '.' + tel.tel_class)

        self.le = _LocationExpander(valid_locations)


    def get_telescope(self, tel_name):
        return self.telescopes.get(tel_name, None)


    def get_telescopes_at_location(self, location):
        locations = self.le.expand_locations(location)

        telescopes = []
        for l in locations:
            tel = self.get_telescope(l)
            telescopes.append(tel)

        return telescopes



class ModelBuilder(object):

    def __init__(self, tel_file):
        self.tel_network = build_telescope_network(tel_file)


    def build_user_request(self, cr_dict):
        requests  = self.build_requests(cr_dict['requests'])
        proposal  = Proposal(cr_dict['proposal'])
        expiry_dt = iso_string_to_datetime(cr_dict['expires'])

        user_request = UserRequest(
                                    operator        = cr_dict['operator'],
                                    requests        = requests,
                                    proposal        = proposal,
                                    expires         = expiry_dt,
                                    tracking_number = cr_dict['tracking_number'],
                                    group_id        = cr_dict['group_id']
                                  )

        return user_request


    def build_requests(self, req_dicts):
        requests = []
        for req_dict in req_dicts:
            req = self.build_request(req_dict)
            requests.append(req)

        return requests


    def build_request(self, req_dict):
        target = Target(req_dict['target'])

        molecules = []
        for mol_dict in req_dict['molecules']:
            molecules.append(Molecule(mol_dict))

        telescopes = self.tel_network.get_telescopes_at_location(req_dict['location'])


        windows = Windows()
        for telescope in telescopes:
            for window_dict in req_dict['windows']:
                window = Window(window_dict=window_dict, resource=telescope)
                windows.append(window)

        constraints = Constraints(req_dict['constraints'])


        req = Request(
                       target         = target,
                       molecules      = molecules,
                       windows        = windows,
                       constraints    = constraints,
                       request_number = req_dict['request_number'],
                     )

        req.received_state = req_dict['state']

        return req



class _LocationExpander(object):
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
