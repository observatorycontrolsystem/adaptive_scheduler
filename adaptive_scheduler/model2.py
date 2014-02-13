#!/usr/bin/env python

'''
model2.py - summary line

description

Author: Eric Saunders
July 2012
'''

# Required for true (non-integer) division
from __future__ import division

from rise_set.sky_coordinates                 import RightAscension, Declination
from rise_set.astrometry                      import make_ra_dec_target, make_moving_object_target
from adaptive_scheduler.utils                 import ( iso_string_to_datetime, EqualityMixin,
                                                       DefaultMixin, join_location)
from adaptive_scheduler.kernel.reservation_v3 import CompoundReservation_v2 as CompoundReservation
from adaptive_scheduler.log                   import UserRequestLogger
from adaptive_scheduler.feedback              import UserFeedbackLogger
from adaptive_scheduler.eventbus              import get_eventbus
from adaptive_scheduler.moving_object_utils   import required_fields_from_scheme
from adaptive_scheduler.camera_mapping        import create_camera_mapping
from adaptive_scheduler                       import semester_service

from datetime    import datetime
from collections import namedtuple
import math
import ast
import logging
log = logging.getLogger(__name__)

multi_ur_log = logging.getLogger('ur_logger')
ur_log = UserRequestLogger(multi_ur_log)

event_bus = get_eventbus()


def n_requests(user_reqs):
    n_urs  = len(user_reqs)
    n_rs   = sum([ur.n_requests() for ur in user_reqs])

    return n_urs, n_rs


def filter_out_compounds(user_reqs):
    '''Given a list of UserRequests, return a list containing only UserRequests of
       type 'single'.'''
    single_urs = []
    for ur in user_reqs:
        if ur.operator != 'single':
            msg = "UR %s is of type %s - removing from consideration" % (ur.tracking_number, ur.operator)
            log.warn(msg)
        else:
            single_urs.append(ur)

    return single_urs


def filter_compounds_by_type(crs):
    '''Given a list of CompoundRequests, Return a dictionary that sorts them by type.'''
    crs_by_type = {
                    'single' : [],
                    'many'   : [],
                    'and'    : [],
                    'oneof'  : [],
                  }

    for cr in crs:
        crs_by_type[cr.operator].append(cr)

    return crs_by_type


def differentiate_by_type(cr_type, crs):
    '''Given an operator type and a list of CompoundRequests, split the list into two
       lists, the chosen type, and the remainder.
       Valid operator types are 'single', 'and', 'oneof', 'many'.'''
    chosen_type = []
    other_types = []
    for cr in crs:
        if cr.operator == cr_type:
            chosen_type.append(cr)
        else:
            other_types.append(cr)

    return chosen_type, other_types


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

    def __init__(self, required_fields, *initial_data, **kwargs):
        DataContainer.__init__(self, *initial_data, **kwargs)
        self.required_fields = required_fields

    def list_missing_fields(self):
        missing_fields = []
        for field in self.required_fields:
            try:
                getattr(self, field)
            except:
                missing_fields.append(field)

        return missing_fields


class SiderealTarget(Target):

    def __init__(self, *initial_data, **kwargs):
        Target.__init__(self, ('name', 'ra', 'dec'), *initial_data, **kwargs)

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

    def in_rise_set_format(self):
        target_dict = make_ra_dec_target(self.ra, self.dec)

        return target_dict


    ra  = property(get_ra, set_ra)
    dec = property(get_dec, set_dec)

    def __repr__(self):
        return "SiderealTarget(%s, RA=%s, Dec=%s)" % (self.name, self.ra, self.dec)



class NonSiderealTarget(Target):

    def __init__(self, *initial_data, **kwargs):
        scheme = initial_data[0]['scheme']

        required_fields = required_fields_from_scheme(scheme)
        Target.__init__(self, required_fields, *initial_data, **kwargs)


    def in_rise_set_format(self):
        target_dict = make_moving_object_target(self.scheme, self.epochofel, self.orbinc,
                                                self.longascnode, self.argofperih,
                                                self.meandist, self.eccentricity, self.meananom)

        return target_dict


    def __repr__(self):
        fields_as_str = []
        for field in self.required_fields:
            fields_as_str.append(field + '=' + str(getattr(self, field)))
        fields_as_str = '(' + ', '.join(fields_as_str) + ')'
        return "NonSiderealTarget%s" % fields_as_str


class Constraints(DataContainer):
    #TODO: Make this a named tuple
    def __init__(self, *args, **kwargs):
        self.max_airmass        = None
        self.min_lunar_distance = None
        self.max_lunar_phase    = None
        self.max_seeing         = None
        self.min_transparency   = None
        DataContainer.__init__(self, *args, **kwargs)


    def __repr__(self):
        return "Constraints(airmass=%s)" % self.max_airmass

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

    def __repr__(self):
        return "Window (%s, %s)" % (self.start, self.end)



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

    def __iter__(self):
        for resource_name, windows in self.windows_for_resource.iteritems():
            yield resource_name, windows



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
    def __init__(self, *initial_data, **kwargs):
        kwargs['events'] = []
        DataContainer.__init__(self, *initial_data, **kwargs)



class Instrument(DefaultMixin):
    def __init__(self):
        self.type = 'NULL-INSTRUMENT'

    def _no_instrument_defined(self):
        raise ConfigurationError("Can't get duration - no instrument has been specified")

    def get_duration(self, dummy_molecule_arg):
        self._no_instrument_defined()



class InstrumentFactory(object):
    def __init__(self):
        self.overhead = namedtuple(
                                    'Overhead',
                                    (
                                      'readout_per_exp',        # Unbinned readout time per frame
                                      'fixed_overhead_per_exp', # Camera-query overhead (binning independent)
                                      'filter_change_time',     # Time to change a filter
                                      'front_padding'           # Guesstimate of sequencer/site agent set-up
                                                                # time (upper bound)
                                    )
                                 )

        self.instruments = {
                             '1M0-SCICAM-SBIG'     : self.make_sbig,
                             '1M0-SCICAM-SINISTRO' : self.make_sinistro,
                             '2M0-SCICAM-SPECTRAL' : self.make_spectral,
                             '2M0-SCICAM-MEROPE'   : self.make_merope,
                             'NULL-INSTRUMENT'     : self.make_null_instrument,
                           }


    def make_sbig(self):
        sbig_overheads = self.overhead(
                                        readout_per_exp        = 60,
                                        fixed_overhead_per_exp = 0.5,
                                        filter_change_time     = 15,
                                        front_padding          = 90,
                                      )

        sbig_ccd = CCD(*sbig_overheads, type='1M0-SCICAM-SBIG')

        return sbig_ccd


    def make_sinistro(self):
        sinistro_overheads = self.overhead(
                                            readout_per_exp        = 46,
                                            fixed_overhead_per_exp = 23,
                                            filter_change_time     = 15,
                                            front_padding          = 90,
                                          )

        sinistro_ccd = CCD(*sinistro_overheads, type='1M0-SCICAM-SINISTRO')

        return sinistro_ccd


    def make_spectral(self):
        spectral_overheads = self.overhead(
                                            readout_per_exp        = 42,
                                            fixed_overhead_per_exp = 12,
                                            filter_change_time     = 15,
                                            front_padding          = 120,
                                          )

        spectral_ccd = CCD(*spectral_overheads, type='2M0-SCICAM-SPECTRAL')

        return spectral_ccd


    def make_merope(self):
        merope_overheads = self.overhead(
                                            readout_per_exp        = 68,
                                            fixed_overhead_per_exp = 12,
                                            filter_change_time     = 15,
                                            front_padding          = 120,
                                          )

        merope_ccd = CCD(*merope_overheads, type='2M0-SCICAM-MEROPE')

        return merope_ccd


    def make_null_instrument(self):
        null_instrument = Instrument()

        return null_instrument


    def make_instrument_by_type(self, instrument_type):

        key = instrument_type.upper()
        if key not in self.instruments:
            key = 'NULL-INSTRUMENT'

        selected_instrument = self.instruments[key]()

        return selected_instrument



class CCD(Instrument):
    def __init__(self, readout_per_exp, fixed_overhead_per_exp, filter_change_time,
                 front_padding, type=''):
        self.readout_per_exp        = readout_per_exp
        self.fixed_overhead_per_exp = fixed_overhead_per_exp
        self.filter_change_time     = filter_change_time
        self.front_padding          = front_padding

        Instrument.__init__(self)
        if type:
            self.type = type

        return

    def get_duration(self, molecules):
        return self._calc_duration(molecules)

    def _calc_duration(self, molecules):
        duration = 0

        # Find number of filter changes, and calculate total filter overhead
        prev_filter = None
        n_filter_changes = 0
        for i, mol in enumerate(molecules):
            if mol.filter != prev_filter:
                n_filter_changes += 1

            prev_filter = mol.filter

        filter_overhead = n_filter_changes * self.filter_change_time

        for mol in molecules:
            binned_overhead_per_exp = self.readout_per_exp / (mol.bin_x * mol.bin_y)
            total_overhead_per_exp  = binned_overhead_per_exp + self.fixed_overhead_per_exp
            mol_duration  = mol.exposure_count * (mol.exposure_time + total_overhead_per_exp)

            duration     += mol_duration

        # Add per-block overheads
        duration += self.front_padding
        duration += filter_overhead

        duration = math.ceil(duration)

        return duration


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
        constraints - a Constraint object (airmass limit, etc.)
        request_number - The unique request number of the Request
        state          - the initial state of the Request
    '''

    def __init__(self, target, molecules, windows, constraints, request_number, state='PENDING',
                 instrument_type='', observation_type='NORMAL'):

        self.inst_factory = InstrumentFactory()

        self.target         = target
        self.molecules      = molecules
        self.windows        = windows
        self.constraints    = constraints
        self.request_number = request_number
        self.state          = state
        self.observation_type  = observation_type

        self.set_instrument(instrument_type)


    def set_instrument(self, instrument_type):
        self.instrument = self.inst_factory.make_instrument_by_type(instrument_type)

        return

    def get_instrument_type(self):
        return self.instrument.type

    def get_duration(self):
        return self.instrument.get_duration(self.molecules)

    def has_windows(self):
        return self.windows.has_windows()

    def n_windows(self):
        return self.windows.size()


    duration = property(get_duration)



class CompoundRequest(DefaultMixin):
    '''
        A user-level request for an observation. This will be translated into the
        Reservation/CompoundReservation of the scheduling kernel.

        operator - the type of compound request (single, and, oneof)
        proposal - proposal meta information associated with this request
        requests - a list of Request objects. There must be at least one.
    '''

    _many_type  = { 'many' : 'As many as possible of the provided blocks are to be scheduled' }
    valid_types = dict(CompoundReservation.valid_types)
    valid_types.update(_many_type)

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

            raise RequestError(error_msg)

        return provided_operator


    def filter_requests(self, filter_test):
        for r in self.requests:
            for resource_name, windows in r.windows.windows_for_resource.items():
                r.windows.windows_for_resource[resource_name] = [w for w in windows if filter_test(w, self, r)]


    def n_requests(self):
        return len(self.requests)


    def n_windows(self):
        n_windows = 0
        for request in self.requests:
            n_windows += request.windows.size()

        return n_windows


    def drop_empty_children(self):
        to_keep = []
        dropped = []
        for r in self.requests:
            if r.has_windows():
                to_keep.append(r)
            else:
                dropped.append(r)

        self.requests = to_keep

        return dropped


    def drop_non_pending(self):
        to_keep = []
        dropped = []
        for r in self.requests:
            if r.state == 'PENDING':
                to_keep.append(r)
            else:
                dropped.append(r)

        self.requests = to_keep

        return dropped


    def has_target_of_opportunity(self):
        '''Return True if request or child request is a ToO
        '''
        is_too_request = False
        for child_request in self.requests:
            if isinstance(child_request, Request):
                is_too_request = (child_request.observation_type == 'TARGET_OF_OPPORTUNITY')
            else:
                is_too_request = is_too_request or child_request.has_target_of_opportunity()

            if is_too_request:
                break

        return is_too_request


    def is_schedulable(self):
        return self._is_schedulable_easy()


    def _is_schedulable_easy(self):
            if self.operator == 'and':
                is_ok_to_return = True
                for r in self.requests:
                    if not r.has_windows():
                        return False

            elif self.operator in ('oneof', 'single', 'many'):
                is_ok_to_return = False
                for r in self.requests:
                    if r.has_windows():
                        return True

            return is_ok_to_return


    def _is_schedulable_hard(self):
        is_ok_to_return = {
                            'and'    : (False, lambda r: not r.has_windows()),
                            'oneof'  : (True,  lambda r: r.has_windows()),
                            'single' : (True,  lambda r: r.has_windows()),
                            'many'   : (True,  lambda r: r.has_windows())
                          }

        for r in self.requests:
            if is_ok_to_return[self.operator][1](r):
                return is_ok_to_return[self.operator][0]

        return not is_ok_to_return[self.operator][0]



class UserRequest(CompoundRequest, DefaultMixin):
    '''UserRequests are just top-level CompoundRequests. They differ only in having
       access to proposal and expiry information.'''

    def __init__(self, operator, requests, proposal, expires,
                 tracking_number, group_id):
        CompoundRequest.__init__(self, operator, requests)

        self.proposal        = proposal
        self.expires         = expires
        self.tracking_number = tracking_number
        self.group_id        = group_id


    def emit_user_feedback(self, msg, tag, timestamp=None):
        if not timestamp:
            timestamp = datetime.utcnow()

        originator = 'scheduler'

        event = UserFeedbackLogger.create_event(timestamp, originator, msg,
                                                tag, self.tracking_number)

        event_bus.fire_event(event)

        return


    def scheduling_horizon(self, now):
        sem_end = semester_service.get_semester_end(now)
        if self.expires and self.expires < sem_end:
            return self.expires
        return sem_end


    def get_priority(self):
        '''This is a placeholder for a more sophisticated priority function. For now,
           it is just a pass-through to the proposal (i.e. TAC-assigned) priority.'''

        #TODO: Placeholder for more sophisticated priority scheme
        return self.proposal.priority

    # Define properties
    priority = property(get_priority)



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

    def __init__(self, tel_file, camera_mappings_file):
        self.tel_network     = build_telescope_network(tel_file)
        self.camera_mappings = camera_mappings_file


    def build_user_request(self, cr_dict):
        requests, invalid_requests  = self.build_requests(cr_dict['requests'])
        if invalid_requests:
            log.warn("Found %d invalid Requests" % len(invalid_requests))

        if not requests:
            msg = "No valid Requests for UR %s" % cr_dict['tracking_number']
            raise RequestError(msg)

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
        requests         = []
        invalid_requests = []
        for req_dict in req_dicts:
            try:
                req = self.build_request(req_dict)
                requests.append(req)
            except RequestError as e:
                log.warn(e)
                invalid_requests.append(req_dict)

        return requests, invalid_requests


    def build_request(self, req_dict):
        target_type = req_dict['target']['type']
        if target_type == 'SIDEREAL':
            target = SiderealTarget(req_dict['target'])
        elif target_type == 'NON_SIDEREAL':
            target = NonSiderealTarget(req_dict['target'])
        else:
            raise RequestError("Unsupported target type '%s'" % target_type)

        # Create the Molecules
        molecules = []
        for mol_dict in req_dict['molecules']:
            molecules.append(Molecule(mol_dict))

        # A Request can only be scheduled on one instrument-based subnetwork
        if not self.have_same_instrument(molecules):
            # Complain
            msg  = "Request %s has molecules with different instruments" % req_dict['request_number']
            msg += " - removing from consideration"
            raise RequestError(msg)

        # To preserve the deprecated interface, map SCICAM -> 1m0-SCICAM-SBIG
        self.map_scicam_keyword(molecules, req_dict['request_number'])

        # Get the instrument (we know they are all the same)
        instrument_name = molecules[0].instrument_name

        mapping = create_camera_mapping(self.camera_mappings)

        generic_camera_names = ('1M0-SCICAM-SINISTRO', '1M0-SCICAM-SBIG',
                                '2M0-SCICAM-SPECTRAL', '2M0-SCICAM-MEROPE')

        if instrument_name.upper() in generic_camera_names:
            instrument_info = mapping.find_by_camera_type(instrument_name)
        else:
            instrument_info = mapping.find_by_camera(instrument_name)

        # Determine the resource subnetwork satisfying the camera and location requirements
        telescopes     = self.tel_network.get_telescopes_at_location(req_dict['location'])
        tels_with_inst = [join_location(x['site'], x['observatory'], x['telescope']) for x in instrument_info]
        subnetwork     = [t for t in telescopes if t.name in tels_with_inst]

        if not subnetwork:
            # Complain
            site_str = req_dict['location']['site'] or ''
            obs_str  = req_dict['location']['observatory'] or ''
            tel_str  = req_dict['location']['telescope'] or ''
            req_location = '.'.join(
                                     (
                                       req_dict['location']['telescope_class'],
                                       site_str,
                                       obs_str,
                                       tel_str
                                     )
                                   )
            msg = "Request %s wants camera %s, which is not available on the subnetwork '%s'" % (
                                                                                req_dict['request_number'],
                                                                                instrument_name,
                                                                                req_location
                                                                               )
            raise RequestError(msg)

        instrument_type = instrument_info[0]['camera_type']

        # Validate we are an allowed type of Request
        valid_observation_types = ['NORMAL', 'TARGET_OF_OPPORTUNITY']
        observation_type        = req_dict['observation_type']
        if not observation_type in valid_observation_types:
            msg = "Request observation_type must be one of %s" % valid_observation_types
            raise RequestError(msg)

        # Create a window for each telescope in the subnetwork
        windows = Windows()
        for telescope in subnetwork:
            for window_dict in req_dict['windows']:
                window = Window(window_dict=window_dict, resource=telescope)
                windows.append(window)

        constraints = Constraints(req_dict['constraints'])

        # Finally, package everything up into the Request
        req = Request(
                       target          = target,
                       molecules       = molecules,
                       windows         = windows,
                       constraints     = constraints,
                       request_number  = req_dict['request_number'],
                       state           = req_dict['state'],
                       instrument_type = instrument_type,
                       observation_type = req_dict['observation_type']
                     )

        return req


    def have_same_instrument(self, molecules):
        instrument_names = []
        for mol in molecules:
            instrument_names.append(mol.instrument_name)

        if len(set(instrument_names)) > 1:
            return False

        return True


    def map_scicam_keyword(self, molecules, request_number):
        # Assume all molecules have the same instrument
        if molecules[0].instrument_name.upper() == 'SCICAM':
            SBIG_SCICAM = '1m0-SCICAM-SBIG'
            msg = "Request %s passed deprecated 'SCICAM' keyword - remapping to %s" % (
                                                                                        request_number,
                                                                                        SBIG_SCICAM
                                                                                      )
            log.warn(msg)
            for mol in molecules:
                mol.instrument_name = SBIG_SCICAM

        return



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


class ConfigurationError(Exception):
    pass

class RequestError(Exception):
    pass
