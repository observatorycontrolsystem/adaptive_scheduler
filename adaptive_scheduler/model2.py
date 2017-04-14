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
from rise_set.astrometry                      import (make_ra_dec_target,
                                                      make_minor_planet_target,
                                                      make_comet_target,
                                                      make_satellite_target)
from rise_set.angle                           import Angle, InvalidAngleError, AngleConfigError
from rise_set.rates                           import ProperMotion, RatesConfigError
from adaptive_scheduler.utils                 import (iso_string_to_datetime, join_location, convert_proper_motion,
                                                      EqualityMixin)
from adaptive_scheduler.printing              import plural_str as pl
from adaptive_scheduler.kernel.reservation_v3 import CompoundReservation_v2 as CompoundReservation
from adaptive_scheduler.log                   import UserRequestLogger
from adaptive_scheduler.feedback              import UserFeedbackLogger
from adaptive_scheduler.eventbus              import get_eventbus
from adaptive_scheduler.moving_object_utils   import required_fields_from_scheme
from adaptive_scheduler.valhalla_connections  import ValhallaConnectionError
from schedutils.instruments                   import InstrumentFactory
from schedutils.camera_mapping                import create_camera_mapping

from datetime    import datetime
import ast
import logging
import random

log = logging.getLogger(__name__)

multi_ur_log = logging.getLogger('ur_logger')
ur_log = UserRequestLogger(multi_ur_log)

event_bus = get_eventbus()


def n_requests(user_reqs):
    n_urs  = len(user_reqs)
    n_rs   = n_base_requests(user_reqs)

    return n_urs, n_rs

def n_base_requests(user_reqs):
    return sum([ur.n_requests() for ur in user_reqs])

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


def filter_compounds_by_type(urs):
    '''Given a list of UserRequests, Return a dictionary that sorts them by type.'''
    urs_by_type = {
                    'single' : [],
                    'many'   : [],
                    'and'    : [],
                    'oneof'  : [],
                  }

    for ur in urs:
        urs_by_type[ur.operator].append(ur)

    return urs_by_type


def generate_request_description(user_request_json, request_json):
    prop_id = None
    user_id = None
    telescope_class = None
    inst_type = None
    if 'proposal' in user_request_json:
        prop_id = user_request_json.get('proposal')
        user_id = user_request_json.get('submitter')
    if 'location' in request_json:
        telescope_class = request_json.get('location').get('telescope_class')
    if 'molecules' in request_json and len(request_json['molecules']) > 0:
        filters = set()
        inst_types = set()
        for molecule in request_json['molecules']:
            if 'filter' in molecule and molecule['filter']:
                filters.add(molecule['filter'])
            if 'instrument_name' in molecule and molecule['instrument_name']:
                inst_types.add(molecule['instrument_name'])
        inst_type = '(' + ', '.join(inst_types) + ')' if len(inst_types) > 0 else ''
        filter_string = '(' + ', '.join(filters) + ')' if len(filters) > 0 else ''
    return 'prop_id={}, user_id={}, TN={}, RN={}, telescope_class={}, inst_names={}, filters={}'.format(
                    prop_id,
                    user_id,
                    user_request_json.get('id'),
                    request_json.get('id'),
                    telescope_class,
                    inst_type,
                    filter_string)


def differentiate_by_type(operator, urs):
    '''Given an operator type and a list of UserRequests, split the list into two
       lists, the chosen type, and the remainder.
       Valid operator types are 'single', 'and', 'oneof', 'many'.'''
    chosen_type = []
    other_types = []
    for ur in urs:
        if ur.operator == operator:
            chosen_type.append(ur)
        else:
            other_types.append(ur)

    return chosen_type, other_types


def file_to_dicts(filename):
    fh = open(filename, 'r')
    data = fh.read()

    return ast.literal_eval(data)


class DataContainer(EqualityMixin):
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

    def __repr__(self):
        fields_as_str = []
        for field in self.required_fields:
            fields_as_str.append(field + '=' + str(getattr(self, field)))
        fields_as_str = '({})'.format(', '.join(fields_as_str))
        return "{} {}".format(self.__class__.__name__, fields_as_str)


class NullTarget(Target):
    def __init__(self, *initial_data, **kwargs):
        Target.__init__(self, (), *initial_data, **kwargs)


class SiderealTarget(Target):
    ''' SiderealTarget for targets with Sidereal parameters (ra/dec)
    '''
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
        if hasattr(self, 'proper_motion_ra') and hasattr(self, 'proper_motion_dec'):
            # if we have proper_motion, then convert the units of proper motion to arcsec/year
            prop_mot_ra, prop_mot_dec = convert_proper_motion(self.proper_motion_ra,
                                                              self.proper_motion_dec,
                                                              self.dec.in_degrees())
            # then set the target_dict with the target with proper motion
            target_dict = make_ra_dec_target(self.ra, self.dec,
                ra_proper_motion=ProperMotion(Angle(degrees=(prop_mot_ra / 3600.0), units='arc'), time='year'),
                dec_proper_motion=ProperMotion(Angle(degrees=(prop_mot_dec / 3600.0), units='arc'), time='year'))
        else:
            target_dict = make_ra_dec_target(self.ra, self.dec)

        return target_dict

    ra  = property(get_ra, set_ra)
    dec = property(get_dec, set_dec)


class NonSiderealTarget(Target):
    ''' NonSiderealTarget for targets with moving object parameters, like comets or minor planets
    '''
    def __init__(self, *initial_data, **kwargs):
        scheme = initial_data[0]['scheme']

        required_fields = required_fields_from_scheme(scheme)
        Target.__init__(self, required_fields, *initial_data, **kwargs)

    def in_rise_set_format(self):
        if self.scheme.lower() == 'mpc_comet':
            target_dict = make_comet_target(self.scheme, self.epochofel, self.epochofperih,
                                            self.orbinc, self.longascnode, self.argofperih,
                                            self.perihdist, self.eccentricity)

        else:
            target_dict = make_minor_planet_target(self.scheme, self.epochofel, self.orbinc,
                                                    self.longascnode, self.argofperih,
                                                    self.meandist, self.eccentricity, self.meananom)

        return target_dict


class SatelliteTarget(Target):
    ''' SatelliteTarget for targets with satellite parameters and fixed windows. Rise-set just returns the
        dark intervals for these, so their parameters must be precomputed for their windows.
    '''
    def __init__(self, *initial_data, **kwargs):
        required_fields = ('altitude', 'azimuth', 'diff_pitch_rate', 'diff_roll_rate', 'diff_epoch_rate',
                           'diff_roll_acceleration', 'diff_pitch_acceleration')
        Target.__init__(self, required_fields, *initial_data, **kwargs)

    def in_rise_set_format(self):
        target_dict = make_satellite_target(self.altitude, self.azimuth, self.diff_pitch_rate, self.diff_roll_rate,
                                            self.diff_pitch_acceleration, self.diff_roll_acceleration, self.diff_epoch_rate)

        return target_dict


class Constraints(DataContainer):
    #TODO: Make this a named tuple
    def __init__(self, *args, **kwargs):
        self.max_airmass        = None
        # default minimum lunar distance is 0 if none is specified in request.
        self.min_lunar_distance = 0.0
        self.max_lunar_phase    = None
        self.max_seeing         = None
        self.min_transparency   = None
        DataContainer.__init__(self, *args, **kwargs)


    def __repr__(self):
        return "Constraints(airmass={}, min_lunar_distance={})".format(self.max_airmass, self.min_lunar_distance)

class Molecule(DataContainer):
    def __init__(self, required_fields, *initial_data, **kwargs):
        self.ag_name = None
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



class MoleculeFactory(object):
    def __init__(self):
        self.required_fields_by_mol = {
                                  'EXPOSE'    : ('type', 'exposure_count', 'bin_x', 'bin_y',
                                                 'instrument_name', 'filter', 'exposure_time',
                                                 'priority'),
                                  'STANDARD'  : ('type', 'exposure_count', 'bin_x', 'bin_y',
                                                 'instrument_name', 'filter', 'exposure_time',
                                                 'priority'),
                                  'ARC'       : ('type', 'exposure_count', 'bin_x', 'bin_y',
                                                 'instrument_name', 'spectra_slit', 'exposure_time',
                                                 'priority'),
                                  'LAMP_FLAT' : ('type', 'exposure_count', 'bin_x', 'bin_y',
                                                 'instrument_name', 'spectra_slit', 'exposure_time',
                                                 'priority'),
                                  'SPECTRUM'  : ('type', 'exposure_count', 'bin_x', 'bin_y',
                                                 'instrument_name', 'spectra_slit', 'exposure_time',
                                                 'priority', 'acquire_mode', 'acquire_radius_arcsec'),
                                  'BIAS'      : ('type', 'exposure_count', 'bin_x', 'bin_y',
                                                 'instrument_name', 'exposure_time',
                                                 'priority'),
                                  'DARK'      : ('type', 'exposure_count', 'bin_x', 'bin_y',
                                                 'instrument_name', 'exposure_time',
                                                 'priority'),
                                  'SKY_FLAT'  : ('type', 'exposure_count', 'bin_x', 'bin_y',
                                                 'instrument_name', 'filter', 'exposure_time',
                                                 'priority'),
                                  'AUTO_FOCUS' : ('type', 'exposure_count', 'bin_x', 'bin_y',
                                                 'instrument_name', 'filter', 'exposure_time',
                                                 'priority'),
                                  'ZERO_POINTING' : ('type', 'exposure_count', 'bin_x', 'bin_y',
                                                 'instrument_name', 'filter', 'exposure_time',
                                                 'priority'),
                                  'TRIPLE'    : ('type', 'exposure_count', 'bin_x', 'bin_y', 'instrument_name',
                                                 'exposure_time', 'priority'),
                                  'NRES_TEST' : ('type', 'exposure_count', 'exposure_time', 'bin_x', 'bin_y',
                                                 'instrument_name', 'priority', 'acquire_mode',
                                                 'acquire_radius_arcsec'),
                                  'NRES_SPECTRUM' : ('type', 'exposure_count', 'exposure_time', 'bin_x', 'bin_y',
                                                 'instrument_name', 'priority', 'acquire_mode',
                                                 'acquire_radius_arcsec'),
                                  'NRES_EXPOSE' : ('type', 'exposure_count', 'exposure_time', 'bin_x', 'bin_y',
                                                 'instrument_name', 'priority', 'acquire_mode',
                                                 'acquire_radius_arcsec'),
                                }

    def build(self, mol_dict):
        required_fields = self.required_fields_by_mol[mol_dict['type'].upper()]
        return Molecule(required_fields, mol_dict)



class Window(EqualityMixin):
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



class Windows(EqualityMixin):
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
        req_fields = ('id', 'tag', 'pi', 'tac_priority')
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



class Request(EqualityMixin):
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
                 duration=0, scheduled_reservation=None):

        self.target            = target
        self.molecules         = molecules
        self.windows           = windows
        self.constraints       = constraints
        self.request_number    = request_number
        self.state             = state
        self.req_duration      = duration
        self.scheduled_reservation = scheduled_reservation

    def get_duration(self):
        return self.req_duration

    def has_windows(self):
        return self.windows.has_windows()

    def n_windows(self):
        return self.windows.size()


    duration = property(get_duration)


class UserRequest(EqualityMixin):
    '''UserRequests are just top-level groups of requests. They contain a set of requests, an operator, proposal info,
       ipp info, an id, and group name. This is translated into a CompoundReservation when scheduling'''

    _many_type = {'many': 'As many as possible of the provided blocks are to be scheduled'}
    valid_types = dict(CompoundReservation.valid_types)
    valid_types.update(_many_type)

    def __init__(self, operator, requests, proposal, tracking_number, observation_type, ipp_value, group_id, expires, submitter):

        self.proposal        = proposal
        self.tracking_number = tracking_number
        self.group_id        = group_id
        self.ipp_value       = ipp_value
        self.observation_type = observation_type
        self.operator = self._validate_type(operator)
        self.requests = requests
        self.expires = expires
        self.submitter = submitter

    @staticmethod
    def _validate_type(provided_operator):
        '''Check the operator type being asked for matches a valid type
           of CompoundObservation.'''

        if provided_operator not in UserRequest.valid_types:

            error_msg = ("You've asked for a type of request that doesn't exist. "
                         "Valid operator types are:\n")

            for res_type, help_txt in UserRequest.valid_types.iteritems():
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

    def is_target_of_opportunity(self):
        '''Return True if request is a ToO.
        '''
        return self.observation_type == 'TARGET_OF_OPPORTUNITY'

    def is_schedulable(self, running_request_numbers):
        return self._is_schedulable_easy(running_request_numbers)

    def _is_schedulable_easy(self, running_request_numbers):
            if self.operator == 'and':
                is_ok_to_return = True
                for r in self.requests:
                    if not r.has_windows() and not r.request_number in running_request_numbers:
                        return False

            elif self.operator in ('oneof', 'single', 'many'):
                is_ok_to_return = False
                for r in self.requests:
                    if r.has_windows() or r.request_number in running_request_numbers:
                        return True

            return is_ok_to_return

    def _is_schedulable_hard(self, running_request_numbers):
        is_ok_to_return = {
                            'and'    : (False, lambda r: not r.has_windows() and not r.request_number in running_request_numbers),
                            'oneof'  : (True,  lambda r: r.has_windows() or r.request_number in running_request_numbers),
                            'single' : (True,  lambda r: r.has_windows() or r.request_number in running_request_numbers),
                            'many'   : (True,  lambda r: r.has_windows() or r.request_number in running_request_numbers)
                          }

        for r in self.requests:
            if is_ok_to_return[self.operator][1](r):
                return is_ok_to_return[self.operator][0]

        return not is_ok_to_return[self.operator][0]

    @staticmethod
    def emit_user_request_feedback(tracking_number, msg, tag, timestamp=None):
        if not timestamp:
            timestamp = datetime.utcnow()

        originator = 'scheduler'

        event = UserFeedbackLogger.create_event(timestamp, originator, msg,
                                                tag, tracking_number)

        event_bus.fire_event(event)

        return

    def emit_user_feedback(self, msg, tag, timestamp=None):
        UserRequest.emit_user_request_feedback(self.tracking_number, msg, tag, timestamp)

    def get_priority_dumb(self):
        '''This is a placeholder for a more sophisticated priority function. For now,
           it is just a pass-through to the proposal (i.e. TAC-assigned) priority.'''
        
        # doesn't have to be statistically random; determinism is important
        random.seed(self.requests[0].request_number)
        perturbation_size = 0.01
        ran = (1.0 - perturbation_size/2.0) + perturbation_size*random.random()

        #TODO: Placeholder for more sophisticated priority scheme
        # add small random bit to help scheduler break degeneracies
        return self.proposal.tac_priority*ran

    def get_priority(self):
        '''This is a placeholder for a more sophisticated priority function. For now,
           it is just a pass-through to the proposal (i.e. TAC-assigned) priority.'''

        # doesn't have to be statistically random; determinism is important
        random.seed(self.requests[0].request_number)
        perturbation_size = 0.01
        ran = (1.0 - perturbation_size/2.0) + perturbation_size*random.random()

        # Assume only 1 child Request
        req = self.requests[0]
        effective_priority = self.get_ipp_modified_priority() * req.get_duration()/60.0

        effective_priority = min(effective_priority, 32000.0)*ran

        return effective_priority

    def get_base_priority(self):
        return self.proposal.tac_priority

    def get_ipp_modified_priority(self):
        return self.get_base_priority()*self.ipp_value

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

    def __init__(self, tel_file, camera_mappings_file, valhalla_interface):
        self.tel_network        = build_telescope_network(tel_file)
        self.camera_mappings    = camera_mappings_file
        self.instrument_factory = InstrumentFactory()
        self.molecule_factory   = MoleculeFactory()
        self.valhalla_interface = valhalla_interface
        self.proposals_by_id = {}
        self.semester_details = None

    def get_proposal_details(self, proposal_id):
        if proposal_id not in self.proposals_by_id:
            try:
                proposal = Proposal(self.valhalla_interface.get_proposal_by_id(proposal_id))
                self.proposals_by_id[proposal_id] = proposal
            except ValhallaConnectionError as e:
                raise RequestError("failed to retrieve proposal {}: {}".format(proposal_id, repr(e)))

        return self.proposals_by_id[proposal_id]


    def get_semester_details(self, date):
        if not self.semester_details:
            try:
                self.semester_details = self.valhalla_interface.get_semester_details(date)
            except ValhallaConnectionError as e:
                raise RequestError("failed to retrieve semester for date {}: {}".format(date.isoformat(), repr(e)))

        return self.semester_details


    def build_user_request(self, ur_dict, scheduled_requests={}, ignore_ipp=False):
        tracking_number = ur_dict['id']
        operator = ur_dict['operator'].lower()
        ipp_value = ur_dict.get('ipp_value', 1.0)
        submitter = ur_dict.get('submitter', '')
        if ignore_ipp:
             # if we want to ignore ipp in the scheduler, then set it to 1.0 here and it will not modify the priority
            ipp_value = 1.0

        requests, invalid_requests  = self.build_requests(ur_dict, scheduled_requests)
        if invalid_requests:
            msg = "Found %s." % pl(len(invalid_requests), 'invalid Request')
            log.warn(msg)
            for invalid_request, error_msg in invalid_requests:
                tag = "InvalidRequest"
                UserRequest.emit_user_request_feedback(tracking_number, error_msg, tag)
            if operator.lower() == 'and':
                msg = "Invalid request found within 'AND' UR %s making UR invalid" % tracking_number
                tag = "InvalidUserRequest"
                UserRequest.emit_user_request_feedback(tracking_number, msg, tag)
                raise RequestError(msg)

        if not requests:
            msg = "No valid Requests for UR %s" % tracking_number
            tag = "InvalidUserRequest"
            UserRequest.emit_user_request_feedback(tracking_number, msg, tag)
            raise RequestError(msg)

        proposal = self.get_proposal_details(ur_dict['proposal'])

        # Validate we are an allowed type of UR
        valid_observation_types = ['NORMAL', 'TARGET_OF_OPPORTUNITY']
        observation_type = ur_dict['observation_type']
        if not observation_type in valid_observation_types:
            msg = "UserRequest observation_type must be one of %s" % valid_observation_types
            raise RequestError(msg)

        # Calculate the maximum window time as the expire time
        max_window_time = datetime(1000, 1, 1)
        for req in requests:
            for windows in req.windows.windows_for_resource.values():
                for window in windows:
                    max_window_time = max(max_window_time, window.end)

        #truncate the expire time by the current semesters end
        semester_details = self.get_semester_details(datetime.utcnow())
        if semester_details:
            max_window_time = min(max_window_time, semester_details['end'])

        user_request = UserRequest(
                                    operator        = operator,
                                    requests        = requests,
                                    proposal        = proposal,
                                    tracking_number = tracking_number,
                                    observation_type = observation_type,
                                    ipp_value       = ipp_value,
                                    group_id        = ur_dict['group_id'],
                                    expires         = max_window_time,
                                    submitter       = submitter,
                                  )

        # Return only the invalid request and not the error message
        invalid_requests = [ir[0] for ir in invalid_requests]
        return user_request, invalid_requests


    def build_requests(self, ur_dict, scheduled_requests={}):
        '''Returns tuple where first element is the list of validated request
        models and the second is a list of invalid request dicts  paired with
        validation errors 
            ([validated_request_model1,
              valicated_request_model2,
              ...
             ],
             [
                (invalid_request_dict1, 'validation error'),
                (invalid_request_dict2, 'validation error'),
                ...
             ]
            )
        '''
        requests         = []
        invalid_requests = []
        for req_dict in ur_dict['requests']:
            try:
                req = self.build_request(req_dict, scheduled_reservation=scheduled_requests.get(req_dict['id']))
                requests.append(req)
            except RequestError as e:
                log.warn(e)
                log.warn('Invalid Request: {}'.format(generate_request_description(ur_dict, req_dict)))
                invalid_requests.append((req_dict, e.message))

        return requests, invalid_requests


    def build_request(self, req_dict, scheduled_reservation=None):
        target_type = req_dict['target']['type']
        try:
            if target_type == 'SIDEREAL':
                target = SiderealTarget(req_dict['target'])
            elif target_type == 'NON_SIDEREAL':
                target = NonSiderealTarget(req_dict['target'])
            elif target_type == 'SATELLITE':
                target = SatelliteTarget(req_dict['target'])
            else:
                raise RequestError("Unsupported target type '%s'" % target_type)
        except (InvalidAngleError, RatesConfigError, AngleConfigError) as er:
            msg = "Rise-Set error: {}. Removing from consideration.".format(repr(er))
            raise RequestError(msg)

        # Create the Molecules
        molecules = []
        for mol_dict in req_dict['molecules']:
            molecules.append(self.molecule_factory.build(mol_dict))

        # A Request can only be scheduled on one instrument-based subnetwork
        if not self.have_same_instrument(molecules):
            # Complain
            msg  = "Request %s has molecules with different instruments" % req_dict['id']
            msg += " - removing from consideration"
            raise RequestError(msg)

        # To preserve the deprecated interface, map SCICAM -> 1m0-SCICAM-SBIG
        self.map_scicam_keyword(molecules, req_dict['id'])

        # Get the instrument (we know they are all the same)
        instrument_name = molecules[0].instrument_name

        mapping = create_camera_mapping(self.camera_mappings)

        generic_camera_names = self.instrument_factory.instrument_names

        if instrument_name.upper() in generic_camera_names:
            instrument_info = mapping.find_by_camera_type(instrument_name)
        else:
            instrument_info = mapping.find_by_camera(instrument_name)

        filters = []

        molecule_types_without_filter = ['dark', 'bias', 'triple', 'nres_test', 'nres_spectrum', 'nres_expose']

        for molecule in molecules:
            if hasattr(molecule, 'filter') and molecule.filter:
                filters.append(molecule.filter.lower())
            elif hasattr(molecule, 'spectra_slit') and molecule.spectra_slit:
                filters.append(molecule.spectra_slit.lower())
            # bias or dark molecules don't need filter or spectra_slit
            elif not hasattr(molecule, 'type') or (molecule.type.lower() not in molecule_types_without_filter):
                raise RequestError("Molecule must have either filter or spectra_slit")

        if filters:
            valid_instruments = mapping.find_by_filter(filters, instrument_info)
        else:
            valid_instruments = mapping.find_by_camera_type(instrument_name)

        # Determine the resource subnetwork satisfying the camera and location requirements
        telescopes     = self.tel_network.get_telescopes_at_location(req_dict['location'])
        tels_with_inst = [join_location(x['site'], x['observatory'], x['telescope']) for x in valid_instruments]
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
                                                                                req_dict['id'],
                                                                                instrument_name,
                                                                                req_location
                                                                               )
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
                       request_number  = req_dict['id'],
                       state           = req_dict['state'],
                       duration        = req_dict['duration'],
                       scheduled_reservation = scheduled_reservation
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
        SBIG_SCICAM = '1M0-SCICAM-SBIG'
        for mol in molecules:
            if mol.instrument_name.upper() == 'SCICAM':
                mol.instrument_name = SBIG_SCICAM

            if mol.ag_name:
                if mol.ag_name.upper() == 'SCICAM':
                    mol.ag_name = SBIG_SCICAM

#            msg = "Request %s passed deprecated 'SCICAM' keyword - remapping to %s" % (
#                                                                                        request_number,
#                                                                                        SBIG_SCICAM
#                                                                                      )
#            log.warn(msg)

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
        if 'telescope_class' in dict_repr and dict_repr['telescope_class']:
            filters.append(dict_repr['telescope_class'])

            if 'site' in dict_repr and dict_repr['site']:
                filters.append(dict_repr['site'])

                if 'observatory' in dict_repr and dict_repr['observatory']:
                    filters.append(dict_repr['observatory'])

                    if 'telescope' in dict_repr and dict_repr['telescope']:
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


class RequestError(Exception):
    pass
