#!/usr/bin/env python

'''
requests.py - A set of python classes to store Request data

description

Author: Eric Saunders
July 2012
'''

# Required for true (non-integer) division
from __future__ import division

from rise_set.sky_coordinates                 import RightAscension, Declination
from rise_set.astrometry                      import (make_ra_dec_target,
                                                      make_minor_planet_target,
                                                      make_major_planet_target,
                                                      make_comet_target,
                                                      make_satellite_target)
from rise_set.angle                           import Angle, InvalidAngleError, AngleConfigError
from rise_set.rates                           import ProperMotion, RatesConfigError
from adaptive_scheduler.utils                 import (iso_string_to_datetime, convert_proper_motion,
                                                      EqualityMixin, safe_unidecode)
from adaptive_scheduler.printing              import plural_str as pl
from adaptive_scheduler.kernel.reservation_v3 import CompoundReservation_v2 as CompoundReservation
from adaptive_scheduler.log                   import RequestGroupLogger
from adaptive_scheduler.feedback              import UserFeedbackLogger
from adaptive_scheduler.eventbus              import get_eventbus
from adaptive_scheduler.moving_object_utils   import required_fields_from_scheme, scheme_mappings
from adaptive_scheduler.observation_portal_connections  import ObservationPortalConnectionError

from datetime    import datetime
from collections import defaultdict
import ast
import logging
import random
import numbers

log = logging.getLogger(__name__)

multi_rg_log = logging.getLogger('rg_logger')
rg_log = RequestGroupLogger(multi_rg_log)

event_bus = get_eventbus()

POND_FIELD_DIGITS = 7


def n_requests(request_groups):
    n_rgs = len(request_groups)
    n_rs = n_base_requests(request_groups)

    return n_rgs, n_rs


def n_base_requests(request_groups):
    return sum([rg.n_requests() for rg in request_groups])


def filter_out_compounds(request_groups):
    '''Given a list of RequestGroups, return a list containing only RequestGroups of
       type 'single'.'''
    single_rgs = []
    for rg in request_groups:
        if rg.operator != 'single':
            msg = "RG %d is of type %s - removing from consideration" % (rg.id, rg.operator)
            log.warn(msg)
        else:
            single_rgs.append(rg)

    return single_rgs


def filter_compounds_by_type(rgs):
    '''Given a list of RequestGroups, Return a dictionary that sorts them by type.'''
    rgs_by_type = {
                    'single' : [],
                    'many'   : [],
                    'and'    : [],
                    'oneof'  : [],
                  }

    for rg in rgs:
        rgs_by_type[rg.operator].append(rg)

    return rgs_by_type


def generate_request_description(request_group_json, request_json):
    prop_id = None
    user_id = None
    telescope_class = None
    inst_type = None
    if 'proposal' in request_group_json:
        prop_id = request_group_json.get('proposal')
        user_id = request_group_json.get('submitter')
    if 'location' in request_json:
        telescope_class = request_json.get('location').get('telescope_class')
    if 'configurations' in request_json and len(request_json['configurations']) > 0:
        filters = set()
        inst_types = set()
        target_names = set()
        for configuration in request_json['configurations']:
            if 'instrument_type' in configuration and configuration['instrument_type']:
                inst_types.add(configuration['instrument_type'])
            if 'target' in configuration and configuration['target'].get('name'):
                target_names.add(configuration['target']['name'])
            for inst_config in configuration['instrument_configs']:
                if 'optical_elements' in inst_config and inst_config['optical_elements']:
                    for element_type, element_value in inst_config['optical_elements'].items():
                        filters.add("{}: {}".format(element_type, element_value))

        inst_type = '(' + ', '.join(inst_types) + ')' if len(inst_types) > 0 else ''
        filter_string = '(' + ', '.join(filters) + ')' if len(filters) > 0 else ''
        target_string = '(' + ', '.join(target_names) + ')' if len(target_names) > 0 else ''
    return 'proposal={}, submitter={}, RG_id={}, R_id={}, telescope_class={}, target_names={}, inst_names={}, filters={}'.format(
                    prop_id,
                    user_id,
                    request_group_json.get('id'),
                    request_json.get('id'),
                    telescope_class,
                    target_string,
                    inst_type,
                    filter_string)


def differentiate_by_type(operator, rgs):
    '''Given an operator type and a list of RequestGroups, split the list into two
       lists, the chosen type, and the remainder.
       Valid operator types are 'single', 'and', 'oneof', 'many'.'''
    chosen_type = []
    other_types = []
    for rg in rgs:
        if rg.operator == operator:
            chosen_type.append(rg)
        else:
            other_types.append(rg)

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


class ICRSTarget(Target):
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

    ra = property(get_ra, set_ra)
    dec = property(get_dec, set_dec)


class OrbitalElementsTarget(Target):
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

        elif self.scheme.lower() == 'mpc_minor_planet':
            target_dict = make_minor_planet_target(self.scheme, self.epochofel, self.orbinc,
                                                   self.longascnode, self.argofperih,
                                                   self.meandist, self.eccentricity, self.meananom)

        elif self.scheme.lower() == 'jpl_major_planet':
            target_dict = make_major_planet_target(self.scheme, self.epochofel, self.orbinc,
                                                   self.longascnode, self.argofperih,
                                                   self.meandist, self.eccentricity, self.meananom,
                                                   self.dailymot)
        else:
            raise RequestError('Invalid target scheme %s', self.scheme.lower())

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


class Configuration(DataContainer):
    def __init__(self, *initial_data, **kwargs):
        DataContainer.__init__(self, *initial_data, **kwargs)

    def get_instrument_requirements(self):
        ''' Return a dictionary of instrument requirements for this configuration '''
        science_optical_elements = defaultdict(set)
        for instrument_config in self.instrument_configs:
            for element_type, element_value in instrument_config['optical_elements'].items():
                plural_element_type = element_type + 's'
                science_optical_elements[plural_element_type].add(element_value)
        guiding_optical_elements = defaultdict(set)
        for element_type, element_value in self.guiding_config['optical_elements'].items():
            plural_element_type = element_type + 's'
            guiding_optical_elements[plural_element_type].add(element_value)

        instrument_requirements = {
            'self_guide': self.extra_params.get('self_guide', False),
            'science_optical_elements': science_optical_elements,
            'guiding_optical_elements': guiding_optical_elements
        }
        return instrument_requirements


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
        return self.resource

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
        return self.size() > 0

    def size(self):
        all_windows_size = 0
        for resource_name, windows in self.windows_for_resource.iteritems():
            all_windows_size += len(windows)

        return all_windows_size

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


class Request(EqualityMixin):
    '''
        Represents a single valid configuration where an observation could take
        place. These are combined within a CompoundRequest to allow AND and OR
        semantics ("do this and this and this", "do this or this").

        configurations - a list of Configuration objects (detailed observing information)
        windows   - a list of start/end datetimes, representing when this observation
                    is eligible to be performed. For user observations with no
                    time constraints, this should be the planning window of the
                    scheduler (e.g. the semester bounds).
        id - The unique id of the Request
        state          - the initial state of the Request
    '''

    def __init__(self, configurations, windows, id, state='PENDING',
                 duration=0, scheduled_reservation=None):

        self.configurations         = configurations
        self.windows           = windows
        self.id    = id
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


class RequestGroup(EqualityMixin):
    '''UserRequests are just top-level groups of requests. They contain a set of requests, an operator, proposal info,
       ipp info, an id, and group name. This is translated into a CompoundReservation when scheduling'''

    _many_type = {'many': 'As many as possible of the provided blocks are to be scheduled'}
    valid_types = dict(CompoundReservation.valid_types)
    valid_types.update(_many_type)

    def __init__(self, operator, requests, proposal, id, observation_type, ipp_value, name, expires, submitter):

        self.proposal = proposal
        self.id = id
        self.name = name
        self.ipp_value = ipp_value
        self.observation_type = observation_type
        self.operator = self._validate_type(operator)
        self.requests = requests
        self.expires = expires
        self.submitter = submitter

    @staticmethod
    def _validate_type(provided_operator):
        '''Check the operator type being asked for matches a valid type
           of CompoundObservation.'''

        if provided_operator not in RequestGroup.valid_types:

            error_msg = ("You've asked for a type of request that doesn't exist. "
                         "Valid operator types are:\n")

            for res_type, help_txt in RequestGroup.valid_types.iteritems():
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

    def clear_scheduled_reservations(self):
        for request in self.requests:
            request.scheduled_reservation = None

    def set_scheduled_reservations(self, scheduled_reservations_by_request):
        for request in self.requests:
            if request.id in scheduled_reservations_by_request:
                request.scheduled_reservation = scheduled_reservations_by_request[request.id]

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

    def is_rapid_response(self):
        '''Return True if request is a Rapid Response.
        '''
        return self.observation_type == 'RAPID_RESPONSE'

    def is_schedulable(self, running_request_ids):
        return self._is_schedulable_easy(running_request_ids)

    def _is_schedulable_easy(self, running_request_ids):
        if self.operator == 'and':
            is_ok_to_return = True
            for r in self.requests:
                if not r.has_windows() and not r.id in running_request_ids:
                    return False

        elif self.operator in ('oneof', 'single', 'many'):
            is_ok_to_return = False
            for r in self.requests:
                if r.has_windows() or r.id in running_request_ids:
                    return True

        return is_ok_to_return

    @staticmethod
    def emit_request_group_feedback(id, msg, tag, timestamp=None):
        if not timestamp:
            timestamp = datetime.utcnow()

        originator = 'scheduler'

        event = UserFeedbackLogger.create_event(timestamp, originator, msg,
                                                tag, id)

        event_bus.fire_event(event)

        return

    def emit_rg_feedback(self, msg, tag, timestamp=None):
        RequestGroup.emit_request_group_feedback(self.id, msg, tag, timestamp)

    def get_priority(self):
        '''This returns the effective priority, seeded by the first request id'''
        return self.get_effective_priority(0)

    def get_effective_priority(self, request_index):
        if request_index < 0 or request_index >= len(self.requests):
            request_index = 0

        # seeded with the request id so it is repeatable with tests
        random.seed(self.requests[request_index].id)
        perturbation_size = 0.01
        ran = (1.0 - perturbation_size/2.0) + perturbation_size*random.random()

        req = self.requests[request_index]

        if self.observation_type.upper() == 'NORMAL':
            effective_priority = self.get_ipp_modified_priority() * req.get_duration() / 60.0
        elif self.observation_type.upper() == 'TIME_CRITICAL':
            # Time critical priority is base * 100 * fixed 1 hour duration
            effective_priority = self.get_base_priority() * 100.0 * 60.0
        elif self.observation_type.upper() == 'RAPID_RESPONSE':
            # Rapid response is only the tac priority
            effective_priority = self.get_base_priority()
        else:
            effective_priority = 0
            log.warning("Unknown observation type encountered: {}. Setting effective priority to 0".format(self.observation_type))

        effective_priority = min(effective_priority, 32000.0)*ran

        return effective_priority

    def get_base_priority(self):
        return self.proposal.tac_priority

    def get_ipp_modified_priority(self):
        return self.get_base_priority()*self.ipp_value

    # Define properties
    priority = property(get_priority)


class ModelBuilder(object):

    def __init__(self, valhalla_interface, configdb_interface, proposals_by_id=None, semester_details=None):
        self.valhalla_interface = valhalla_interface
        self.configdb_interface = configdb_interface
        self.proposals_by_id = proposals_by_id if proposals_by_id else {}
        self.semester_details = semester_details
        if not self.proposals_by_id:
            self._get_all_proposals()

    def _get_all_proposals(self):
        try:
            proposals = self.valhalla_interface.get_proposals()
            for prop in proposals:
                proposal = Proposal(prop)
                self.proposals_by_id[proposal.id] = proposal
        except ObservationPortalConnectionError as e:
            log.warning("failed to retrieve bulk proposals: {}".format(repr(e)))

    def get_proposal_details(self, proposal_id):
        if proposal_id not in self.proposals_by_id:
            try:
                proposal = Proposal(self.valhalla_interface.get_proposal_by_id(proposal_id))
                self.proposals_by_id[proposal_id] = proposal
            except ObservationPortalConnectionError as e:
                raise RequestError("failed to retrieve proposal {}: {}".format(proposal_id, repr(e)))

        return self.proposals_by_id[proposal_id]

    def get_semester_details(self, date):
        if not self.semester_details:
            try:
                self.semester_details = self.valhalla_interface.get_semester_details(date)
            except ObservationPortalConnectionError as e:
                raise RequestError("failed to retrieve semester for date {}: {}".format(date.isoformat(), repr(e)))

        return self.semester_details

    def build_request_group(self, rg_dict, scheduled_requests=None, ignore_ipp=False):
        if scheduled_requests is None:
            scheduled_requests = {}
        rg_id = int(rg_dict['id'])
        operator = rg_dict['operator'].lower()
        ipp_value = rg_dict.get('ipp_value', 1.0)
        submitter = rg_dict.get('submitter', '')
        if ignore_ipp:
             # if we want to ignore ipp in the scheduler, then set it to 1.0 here and it will not modify the priority
            ipp_value = 1.0

        requests, invalid_requests  = self.build_requests(rg_dict, scheduled_requests)
        if invalid_requests:
            msg = "Found %s." % pl(len(invalid_requests), 'invalid Request')
            log.warn(msg)
            for invalid_request, error_msg in invalid_requests:
                tag = "InvalidRequest"
                RequestGroup.emit_request_group_feedback(rg_id, error_msg, tag)
            if operator.lower() == 'and':
                msg = "Invalid request found within 'AND' RG %s making RG invalid" % rg_id
                tag = "InvalidRequestGroup"
                RequestGroup.emit_request_group_feedback(rg_id, msg, tag)
                raise RequestError(msg)

        if not requests:
            msg = "No valid Requests for RG %s" % rg_id
            tag = "InvalidRequestGroup"
            RequestGroup.emit_request_group_feedback(rg_id, msg, tag)
            raise RequestError(msg)

        proposal = self.get_proposal_details(rg_dict['proposal'])

        # Validate we are an allowed type of UR
        valid_observation_types = ['NORMAL', 'RAPID_RESPONSE', 'TIME_CRITICAL']
        observation_type = rg_dict['observation_type']
        if not observation_type in valid_observation_types:
            msg = "RequestGroup observation_type must be one of %s" % valid_observation_types
            raise RequestError(msg)

        # Calculate the maximum window time as the expire time
        max_window_time = datetime(1000, 1, 1)
        for req in requests:
            for windows in req.windows.windows_for_resource.values():
                for window in windows:
                    max_window_time = max(max_window_time, window.end)

        # Truncate the expire time by the current semester's end
        semester_details = self.get_semester_details(datetime.utcnow())
        if semester_details:
            max_window_time = min(max_window_time, semester_details['end'])

        request_group = RequestGroup(
                                    operator        = operator,
                                    requests        = requests,
                                    proposal        = proposal,
                                    id= rg_id,
                                    observation_type = observation_type,
                                    ipp_value       = ipp_value,
                                    name= rg_dict['name'],
                                    expires         = max_window_time,
                                    submitter       = safe_unidecode(submitter, 50),
                                  )

        # Return only the invalid request and not the error message
        invalid_requests = [ir[0] for ir in invalid_requests]
        return request_group, invalid_requests

    def build_requests(self, ur_dict, scheduled_requests=None):
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
        if scheduled_requests is None:
            scheduled_requests = {}
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
        # Create the Configurations
        configurations = []
        instrument_types_to_requirements = {}
        for configuration in req_dict['configurations']:
            config = self.build_configuration(configuration)
            configurations.append(config)
            instrument_types_to_requirements[config.instrument_type] = config.get_instrument_requirements()

        telescopes = self.configdb_interface.get_telescopes_for_instruments(
            instrument_types_to_requirements, req_dict['location']
        )

        if not telescopes:
            # Complain
            site_str = req_dict['location']['site'] if 'site' in req_dict['location'] else ''
            obs_str = req_dict['location']['observatory'] if 'observatory' in req_dict['location'] else ''
            tel_str = req_dict['location']['telescope'] if 'telescope' in req_dict['location'] else ''
            telescope_class = req_dict['location']['telescope_class'] if 'telescope_class' in req_dict['location'] else ''
            req_location = '.'.join(
                (
                    telescope_class,
                    site_str,
                    obs_str,
                    tel_str
                )
            )
            msg = "Request {} wants cameras [{}], which are not available on the subnetwork '{}'".format(
                req_dict['id'],
                ', '.join(instrument_types_to_requirements.keys()),
                req_location
            )
            raise RequestError(msg)

        # Create a window for each telescope in the subnetwork
        windows = Windows()
        for telescope in telescopes:
            for window_dict in req_dict['windows']:
                window = Window(window_dict=window_dict, resource=telescope)
                windows.append(window)

        # Finally, package everything up into the Request
        req = Request(
                       configurations=configurations,
                       windows         = windows,
                       id= int(req_dict['id']),
                       state           = req_dict['state'],
                       duration        = req_dict['duration'],
                       scheduled_reservation = scheduled_reservation
                     )

        return req

    def build_configuration(self, configuration):
        # build the target first
        if isinstance(configuration['target'], Target):
            target = configuration['target']
        else:
            target_type = configuration['target']['type']
            try:
                if target_type == 'ICRS':
                    target = ICRSTarget(configuration['target'])
                elif target_type == 'ORBITAL_ELEMENTS':
                    target = OrbitalElementsTarget(configuration['target'])
                elif target_type == 'SATELLITE':
                    target = SatelliteTarget(configuration['target'])
                else:
                    raise RequestError("Unsupported target type '%s'" % target_type)
            except (InvalidAngleError, RatesConfigError, AngleConfigError) as er:
                msg = "Rise-Set error: {}. Removing from consideration.".format(repr(er))
                raise RequestError(msg)

        configuration['target'] = target
        configuration = Configuration(
            **configuration
        )
        return configuration


class RequestError(Exception):
    pass
