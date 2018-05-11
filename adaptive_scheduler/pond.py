#!/usr/bin/env python

'''
pond.py - Facade to the pond_client library.

This module provides the scheduler's interface for constructing POND objects.
It maps objects across domains from 1) -> 2) (described below).

1) A complete scheduled observation in this facade needs the following:
    * A ScheduledBlock made up of
            * A Proposal object
        * One or more Molecules, each made up of
            * A set of Molecule-specific parameters
            * A Target, if applicable


2) A complete scheduled observation in the POND needs the following:
    * A ScheduledBlock made up of
        * One or more Observations, each made up of
            * A set of Observation-specific parameters
            * A Pointing, if applicable

   Meta information about Observations is added by means of Group objects.

Author: Eric Saunders
February 2012
'''
from __future__ import division

import time

from adaptive_scheduler.model2         import (Proposal, SiderealTarget, NonSiderealTarget, SatelliteTarget,
                                               NullTarget)
from adaptive_scheduler.utils          import (get_reservation_datetimes, timeit,
                                               split_location, merge_dicts, convert_proper_motion)

from adaptive_scheduler.printing       import pluralise as pl
from adaptive_scheduler.printing       import plural_str
from adaptive_scheduler.log            import UserRequestLogger
# from adaptive_scheduler.moving_object_utils import pond_pointing_from_scheme
from adaptive_scheduler.interfaces     import RunningRequest, RunningUserRequest, ScheduleException
from adaptive_scheduler.configdb_connections import ConfigDBError

# Set up and configure a module scope logger
import logging
from adaptive_scheduler.utils            import metric_timer
from adaptive_scheduler.kernel.intervals import Intervals
from adaptive_scheduler.kernel.timepoint import Timepoint

import requests
from dateutil.parser import parse
import json

log = logging.getLogger(__name__)

multi_ur_log = logging.getLogger('ur_logger')
ur_log = UserRequestLogger(multi_ur_log)

AG_MODE_MAPPING = {
    'OPTIONAL': 'OPT',
    'ON': 'YES',
    'OFF': 'NO'
}


class PondRunningRequest(RunningRequest):
    
    def __init__(self, telescope, request_number, block_id, start, end):
        RunningRequest.__init__(self, telescope, request_number, start, end)
        self.block_id = block_id

    def __str__(self):
        return RunningRequest.__str__(self) + ", block_id: %s" % (self.block_id)


class PondScheduleInterface(object):
    
    def __init__(self, host=None, port=None):
        self.host = host
        self.port = port
        self.running_blocks_by_telescope = None
        self.running_intervals_by_telescope = None
        self.too_intervals_by_telescope = None
        
        self.log = logging.getLogger(__name__)
    
    def fetch_data(self, telescopes, running_window_start, running_window_end):
        #Fetch the data
        self.running_blocks_by_telescope = self._fetch_running_blocks(telescopes, running_window_start, running_window_end)
        self.running_intervals_by_telescope = get_network_running_intervals(self.running_blocks_by_telescope)
        # TODO: Possible inefficency here.  Might be able to determine running too intervals from running blocks wihtout another call to pond
        self.too_intervals_by_telescope = self._fetch_too_intervals(telescopes, running_window_start, running_window_end)

    @metric_timer('pond.get_running_blocks', num_blocks=lambda x: len(x))
    def _fetch_running_blocks(self, telescopes, end_after, start_before):
        running_blocks = self._get_network_running_blocks(telescopes, end_after, start_before)
        # This is just logging held over from when this was in the scheduling loop
        all_running_blocks = []
        for blocks in running_blocks.values():
            all_running_blocks += blocks
        for block in all_running_blocks:
            msg = "UR %s has a running block (id=%d, finishing at %s)" % (
                                                         'tracking_num' in block['molecules'][0],
                                                         block['id'],
                                                         block['end']
                                                       )
            self.log.debug(msg)
        # End of logging block
        return running_blocks 

    @metric_timer('pond.get_too_intervals')
    def _fetch_too_intervals(self, telescopes, end_after, start_before):
        too_blocks = self._get_too_intervals_by_telescope(telescopes, end_after, start_before)
        
        return too_blocks

    def running_user_requests_by_tracking_number(self):
        running_urs = {}
        for blocks in self.running_blocks_by_telescope.values():
            for block in blocks:
                tracking_number = int(block['molecules'][0]['tracking_num'])
                running_ur = running_urs.setdefault(tracking_number, RunningUserRequest(tracking_number))
                telescope = block['telescope'] + '.' + block['observatory'] + '.' + block['site']
                request_number = block['molecules'][0]['request_num'] if 'request_num' in block['molecules'][0] else ''
                running_request = PondRunningRequest(telescope, request_number, block['id'],
                                                     block['start'], block['end'])
                if any([mol['failed'] for mol in block['molecules']]):
                    running_request.add_error("Block has failed molecules")
                running_ur.add_running_request(running_request)
            
        return running_urs
        
    def too_user_request_intervals_by_telescope(self):
        ''' Return the schedule ToO intervals for the supplied telescope
        '''
        return self.too_intervals_by_telescope
    
    @metric_timer('pond.cancel_requests', num_requests=lambda x: x, rate=lambda x: x)
    def cancel(self, cancelation_date_list_by_resource, reason, cancel_toos, cancel_normals):
        ''' Cancel the current scheduler between start and end
        ''' 
        n_deleted = 0
        if cancelation_date_list_by_resource:
            n_deleted = self._cancel_schedule(cancelation_date_list_by_resource, reason, cancel_toos,
                                              cancel_normals)
        return n_deleted
    
    def abort(self, pond_running_request, reason):
        ''' Abort a running request
        '''
        block_ids = [pond_running_request.block_id]
        return self._cancel_blocks(block_ids, reason)

    @metric_timer('pond.save_requests', num_requests=lambda x: x, rate=lambda x: x)
    def save(self, schedule, semester_start, configdb_interface, dry_run=False):
        ''' Save the provided observing schedule
        '''
        # Convert the kernel schedule into POND blocks, and send them to the POND
        n_submitted = self._send_schedule_to_pond(schedule, semester_start,
                                            configdb_interface, dry_run)
        return n_submitted
    
    # Already timed by the save method
    @timeit
    def _send_schedule_to_pond(self, schedule, semester_start, configdb_interface, dry_run=False):
        '''Convert a kernel schedule into POND blocks, and send them to the POND.'''
    
        blocks_by_resource = {}
        for resource_name, reservations in schedule.items():
            blocks_by_resource[resource_name] = []
            for reservation in reservations:
                block = build_block(reservation, reservation.request,
                                    reservation.user_request, semester_start,
                                    configdb_interface)
    
                blocks_by_resource[resource_name].append(block)
            _, block_str = pl(len(blocks_by_resource[resource_name]), 'block')
            msg = 'Sending {} {} to {}'.format(len(blocks_by_resource[resource_name]), block_str, resource_name)
            log_info_dry_run(msg, dry_run)
        n_submitted_total = self._send_blocks_to_pond(blocks_by_resource, dry_run)

        return n_submitted_total

    def _send_blocks_to_pond(self, blocks_by_resource, dry_run=False):
        pond_blocks = [block for blocks in blocks_by_resource.values() for block in blocks]
        # with open('/data/adaptive_scheduler/pond_blocks.json', 'w') as open_file:
        #     json.dump(pond_blocks, open_file)
        num_created = len(pond_blocks)
        if not dry_run:
            try:
                response = requests.post(self.host + '/blocks/', json=pond_blocks)
                response.raise_for_status()
                num_created = response.json()['num_created']
            except Exception as e:
                print(pond_blocks[0])
                log.error("_send_blocks_to_pond error: {}".format(repr(e)))

        return num_created

    def _get_blocks_by_telescope_for_tracking_numbers(self, tracking_numbers, tels, ends_after, starts_before):
        telescope_blocks = {}     
        for full_tel_name in tels:
            tel_name, obs_name, site_name = full_tel_name.split('.')
            schedule = self._get_schedule(ends_after, starts_before, site_name, obs_name, tel_name)
            filtered_blocks = [block for block in schedule if block['molecules'][0]['tracking_num'] in tracking_numbers]
            telescope_blocks[full_tel_name] = filtered_blocks
            
        return telescope_blocks
    
    def _get_too_blocks_by_telescope(self, tels, ends_after, starts_before):
        telescope_blocks = {}     
        for full_tel_name in tels:
            tel_name, obs_name, site_name = full_tel_name.split('.')
            schedule = self._get_schedule(ends_after, starts_before, site_name, obs_name, tel_name, too_blocks=True)
            telescope_blocks[full_tel_name] = schedule
            
        return telescope_blocks

    # Already timed by the fetch_too_intervals method
    @timeit
    def _get_too_intervals_by_telescope(self, tels, ends_after, starts_before):
        '''
            Return a map of telescope name to intervals for all currently
            schedule ToO intervals.
        '''
        telescope_interval = {}
        too_blocks_by_telescope = self._get_too_blocks_by_telescope(tels, ends_after, starts_before)
        
        for full_tel_name in tels:
            blocks = too_blocks_by_telescope.get(full_tel_name, [])
    
            intervals = get_intervals(blocks)
            if not intervals.is_empty():
                telescope_interval[full_tel_name] = intervals
    
        return telescope_interval

    @metric_timer('pond.get_schedule')
    def _get_schedule(self, start, end, site, obs, tel, too_blocks=None):
        # Only retrieve blocks which have not been cancelled or aborted
        args = dict(end_after=start, start_before=end, site=site,
                                 observatory=obs, telescope=tel,
                                 canceled=False, aborted=False)
        json_results = []
        if too_blocks:
            args['too_blocks'] = too_blocks

        try:
            results = requests.get(self.host + '/blocks/', params=args)
            json_results = results.json()['results']
            for block in json_results:
                block['start'] = parse(block['start'], ignoretz=True)
                block['end'] = parse(block['end'], ignoretz=True)
        except Exception as e:
            raise ScheduleException(e, "Unable to retrieve Schedule from Pond")

        return json_results

    @timeit
    def _cancel_schedule(self, cancelation_date_list_by_resource, reason, cancel_toos, cancel_normals):
        total_num_canceled = 0
        for full_tel_name, cancel_dates in cancelation_date_list_by_resource.items():
            for (start, end) in cancel_dates:
                tel, obs, site = full_tel_name.split('.')
                log.info("Cancelling schedule at %s, from %s to %s", full_tel_name,
                         start, end)

                data = {
                    'start': start.isoformat(),
                    'end': end.isoformat(),
                    'site': site,
                    'observatory': obs,
                    'telescope': tel,
                    'cancel_reason': reason
                }
                if cancel_toos != cancel_normals:
                    data['is_too'] = cancel_toos

                try:
                    results = requests.post(self.host + '/blocks/cancel/', json=data)
                    results.raise_for_status()
                    num_canceled = int(results.json()['canceled'])
                    total_num_canceled += num_canceled
                    msg = 'Cancelled {} blocks at {}'.format(num_canceled, full_tel_name)
                    log.info(msg)
                except Exception as e:
                    raise ScheduleException("Failed to cancel blocks in pond: {}".format(repr(e)))

        return total_num_canceled

    def _cancel_blocks(self, block_ids, reason):
        try:
            data = {'block_ids': block_ids,
                    'cancel_reason': reason}
            results = requests.post(self.host + '/blocks/cancel/', json=data)
            results.raise_for_status()
            num_canceled = results.json()['canceled']
        except Exception as e:
            raise ScheduleException("Failed to abort blocks in pond: {}".format(repr(e)))
    
        return num_canceled
    
    def _get_network_running_blocks(self, tels, ends_after, starts_before):
        n_running_total = 0
        running_at_tel = {}
        for full_tel_name in tels:
            tel_name, obs_name, site_name = full_tel_name.split('.')
            log.debug("Acquiring running blocks and first availability at %s",
                                                              full_tel_name)
    
            running = self._get_running_blocks(ends_after, starts_before,
                                                     site_name, obs_name, tel_name)
    
            running_at_tel[full_tel_name] = running
    
            n_running = len(running)
            _, block_str = pl(n_running, 'block')
            log.debug("Found %d running %s at %s", n_running, block_str, full_tel_name)
            n_running_total += n_running
    
        _, block_str = pl(n_running_total, 'block')
        log.info("Network-wide, found %d running %s", n_running_total, block_str)
    
        return running_at_tel
    
    def _get_running_blocks(self, ends_after, starts_before, site, obs, tel):
        schedule  = self._get_schedule(ends_after, starts_before, site, obs, tel)

        cutoff_dt = starts_before
        for block in schedule:
            if block['start'] < starts_before < block['end']:
                if (len(block['molecules']) > 0 and 'tracking_num' in block['molecules'][0]
                        and block['molecules'][0]['tracking_num']):
                    if block['end'] > cutoff_dt:
                        cutoff_dt = block['end']
    
        running = [b for b in schedule if b['start'] < cutoff_dt]
    
        return running


def log_info_dry_run(msg, dry_run):
    if dry_run:
        msg = "DRY-RUN: " + msg
    log.info(msg)


def resolve_instrument(instrument_name, site, obs, tel, configdb_interface):
    '''Determine the specific camera name for a given site.
       If a non-generic name is provided, we just pass it through and assume it's ok.'''
    try:
        specific_camera = configdb_interface.get_specific_instrument(instrument_name, site, obs, tel)
    except ConfigDBError as e:
        msg = "Couldn't find any instrument for '{}' at {}.{}.{}".format(instrument_name, tel, obs, site)
        raise InstrumentResolutionError(msg)

    return specific_camera


def resolve_autoguider(ag_name, specific_camera, site, obs, tel, configdb_interface):
    '''Determine the specific autoguider for a given site.
       If a specific name is provided, pass through and return it.
       If a generic name (SCICAM-*) is provided, resolve for self-guiding.
       If nothing is specified, resolve to the preferred autoguider.'''

    try:
        ag_match = configdb_interface.get_autoguider_for_instrument(specific_camera, ag_name)
    except ConfigDBError as e:
        msg = "Couldn't find any autoguider {} for '{}' at {}.{}.{}".format(ag_name, specific_camera, tel, obs, site)
        raise InstrumentResolutionError(msg)

    return ag_match


def build_block(reservation, request, user_request, semester_start, configdb_interface):
    res_start, res_end = get_reservation_datetimes(reservation, semester_start)
    is_too = user_request.observation_type == 'TARGET_OF_OPPORTUNITY'
    telescope, observatory, site = split_location(reservation.scheduled_resource)

    block = {
        'telescope': telescope,
        'observatory': observatory,
        'site': site,
        'start': res_start.isoformat(),
        'end': res_end.isoformat(),
        # Hard-code all scheduler output to a highish number, for now
        'priority': 30,
        'is_too': is_too,
        'max_airmass': request.constraints.max_airmass,
        'min_lunar_dist': request.constraints.min_lunar_distance,
        'max_lunar_phase': request.constraints.max_lunar_phase,
        'max_seeing': request.constraints.max_seeing,
        'min_transparency': request.constraints.min_transparency,
        'instrument_class': request.molecules[0].instrument_name,
        'molecules': []
    }

    pointing = request.target.in_pond_format()

    for i, molecule in enumerate(request.molecules):
        specific_camera = resolve_instrument(molecule.instrument_name, site,
                                             observatory, telescope, configdb_interface)
        # copy all of the fields already in the molecule (passed through from valhalla)
        mol_dict = molecule.mol_dict.copy()
        mol_dict['prop_id'] = user_request.proposal.id
        mol_dict['tag_id'] = user_request.proposal.tag
        mol_dict['user_id'] = user_request.submitter
        mol_dict['group'] = user_request.group_id
        mol_dict['tracking_num'] = str(user_request.tracking_number).zfill(10)
        mol_dict['request_num'] = str(request.request_number).zfill(10)
        # Add the pointing into the molecule
        mol_dict['pointing'] = pointing
        # Set the specific instrument as resolved with configdb
        mol_dict['inst_name'] = specific_camera
        # Set the autoguider name if needed
        if molecule.ag_mode != 'OFF':
            ag_name = getattr(molecule, 'ag_name', None) or ''
            specific_ag = resolve_autoguider(ag_name, specific_camera,
                                             site, observatory, telescope,
                                             configdb_interface)
            mol_dict['ag_name'] = specific_ag

            msg = "Autoguider resolved as {}".format(specific_ag)
            log.debug(msg)
            ur_log.debug(msg, user_request.tracking_number)

        # Need to map the ag_mode values
        mol_dict['ag_mode'] = AG_MODE_MAPPING[mol_dict['ag_mode'].upper()]
        # Need to map the expmeter_mode
        if mol_dict['expmeter_mode'] == 'OFF':
            mol_dict['expmeter_mode'] = 'EXPMETER_OFF'
        mol_dict['expmeter_snr'] = mol_dict['expmeter_snr'] or 0.0

        mol_summary_msg = "Building {} molecule {}/{} ({} x {:.3f}s)".format(
            molecule.type,
            i + 1,
            len(request.molecules),
            molecule.exposure_count,
            molecule.exposure_time,
        )
        log.debug(mol_summary_msg)
        ur_log.debug(mol_summary_msg, user_request.tracking_number)

        block['molecules'].append(mol_dict)
    log.debug("Constructing block: RN=%s TN=%s, %s <-> %s, priority %s",
                                     block['molecules'][0]['request_num'], block['molecules'][0]['tracking_num'],
                                     block['start'], block['end'], block['priority'])

    return block


@metric_timer('pond.get_network_running_interavls')
def get_network_running_intervals(running_blocks_by_telescope):
    running_at_tel = {}

    for key, blocks in running_blocks_by_telescope.items():
        running_at_tel[key] = get_intervals(blocks)

    return running_at_tel


def get_intervals(blocks):
    ''' Create Intervals from given blocks  '''
    timepoints = []
    for block in blocks:
        timepoints.append(Timepoint(block['start'], 'start'))
        timepoints.append(Timepoint(block['end'], 'end'))

    return Intervals(timepoints)


class InstrumentResolutionError(Exception):
    '''Raised when no instrument can be determined for a given resource.'''
    pass
