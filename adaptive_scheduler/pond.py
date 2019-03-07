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

from datetime import timedelta

from adaptive_scheduler.utils          import (get_reservation_datetimes, timeit, split_location)

from adaptive_scheduler.printing       import pluralise as pl
from adaptive_scheduler.log            import RequestGroupLogger
from adaptive_scheduler.interfaces     import RunningRequest, RunningRequestGroup, ScheduleException
from adaptive_scheduler.configdb_connections import ConfigDBError

# Set up and configure a module scope logger
import logging
from adaptive_scheduler.utils            import metric_timer
from time_intervals.intervals import Intervals

import requests
from dateutil.parser import parse

log = logging.getLogger(__name__)

multi_rg_log = logging.getLogger('rg_logger')
rg_log = RequestGroupLogger(multi_rg_log)

AG_MODE_MAPPING = {
    'OPTIONAL': 'OPT',
    'ON': 'YES',
    'OFF': 'NO'
}


class ObservationRunningRequest(RunningRequest):
    
    def __init__(self, telescope, id, observation_id, start, end):
        RunningRequest.__init__(self, telescope, id, start, end)
        self.observation_id = observation_id

    def __str__(self):
        return RunningRequest.__str__(self) + ", observation_id: {}".format(self.observation_id)


class ObservationScheduleInterface(object):
    
    def __init__(self, host=None):
        self.host = host
        self.running_observations_by_telescope = None
        self.running_intervals_by_telescope = None
        self.rr_intervals_by_telescope = None
        
        self.log = logging.getLogger(__name__)
    
    def fetch_data(self, telescopes, running_window_start, running_window_end):
        #Fetch the data
        self.running_observations_by_telescope = self._fetch_running_blocks(telescopes, running_window_start, running_window_end)
        self.running_intervals_by_telescope = get_network_running_intervals(self.running_observations_by_telescope)
        # TODO: Possible inefficency here.  Might be able to determine running too intervals from running blocks wihtout another call to pond
        self.rr_intervals_by_telescope = self._fetch_too_intervals(telescopes, running_window_start, running_window_end)

    @metric_timer('pond.get_running_blocks', num_blocks=lambda x: len(x))
    def _fetch_running_blocks(self, telescopes, end_after, start_before):
        running_blocks = self._get_network_running_observations(telescopes, end_after, start_before)
        # This is just logging held over from when this was in the scheduling loop
        all_running_observations = []
        for blocks in running_blocks.values():
            all_running_observations += blocks
        for block in all_running_observations:
            msg = "UR %s has a running block (id=%d, finishing at %s)" % (
                                                         block['molecules'][0]['tracking_num'],
                                                         block['id'],
                                                         block['end']
                                                       )
        return running_blocks

    @metric_timer('pond.get_too_intervals')
    def _fetch_too_intervals(self, telescopes, end_after, start_before):
        too_blocks = self._get_too_intervals_by_telescope(telescopes, end_after, start_before)
        
        return too_blocks

    def running_request_groups_by_id(self):
        running_rgs = {}
        for blocks in self.running_observations_by_telescope.values():
            for block in blocks:
                request_group_id = int(block['molecules'][0]['tracking_num'])
                running_rg = running_rgs.setdefault(request_group_id, RunningRequestGroup(request_group_id))
                telescope = block['telescope'] + '.' + block['observatory'] + '.' + block['site']
                request_number = block['molecules'][0]['request_num'] if 'request_num' in block['molecules'][0] else ''
                running_request = ObservationRunningRequest(telescope, request_number, block['id'],
                                                            block['start'], block['end'])
                if any([mol['failed'] for mol in block['molecules']]):
                    running_request.add_error("Block has failed molecules")
                running_rg.add_running_request(running_request)
            
        return running_rgs
        
    def rr_request_group_intervals_by_telescope(self):
        ''' Return the schedule ToO intervals for the supplied telescope
        '''
        return self.rr_intervals_by_telescope
    
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
                try:
                    block = build_block(reservation, reservation.request,
                                        reservation.request_group, semester_start,
                                        configdb_interface)
                except Exception:
                    log.exception('Unable to build block from reservation for request number {}'.format(
                        self._get_request_id_from_reservation(reservation)
                    ))
                    continue
                blocks_by_resource[resource_name].append(block)
            _, block_str = pl(len(blocks_by_resource[resource_name]), 'block')
            msg = 'Will send {} {} to {}'.format(len(blocks_by_resource[resource_name]), block_str, resource_name)
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
                response = requests.post(self.host + '/blocks/', json=pond_blocks, timeout=120)
                response.raise_for_status()
                num_created = response.json()['num_created']
                self._log_bad_requests(pond_blocks, response.json()['errors'] if 'errors' in response.json() else {})
            except Exception as e:
                log.error("_send_blocks_to_pond error: {}".format(repr(e)))

        return num_created

    def _log_bad_requests(self, block_list, errors):
        for index, error in errors.items():
            bad_block = block_list[int(index)]
            log.warning('Failed to schedule block for request {}, user request {} due to reason {}'
                        .format(bad_block['molecules'][0]['request_num'],
                                bad_block['molecules'][0]['tracking_num'],
                                error))

    def _get_request_id_from_reservation(self, reservation):
        request_id = 0
        try:
            request_id = reservation.request.id
        except AttributeError:
            pass
        return request_id

    def _get_blocks_by_telescope_for_request_group_ids(self, request_group_ids, tels, ends_after, starts_before):
        telescope_blocks = {}     
        for full_tel_name in tels:
            tel_name, obs_name, site_name = full_tel_name.split('.')
            schedule = self._get_schedule(ends_after, starts_before, site_name, obs_name, tel_name)
            filtered_blocks = [block for block in schedule if block['molecules'][0]['tracking_num'] in request_group_ids]
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
        params = dict(end_after=start, start_before=end, start_after=start - timedelta(days=1), site=site,
                    observatory=obs, telescope=tel, limit=1000,
                    canceled=False, aborted=False, offset=0)
        if too_blocks:
            params['too_blocks'] = too_blocks

        base_url = self.host + '/blocks/'

        initial_results = self._get_block_helper(base_url, params)
        blocks = initial_results['results']
        count = initial_results['count']
        total = len(blocks)
        while total < count:
            params['offset'] += params['limit']
            results = self._get_block_helper(base_url, params)
            count = results['count']
            total += len(results['results'])
            blocks.extend(results['results'])

        for block in blocks:
            block['start'] = parse(block['start'], ignoretz=True)
            block['end'] = parse(block['end'], ignoretz=True)

        return blocks

    def _get_block_helper(self, base_url, params):
        try:
            results = requests.get(base_url, params=params, timeout=120)
            results.raise_for_status()
            return results.json()
        except Exception as e:
            raise ScheduleException(e, "Unable to retrieve Schedule from Pond: {}".format(repr(e)))

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
                    results = requests.post(self.host + '/blocks/cancel/', json=data, timeout=120)
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
            data = {'blocks': block_ids,
                    'cancel_reason': reason}
            results = requests.post(self.host + '/blocks/cancel/', json=data, timeout=120)
            results.raise_for_status()
            num_canceled = results.json()['canceled']
        except Exception as e:
            raise ScheduleException("Failed to abort blocks in pond: {}".format(repr(e)))
    
        return num_canceled
    
    def _get_network_running_observations(self, tels, ends_after, starts_before):
        n_running_total = 0
        running_at_tel = {}
        for full_tel_name in tels:
            tel_name, obs_name, site_name = full_tel_name.split('.')
            log.debug("Acquiring running observations and first availability at %s",
                                                              full_tel_name)
    
            running = self._get_running_observations(ends_after, starts_before,
                                                     site_name, obs_name, tel_name)
    
            running_at_tel[full_tel_name] = running
    
            n_running = len(running)
            _, block_str = pl(n_running, 'block')
            log.debug("Found %d running %s at %s", n_running, block_str, full_tel_name)
            n_running_total += n_running
    
        _, block_str = pl(n_running_total, 'block')
        log.info("Network-wide, found %d running %s", n_running_total, block_str)
    
        return running_at_tel
    
    def _get_running_observations(self, ends_after, starts_before, site, obs, tel):
        schedule  = self._get_schedule(ends_after, starts_before, site, obs, tel)

        cutoff_dt = starts_before
        for observation in schedule:
            if observation['start'] < starts_before < observation['end']:
                if (len(observation['molecules']) > 0 and 'tracking_num' in observation['molecules'][0]
                        and observation['molecules'][0]['tracking_num']):
                    if observation['end'] > cutoff_dt:
                        cutoff_dt = observation['end']
    
        running = [b for b in schedule if b['start'] < cutoff_dt and 'tracking_num' in b['molecules'][0] and b['molecules'][0]['tracking_num']]
    
        return running


def log_info_dry_run(msg, dry_run):
    if dry_run:
        msg = "DRY-RUN: " + msg
    log.info(msg)


def resolve_instrument(instrument_type, site, obs, tel, configdb_interface):
    """Determine the specific camera name for a given site.

    If a non-generic name is provided, we just pass it through and assume it's ok.
    """
    try:
        specific_camera = configdb_interface.get_specific_instrument(instrument_type, site, obs, tel)
    except ConfigDBError:
        msg = "Couldn't find any instrument for '{}' at {}.{}.{}".format(instrument_type, tel, obs, site)
        raise InstrumentResolutionError(msg)

    return specific_camera


def resolve_autoguider(self_guide, instrument_name, site, enc, tel, configdb_interface):
    """Determine the specific autoguider for a given site.

    If a specific name is provided, pass through and return it.
    If a generic name (SCICAM-*) is provided, resolve for self-guiding.
    If nothing is specified, resolve to the preferred autoguider.
    """

    try:
        ag_match = configdb_interface.get_autoguider_for_instrument(instrument_name, self_guide)
    except ConfigDBError:
        msg = "Couldn't find any autoguider for '{}' at {}.{}.{}".format(instrument_name, tel, enc, site)
        raise InstrumentResolutionError(msg)

    return ag_match


def build_block(reservation, request, request_group, semester_start, configdb_interface):
    res_start, res_end = get_reservation_datetimes(reservation, semester_start)
    is_too = request_group.observation_type == 'RAPID_RESPONSE'
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
        mol_dict['exposure_time'] = round(mol_dict['exposure_time'], 7)
        mol_dict['prop_id'] = request_group.proposal.id
        mol_dict['tag_id'] = request_group.proposal.tag
        mol_dict['user_id'] = request_group.submitter
        mol_dict['group'] = request_group.group_id
        mol_dict['tracking_num'] = request_group.id
        mol_dict['request_num'] = request.id
        # Add the pointing into the molecule
        mol_dict['pointing'] = pointing
        # Set the specific instrument as resolved with configdb
        mol_dict['inst_name'] = specific_camera
        # Set the autoguider name if needed
        if molecule.ag_mode != 'OFF':
            ag_name = getattr(molecule, 'ag_name', None) or ''
            # TODO: Don't pass in an ag_name (as there will no longer be one...) Instead pass if self_guide is set to
            # TODO: true in the extra params of the configuration
            specific_ag = resolve_autoguider(ag_name, specific_camera,
                                             site, observatory, telescope,
                                             configdb_interface)
            mol_dict['ag_name'] = specific_ag

            msg = "Autoguider resolved as {}".format(specific_ag)
            log.debug(msg)
            rg_log.debug(msg, request_group.id)

        # Need to map the ag_mode values
        mol_dict['ag_mode'] = AG_MODE_MAPPING[mol_dict['ag_mode'].upper()]
        # Need to map the expmeter_mode
        if mol_dict['expmeter_mode'] == 'OFF':
            mol_dict['expmeter_mode'] = 'EXPMETER_OFF'
        mol_dict['expmeter_snr'] = mol_dict['expmeter_snr'] or 0.0
        # replace '*' with ',' in filter if necessary
        if 'filter' in mol_dict and '*' in mol_dict['filter']:
            mol_dict['filter'] = mol_dict['filter'].replace('*', ',')

        mol_summary_msg = "Building {} molecule {}/{} ({} x {:.3f}s)".format(
            molecule.type,
            i + 1,
            len(request.molecules),
            molecule.exposure_count,
            molecule.exposure_time,
        )
        log.debug(mol_summary_msg)
        rg_log.debug(mol_summary_msg, request_group.id)

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
    intervals = []
    for block in blocks:
        intervals.append((block['start'], block['end']))

    return Intervals(intervals)


class InstrumentResolutionError(Exception):
    '''Raised when no instrument can be determined for a given resource.'''
    pass
