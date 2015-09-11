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
import time

from adaptive_scheduler.model2         import (Proposal, SiderealTarget, NonSiderealTarget,
                                               NullTarget)
from adaptive_scheduler.utils          import (get_reservation_datetimes, timeit,
                                               split_location, merge_dicts)

from adaptive_scheduler.printing       import pluralise as pl
from adaptive_scheduler.printing       import plural_str
from adaptive_scheduler.log            import UserRequestLogger
from adaptive_scheduler.moving_object_utils import pond_pointing_from_scheme
from adaptive_scheduler.interfaces     import RunningRequest, RunningUserRequest, ScheduleException

from schedutils.instruments            import InstrumentFactory
from schedutils.camera_mapping         import create_camera_mapping

from lcogtpond                         import pointing
from lcogtpond.block                   import Block as PondBlock
from lcogtpond.block                   import BlockSaveException, BlockCancelException
from lcogtpond.molecule                import Expose, Standard, Arc, LampFlat, Spectrum
from lcogtpond.schedule                import Schedule

# Set up and configure a module scope logger
import logging, sys
from datetime import datetime
from adaptive_scheduler.utils            import metric_timer
from adaptive_scheduler.kernel.intervals import Intervals
from adaptive_scheduler.kernel.timepoint import Timepoint
log = logging.getLogger(__name__)

multi_ur_log = logging.getLogger('ur_logger')
ur_log = UserRequestLogger(multi_ur_log)


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
        try:
            running_blocks = self._get_network_running_blocks(telescopes, end_after, start_before)
        except PondFacadeException, pfe:
            raise ScheduleException(pfe, "Unable to get running blocks from POND")
        
        # This is just logging held over from when this was in the scheduling loop
        all_running_blocks = []
        for blocks in running_blocks.values():
            all_running_blocks += blocks
        for block in all_running_blocks:
            msg = "UR %s has a running block (id=%d, finishing at %s)" % (
                                                         block.tracking_num_set()[0],
                                                         block.id,
                                                         block.end
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
                tracking_number = block.tracking_num_set()[0]
                running_ur = running_urs.setdefault(tracking_number, RunningUserRequest(tracking_number))
                telescope = block.telescope + '.' +  block.observatory + '.' + block.site
                running_request = PondRunningRequest(telescope, block.request_num_set()[0], block.id, block.start, block.end)
                if block.any_molecules_failed():
                    running_request.add_error("Block has failed molecules")
                running_ur.add_running_request(running_request)
            
        return running_urs
        
    def too_user_request_intervals_by_telescope(self):
        ''' Return the schedule ToO intervals for the supplied telescope
        '''
        return self.too_intervals_by_telescope
    
    @metric_timer('pond.cancel_requests', num_requests=lambda x: x, rate=lambda x: x)
    def cancel(self, cancelation_dates_by_resource, reason):
        ''' Cancel the current scheduler between start and end
        ''' 
        n_deleted = 0
        if cancelation_dates_by_resource:
            try:
                n_deleted = self._cancel_schedule(cancelation_dates_by_resource, reason)
            except PondFacadeException, pfe:
                raise ScheduleException(pfe, "Unable to cancel POND schedule")
        return n_deleted
    
    def abort(self, pond_running_request, reason):
        ''' Abort a running request
        ''' 
        try:
            block_ids = [pond_running_request.block_id]
            self._cancel_blocks(block_ids, reason, delete=False)
        except PondFacadeException, pfe:
            raise ScheduleException(pfe, "Unable to abort POND block")
    
    @metric_timer('pond.save_requests', num_requests=lambda x: x, rate=lambda x: x)
    def save(self, schedule, semester_start, camera_mappings, dry_run=False):
        ''' Save the provided observing schedule
        '''
        # Convert the kernel schedule into POND blocks, and send them to the POND
        n_submitted = self._send_schedule_to_pond(schedule, semester_start,
                                            camera_mappings, dry_run)
        return n_submitted
    
    # Already timed by the save method
    @timeit
    def _send_schedule_to_pond(self, schedule, semester_start, camera_mappings_file, dry_run=False):
        '''Convert a kernel schedule into POND blocks, and send them to the POND.'''
    
        blocks_by_resource = {}
        for resource_name, reservations in schedule.items():
            blocks_by_resource[resource_name] = []
            for reservation in reservations:
                block = build_block(reservation, reservation.request,
                                    reservation.compound_request, semester_start,
                                    camera_mappings_file)
    
                blocks_by_resource[resource_name].append(block)
    
        sent_blocks, not_sent_blocks= self._send_blocks_to_pond(blocks_by_resource, dry_run)
    
        # Summarise what was supposed to have been sent
        # The sorting is just a way to iterate through the output in a human-readable way
        n_submitted_total = 0
        for resource_name in sorted(sent_blocks, key=lambda x: x[::-1]):
            n_submitted = len(sent_blocks[resource_name])
            n_not_sent  = len(not_sent_blocks[resource_name])
            _, block_str = pl(n_submitted, 'block')
            msg = "%d %s to %s..." % (n_submitted, block_str, resource_name)
            if n_not_sent:
                msg += " (%s)" % plural_str(n_not_sent, "bad block")
            log_info_dry_run(msg, dry_run)
            n_submitted_total += n_submitted
    
    
        return n_submitted_total
    
    
    def _send_blocks_to_pond(self, blocks_by_resource, dry_run=False):
        pond_blocks     = []
        sent_blocks     = {}
        not_sent_blocks = {}
        for resource_name, blocks in blocks_by_resource.items():
            sent_blocks[resource_name]     = []
            not_sent_blocks[resource_name] = []
            for block in blocks:
                try:
                    pb = block.create_pond_block()
                    pond_blocks.append(pb)
                    msg = "Request %s (part of UR %s) to POND (%s.%s.%s)" % (block.request_number,
                                                                             block.tracking_number,
                                                                             pb.telescope,
                                                                             pb.observatory,
                                                                             pb.site)
                    if dry_run:
                        msg = "Dry-run: Would have sent " + msg
                    else:
                        msg = "Sent " + msg
                    log.debug(msg)
                    ur_log.info(msg, block.tracking_number)
                    sent_blocks[resource_name].append(block)
                except (IncompleteBlockError, InstrumentResolutionError) as e:
                    msg = "Request %s (UR %s) -> POND block conversion impossible:" % (
                                                                       block.request_number,
                                                                       block.tracking_number)
                    log.error(msg)
                    ur_log.error(msg, block.tracking_number)
                    log.error(e)
                    ur_log.error(e, block.tracking_number)
                    not_sent_blocks[resource_name].append(block)
    
    
        if not dry_run:
            try:
                PondBlock.save_blocks(pond_blocks, port=self.port, host=self.host)
            except BlockSaveException as e:
                log.error(e)
    
        return sent_blocks, not_sent_blocks
    
    
    def _get_blocks_by_telescope_for_tracking_numbers(self, tracking_numbers, tels, ends_after, starts_before):
        telescope_blocks = {}     
        for full_tel_name in tels:
            tel_name, obs_name, site_name = full_tel_name.split('.')
            blocks = self._get_schedule(ends_after, starts_before, site_name, obs_name, tel_name).blocks
            
            filtered_blocks = filter(lambda block: block.tracking_num_set() and block.tracking_num_set()[0] in tracking_numbers, blocks)
            telescope_blocks[full_tel_name] = filtered_blocks
            
        return telescope_blocks
    
    
    def _get_too_blocks_by_telescope(self, tels, ends_after, starts_before):
        telescope_blocks = {}     
        for full_tel_name in tels:
            tel_name, obs_name, site_name = full_tel_name.split('.')
            blocks = self._get_schedule(ends_after, starts_before, site_name, obs_name, tel_name, too_blocks=True).blocks
            
            # Remove blocks that dont have tracking numbers
            filtered_blocks = filter(lambda block: block.tracking_num_set(), blocks)
            telescope_blocks[full_tel_name] = filtered_blocks
            
        return telescope_blocks
    
    # This method is only called in test code, so no need to collect metrics from it
    @timeit
    def _get_intervals_by_telescope_for_tracking_numbers(self, tracking_numbers, tels, ends_after, starts_before):
        '''
            Return a map of telescope name to intervals covering all intervals
            currently scheduled for user requests in the supplied set of 
            tracking numbers.
        '''
        telescope_interval = {}
        blocks_by_telescope_for_tracking_numbers = self._get_blocks_by_telescope_for_tracking_numbers(tracking_numbers, tels, ends_after, starts_before)
        
        for full_tel_name in tels:
            blocks = blocks_by_telescope_for_tracking_numbers.get(full_tel_name, [])
    
            intervals = get_intervals(blocks)
            if not intervals.is_empty():
                telescope_interval[full_tel_name] = intervals
    
        return telescope_interval

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

    #@retry_or_reraise(max_tries=6, delay=10)
    def _get_schedule(self, start, end, site, obs, tel, too_blocks=None):
        # Only retrieve blocks which have not been cancelled or aborted
        args = dict(start=start, end=end, site=site,
                                 observatory=obs, telescope=tel,
                                 canceled_blocks=False, aborted_blocks=False, 
                                 use_master_db=True,
                                 port=self.port, host=self.host)
        if too_blocks:
            args['too_blocks'] = too_blocks
        return Schedule.get(**args)
        
        
    def _get_deletable_blocks(self, start, end, site, obs, tel):
        # Only retrieve blocks which have not been cancelled
        schedule = self._get_schedule(start, end, site, obs, tel)
        # Filter out blocks not placed by scheduler, they have not tracking number
        scheduler_placed_blocks = [b for b in schedule.blocks if b.tracking_num_set()]
    
        log.info("Retrieved %d blocks from %s.%s.%s (%s <-> %s)", len(schedule.blocks),
                                                                  tel, obs, site,
                                                                  start, end)
        log.info("%d/%d were placed by the scheduler and will be deleted", len(scheduler_placed_blocks),
                                                                           len(schedule.blocks))
        if scheduler_placed_blocks:
            to_delete_nums = [b.tracking_num_set() for b in scheduler_placed_blocks]
            log.debug("Deleting: %s", to_delete_nums)
    
        return scheduler_placed_blocks
    
    # This does not need a metric because it is called by the public cancel method which is timed
    @timeit
    def _cancel_schedule(self, cancelation_dates_by_resource, reason):
        all_to_delete = []
        for full_tel_name, (start, end) in cancelation_dates_by_resource.iteritems():
            tel, obs, site = full_tel_name.split('.')
            log.info("Cancelling schedule at %s, from %s to %s", full_tel_name,
                                                                 start, end)
    
            to_delete = self._get_deletable_blocks(start, end, site, obs, tel)
    
            n_to_delete = len(to_delete)
            all_to_delete.extend(to_delete)
    
            _, block_str = pl(n_to_delete, 'block')
            msg = "%d %s at %s" % (n_to_delete, block_str, full_tel_name)
            msg = "Cancelled " + msg
            log.info(msg)
    
        block_ids = [b.id for b in all_to_delete]
        self._cancel_blocks(block_ids, reason)
    
        return len(all_to_delete)


    def _cancel_blocks(self, block_ids, reason, delete=True):
        try:
            PondBlock.cancel_blocks(block_ids, reason=reason, delete=delete, port=self.port, host=self.host)
        except BlockCancelException as e:
            log.error(e)
    
        return len(block_ids)
    
    
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
        cutoff_dt = schedule.end_of_overlap(starts_before)
    
        running = [b for b in schedule.blocks if b.start < cutoff_dt and
                                                 b.tracking_num_set()]
    
        return running


def log_info_dry_run(msg, dry_run):
    if dry_run:
        msg = "DRY-RUN: " + msg
    log.info(msg)


def retry_or_reraise(max_tries=6, delay=10):
    '''Decorator to retry a POND operation several times, intended for sporadic
       outages. If max_tries is reached, we give up and raise a PondFacadeException.'''
    def wrapper(fn):
        def inner_func(*args, **kwargs):
            retries_available = max_tries
            while(retries_available):
                try:
                    result = fn(*args, **kwargs)
                    retries_available = 0

                # TODO: This throws a protobuf.socketrpc.error.RpcError - fix POND client to
                # TODO: return a POND client error instead
                # TODO: See #6578
                except Exception as e:
                    log.warn("POND RPC Error: %s", repr(e))

                    retries_available -= 1
                    if not retries_available:
                        log.warn("Retries exhausted - aborting current scheduler run")
                        raise PondFacadeException(str(e))
                    else:
                        log.warn("Sleeping for %s seconds" % delay)
                        time.sleep(delay)
                        log.warn("Will try %d more times" % retries_available)

            return result

        return inner_func

    return wrapper


def resolve_instrument(instrument_name, site, obs, tel, mapping):
    '''Determine the specific camera name for a given site.
       If a non-generic name is provided, we just pass it through and assume it's ok.'''

    instrument_factory   = InstrumentFactory()
    generic_camera_names = instrument_factory.instrument_names
    specific_camera      = instrument_name
    if instrument_name.upper() in generic_camera_names:
        inst_match = mapping.find_by_camera_type_and_location(site,
                                                              obs,
                                                              tel,
                                                              instrument_name)

        if not inst_match:
            msg = "Couldn't find any instrument for '%s' at %s.%s.%s" % (
                                                                    instrument_name,
                                                                    tel, obs, site)
            raise InstrumentResolutionError(msg)
        specific_camera = inst_match[0]['camera']

    return specific_camera


def resolve_autoguider(ag_name, specific_camera, site, obs, tel, mapping):
    '''Determine the specific autoguider for a given site.
       If a specific name is provided, pass through and return it.
       If a generic name (SCICAM-*) is provided, resolve for self-guiding.
       If nothing is specified, resolve to the preferred autoguider.'''

    if ag_name:
        # If SCICAM-* is provided, we will self-guide
        # Otherwise, we'll use whatever they suggest
        specific_ag = resolve_instrument(ag_name, site, obs, tel, mapping)
    else:
        # With no autoguider specified, we go with the preferred autoguider
        ag_match = mapping.find_by_camera(specific_camera)

        if not ag_match:
            msg = "Couldn't find any autoguider for '%s' at %s.%s.%s" % (
                                                                         specific_camera, tel,
                                                                         obs, site)
            raise InstrumentResolutionError(msg)
        specific_ag = ag_match[0]['autoguider']

    return specific_ag


class PondMoleculeFactory(object):

    def __init__(self, tracking_number, request_number, proposal, group_id):
        self.tracking_number = tracking_number
        self.request_number  = request_number
        self.proposal        = proposal
        self.group_id        = group_id
        self.molecule_classes = {
                                  'EXPOSE'    : Expose,
                                  'STANDARD'  : Standard,
                                  'ARC'       : Arc,
                                  'LAMP_FLAT' : LampFlat,
                                  'SPECTRUM'  : Spectrum,
                                }


    def build(self, molecule, pond_pointing):

        mol_type = self._determine_molecule_class(molecule, self.tracking_number)

        molecule_fields = {
                            'EXPOSE'    : (self._common, self._imaging, self._targeting),
                            'STANDARD'  : (self._common, self._imaging, self._targeting),
                            'SPECTRUM'  : (self._common, self._spectro, self._targeting),
                            'ARC'       : (self._common, self._spectro_calib),
                            'LAMP_FLAT' : (self._common, self._spectro_calib),
                          }

        param_dicts = [params(molecule, pond_pointing) for params in molecule_fields[molecule.type.upper()]]
        combined_params = merge_dicts(*param_dicts)

        return self._build_molecule(mol_type, combined_params)


    def _build_molecule(self, mol_type, fields):
        return mol_type.build(**fields)


    def _determine_molecule_class(self, molecule, tracking_number):
        ''' Validate the provided molecule type against supported POND molecule classes.
            Default to EXPOSE if the provided type is unknown.
        '''
        mol_type_incoming = molecule.type.upper()
        mol_class = None
        if mol_type_incoming in self.molecule_classes:
            msg = "Creating a %s molecule" % mol_type_incoming
            ur_log.debug(msg, tracking_number)
            mol_class = self.molecule_classes[mol_type_incoming]
        else:
            msg = "Unsupported molecule type %s provided; defaulting to EXPOSE" % mol_type_incoming
            log.warn(msg)
            ur_log.warn(msg, tracking_number)
            mol_class = self.molecule_classes['EXPOSE']

        return mol_class


    def _common(self, molecule, pond_pointing=None):
        return {
                 # Meta data
                 'tracking_num' : self.tracking_number,
                 'request_num'  : self.request_number,
                 'tag'          : self.proposal.tag_id,
                 'user'         : self.proposal.observer_name,
                 'proposal'     : self.proposal.proposal_id,
                 'group'        : self.group_id,
                 # Observation details
                 'exp_cnt'      : molecule.exposure_count,
                 'exp_time'     : molecule.exposure_time,
                 'bin'          : molecule.bin_x,
                 'inst_name'    : molecule.instrument_name,
                 'priority'     : molecule.priority,
               }

    def _imaging(self, molecule, pond_pointing=None):
        return {
                 'filters' : molecule.filter,
               }

    def _targeting(self, molecule, pond_pointing=None):
        ag_mode_pond_mapping = {
                                 'ON'       : 0,
                                 'OFF'      : 1,
                                 'OPTIONAL' : 2,
                               }
        return {
                 'pointing' : pond_pointing,
                 'defocus'  : getattr(molecule, 'defocus', None) or 0.0,
                 # Autoguider name might not exist if autoguiding disabled by ag_type
                 'ag_name'  : getattr(molecule, 'ag_name', None) or '',
                 'ag_mode'  : ag_mode_pond_mapping[molecule.ag_mode.upper()],
               }

    def _spectro(self, molecule, pond_pointing=None):
        acquire_mode_pond_mapping = {
                                      'WCS'       : 0,
                                      'BRIGHTEST' : 1,
                                      'OFF'       : 2,
                                    }

        return {
                 'spectra_slit' : molecule.spectra_slit,
                 'acquire_mode' : acquire_mode_pond_mapping[molecule.acquire_mode.upper()],
                 'acquire_radius_arcsec' : molecule.acquire_radius_arcsec,
               }

    def _spectro_calib(self, molecule, pond_pointing=None):
        return {
                 'spectra_slit' : molecule.spectra_slit,
               }



class Block(object):

    def __init__(self, location, start, end, group_id, tracking_number,
                 request_number, camera_mapping, priority=0, is_too=False,
                 max_airmass=None, min_lunar_distance=None, max_lunar_phase=None,
                 max_seeing=None, min_transparency=None):
        # TODO: Extend to allow datetimes or epoch times (and convert transparently)
        self.location           = location
        self.start              = start
        self.end                = end
        self.group_id           = group_id
        self.tracking_number    = str(tracking_number)
        self.request_number     = str(request_number)
        self.priority           = priority
        self.is_too             = is_too

        self.camera_mapping     = camera_mapping

        self.max_airmass        = max_airmass
        self.min_lunar_distance = min_lunar_distance
        self.max_lunar_phase    = max_lunar_phase
        self.max_seeing         = max_seeing
        self.min_transparency   = min_transparency

        self.proposal  = Proposal()
        self.molecules = []
        self.target    = NullTarget()

        self.pond_block = None


    def list_missing_fields(self):
        # Find the list of missing proposal fields
        proposal_missing = self.proposal.list_missing_fields()

        # Find the list of missing molecule fields
        molecule_missing = ['[No molecules specified]']
        if len(self.molecules) > 0:
            molecule_missing = []
            for molecule in self.molecules:
                molecule_missing.extend(molecule.list_missing_fields())

        # Find the list of missing target fields
        target_missing = self.target.list_missing_fields()

        # Aggregate the missing fields to return
        missing_fields = {}

        if len(proposal_missing) > 0:
            missing_fields['proposal'] = proposal_missing

        if len(molecule_missing) > 0:
            missing_fields['molecule'] = molecule_missing

        if len(target_missing) > 0:
            missing_fields['target'] = target_missing


        return missing_fields


    def add_proposal(self, proposal):
        self.proposal = proposal

    def add_molecule(self, molecule):
        # TODO: Handle molecule priorities
        self.molecules.append(molecule)

    def add_target(self, target):
        self.target = target


    def resolve_autoguider_in_molecule_fields(self, fields, tracking_number, site,
                                              observatory, telescope, camera_mapping):
        if fields['ag_mode'] != 'OFF':
            specific_ag = resolve_autoguider(fields['ag_name'], fields['inst_name'],
                                             site, observatory, telescope, camera_mapping)
            fields['ag_name'] = specific_ag

            msg = "Autoguider resolved as '%s'" % specific_ag
            log.debug(msg)
            ur_log.debug(msg, self.tracking_number)

        return


    def create_pond_block(self):
        if self.pond_block:
            return self.pond_block

        # Check we have everything we need
        missing_fields = self.list_missing_fields()
        if len(missing_fields) > 0:
            raise IncompleteBlockError(missing_fields)

        # Construct the POND objects...
        # 1) Create a POND ScheduledBlock
        telescope, observatory, site = split_location(self.location)
        block_build_args = dict(
                                start=self.start,
                                end=self.end,
                                site=site,
                                observatory=observatory,
                                telescope=telescope,
                                priority=self.priority,
                                is_too=self.is_too
                                )

        # If constraints are provided, include them in the block
        if self.max_airmass:
            block_build_args['airmass'] = self.max_airmass
        if self.min_lunar_distance:
            block_build_args['lunar_dist'] = self.min_lunar_distance
        if self.max_lunar_phase:
            block_build_args['lunar_phase'] = self.max_lunar_phase
        if self.max_seeing:
            block_build_args['seeing'] = self.max_seeing
        if self.min_transparency:
            block_build_args['trans'] = self.min_transparency

        pond_block = PondBlock.build(**block_build_args)

        if isinstance(self.target, SiderealTarget):
            # 2a) Construct the Pointing Coordinate
            coord = pointing.ra_dec(
                                     ra=self.target.ra.in_degrees(),
                                     dec=self.target.dec.in_degrees()
                                   )
            # 2b) Construct the Pointing
            pond_pointing = pointing.sidereal(
                                               name=self.target.name,
                                               coord=coord,
                                             )

        elif isinstance(self.target, NonSiderealTarget):
            pond_pointing = pond_pointing_from_scheme(self.target)

        elif isinstance(self.target, NullTarget):
            pond_pointing = None
        else:
            raise Exception("No mapping to POND pointing for type %s" % str(type(self.target)))

        # Set default rotator parameters if none provided
        if pond_pointing:
            if self.target.rot_mode:
                pond_pointing.rot_mode = self.target.rot_mode
            else:
                pond_pointing.rot_mode  = 'SKY'

            if self.target.rot_angle:
                pond_pointing.rot_angle = self.target.rot_angle
            else:
                pond_pointing.rot_angle = 0.0


        # 3) Construct the Observations
        observations = []


        for i, molecule in enumerate(self.molecules):
            filter_or_slit = 'Unknown'
            if molecule.type.upper() in ('EXPOSE', 'STANDARD'):
                filter_or_slit = molecule.filter
            else:
                filter_or_slit = molecule.spectra_slit
            mol_summary_msg = "Building %s molecule %d/%d (%dx%.03d %s)" % (
                                                                             molecule.type,
                                                                             i + 1,
                                                                             len(self.molecules),
                                                                             molecule.exposure_count,
                                                                             molecule.exposure_time,
                                                                             filter_or_slit,
                                                                             )
            log.debug(mol_summary_msg)
            ur_log.debug(mol_summary_msg, self.tracking_number)

            specific_camera = resolve_instrument(molecule.instrument_name, site,
                                                 observatory, telescope, self.camera_mapping)

            molecule.instrument_name = specific_camera

            msg = "Instrument resolved as '%s'" % specific_camera
            log.debug(msg)
            ur_log.debug(msg, self.tracking_number)

            if molecule.ag_mode != 'OFF':
                ag_name = getattr(molecule, 'ag_name', None) or ''
                specific_ag = resolve_autoguider(ag_name, specific_camera,
                                                 site, observatory, telescope,
                                                 self.camera_mapping)
                molecule.ag_name = specific_ag

                msg = "Autoguider resolved as '%s'" % specific_ag
                log.debug(msg)
                ur_log.debug(msg, self.tracking_number)


            pond_molecule_factory = PondMoleculeFactory(self.tracking_number, self.request_number,
                                                        self.proposal, self.group_id)
            obs = pond_molecule_factory.build(molecule, pond_pointing)

            observations.append(obs)

        # 4) Add the Observations to the Block
        for obs in observations:
            pond_block.add_molecule(obs)

        self.pond_block = pond_block

        return pond_block


def build_block(reservation, request, compound_request, semester_start, camera_mappings_file):
    camera_mapping = create_camera_mapping(camera_mappings_file)
    res_start, res_end = get_reservation_datetimes(reservation, semester_start)
    is_too = request.observation_type == 'TARGET_OF_OPPORTUNITY'
    block = Block(
                   location=reservation.scheduled_resource,
                   start=res_start,
                   end=res_end,
                   group_id=compound_request.group_id,
                   tracking_number=compound_request.tracking_number,
                   request_number=request.request_number,
                   camera_mapping=camera_mapping,
                   # Hard-code all scheduler output to a highish number, for now
                   priority=30,
                    is_too = is_too,
                   max_airmass=request.constraints.max_airmass,
                   min_lunar_distance=request.constraints.min_lunar_distance,
                   max_lunar_phase=request.constraints.max_lunar_phase,
                   max_seeing=request.constraints.max_seeing,
                   min_transparency=request.constraints.min_transparency,
                 )

    block.add_proposal(compound_request.proposal)
    for molecule in request.molecules:
        block.add_molecule(molecule)
    block.add_target(request.target)

    log.debug("Constructing block: RN=%s TN=%s, %s <-> %s, priority %s",
                                     block.request_number, block.tracking_number,
                                     block.start, block.end, block.priority)

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
        timepoints.append(Timepoint(block.start, 'start'))
        timepoints.append(Timepoint(block.end, 'end'))

    return Intervals(timepoints)


class IncompleteBlockError(Exception):
    '''Raised when a block is missing required parameters.'''
    pass

class InstrumentResolutionError(Exception):
    '''Raised when no instrument can be determined for a given resource.'''
    pass

class PondFacadeException(Exception):
    '''Placeholder until POND client raises this exception on our behalf.'''
    pass
