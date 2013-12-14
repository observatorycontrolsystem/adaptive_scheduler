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
from datetime import datetime
import time

from adaptive_scheduler.model2         import Proposal, SiderealTarget, NonSiderealTarget
from adaptive_scheduler.utils          import get_reservation_datetimes, timeit
from adaptive_scheduler.camera_mapping import create_camera_mapping
from adaptive_scheduler.printing       import pluralise as pl
from adaptive_scheduler.log            import UserRequestLogger

from lcogtpond                         import pointing
from lcogtpond.block                   import Block as PondBlock
from lcogtpond.block                   import BlockSaveException, BlockCancelException
from lcogtpond.molecule                import Expose, Standard
from lcogtpond.schedule                import Schedule

# Set up and configure a module scope logger
import logging
log = logging.getLogger(__name__)

multi_ur_log = logging.getLogger('ur_logger')
ur_log = UserRequestLogger(multi_ur_log)


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
    '''Determine the specific camera name for a given site.'''

    generic_camera_names = ('SCICAM', 'FASTCAM')
    specific_camera = instrument_name
    if instrument_name in generic_camera_names:
        tel_class = tel[:-1]
        search = tel_class + '-' + instrument_name
        inst_match = mapping.find_by_camera_type_and_location(site,
                                                              obs,
                                                              tel,
                                                              search)

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
       If a generic name (SCICAM) is provided, resolve for self-guiding.
       If nothing is specified, resolve to the preferred autoguider.'''

    if ag_name:
        # If SCICAM is provided, we will self-guide
        # Otherwise, we'll use whatever they suggest
        specific_ag = resolve_instrument(ag_name, site, obs, tel, mapping)
    else:
        # With no autoguider specified, we go with the preferred autoguider
        ag_match    = mapping.find_by_camera(specific_camera)

        if not ag_match:
            msg = "Couldn't find any autoguider for '%s' at %s.%s.%s" % (
                                                                         ag_name, tel,
                                                                         obs, site)
            raise InstrumentResolutionError(msg)
        specific_ag = ag_match[0]['autoguider']

    return specific_ag



class Block(object):

    def __init__(self, location, start, end, group_id, tracking_number,
                 request_number, priority=0, max_airmass=None,
                 min_lunar_distance=None, max_lunar_phase=None,
                 max_seeing=None, min_transparency=None):
        # TODO: Extend to allow datetimes or epoch times (and convert transparently)
        self.location           = location
        self.start              = start
        self.end                = end
        self.group_id           = group_id
        self.tracking_number    = str(tracking_number)
        self.request_number     = str(request_number)
        self.priority           = priority
        self.max_airmass        = max_airmass
        self.min_lunar_distance = min_lunar_distance
        self.max_lunar_phase    = max_lunar_phase
        self.max_seeing         = max_seeing
        self.min_transparency   = min_transparency

        self.proposal  = Proposal()
        self.molecules = []
        self.target    = SiderealTarget()

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


    def create_pond_block(self):
        if self.pond_block:
            return self.pond_block

        # Check we have everything we need
        missing_fields = self.list_missing_fields()
        if len(missing_fields) > 0:
            raise IncompleteBlockError(missing_fields)

        molecule_classes = {
                             'EXPOSE'   : Expose,
                             'STANDARD' : Standard
                           }

        # Construct the POND objects...
        # 1) Create a POND ScheduledBlock
        telescope, observatory, site = self.split_location()
        block_build_args = dict(
                                start       = self.start,
                                end         = self.end,
                                site        = site,
                                observatory = observatory,
                                telescope   = telescope,
                                priority    = self.priority
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
                                     ra  = self.target.ra.in_degrees(),
                                     dec = self.target.dec.in_degrees()
                                   )
            # 2b) Construct the Pointing
            pond_pointing = pointing.sidereal(
                                               name  = self.target.name,
                                               coord = coord,
                                             )
        elif isinstance(self.target, NonSiderealTarget):
            if self.target.scheme == 'ASA_MAJOR_PLANET':
                pond_pointing = pointing.nonsidereal_asa_major_planet(name    = self.target.name,
                                                              epochofel = self.target.epochofel,
                                                              orbinc = self.target.orbinc,
                                                              longascnode = self.target.longascnode,
                                                              longofperih = self.target.longofperih,
                                                              meandist    = self.target.meandist, 
                                                              eccentricity = self.target.eccentricity,
                                                              meanlong = self.target.meanlong,
                                                              dailymot = self.target.dailymot
                                                              )
            elif self.target.scheme == 'ASA_MINOR_PLANET':
                pond_pointing = pointing.nonsidereal_asa_minor_planet(name    = self.target.name,
                                                              epochofel = self.target.epochofel,
                                                              orbinc = self.target.orbinc,
                                                              longascnode = self.target.longascnode,
                                                              argofperih = self.target.argofperih,
                                                              meandist    = self.target.meandist, 
                                                              eccentricity = self.target.eccentricity,
                                                              meananom = self.target.meananom,
                                                              )
            elif self.target.scheme == 'ASA_COMET':
                pond_pointing = pointing.nonsidereal_asa_comet(name    = self.target.name,
                                                              epochofel = self.target.epochofel,
                                                              orbinc = self.target.orbinc,
                                                              longascnode = self.target.longascnode,
                                                              argofperih = self.target.argofperih,
                                                              perihdist    = self.target.perihdist, 
                                                              eccentricity = self.target.eccentricity,
                                                              epochofperih = self.target.epochofperih,
                                                              )
            elif self.target.scheme == 'JPL_MAJOR_PLANET':
                pond_pointing = pointing.nonsidereal_jpl_major_planet(name    = self.target.name,
                                                              epochofel = self.target.epochofel,
                                                              orbinc = self.target.orbinc,
                                                              longascnode = self.target.longascnode,
                                                              argofperih = self.target.argofperih,
                                                              meandist    = self.target.meandist, 
                                                              eccentricity = self.target.eccentricity,
                                                              meananom = self.target.meananom,
                                                              dailymot = self.target.dailymot
                                                              )
            elif self.target.scheme == 'JPL_MINOR_PLANET':
                pond_pointing = pointing.nonsidereal_jpl_minor_planet(name    = self.target.name,
                                                              epochofel = self.target.epochofel,
                                                              orbinc = self.target.orbinc,
                                                              longascnode = self.target.longascnode,
                                                              argofperih = self.target.argofperih,
                                                              perihdist    = self.target.perihdist, 
                                                              eccentricity = self.target.eccentricity,
                                                              epochofperih = self.target.epochofperih,
                                                              )
            elif self.target.scheme == 'MPC_MINOR_PLANET':
                pond_pointing = pointing.nonsidereal_mpc_minor_planet(name    = self.target.name,
                                                              epochofel = self.target.epochofel,
                                                              orbinc = self.target.orbinc,
                                                              longascnode = self.target.longascnode,
                                                              argofperih = self.target.argofperih,
                                                              meandist    = self.target.meandist, 
                                                              eccentricity = self.target.eccentricity,
                                                              meananom = self.target.meananom,
                                                              )
            elif self.target.scheme == 'MPC_COMET':
                pond_pointing = pointing.nonsidereal_mpc_comet(name    = self.target.name,
                                                              epochofel = self.target.epochofel,
                                                              orbinc = self.target.orbinc,
                                                              longascnode = self.target.longascnode,
                                                              argofperih = self.target.argofperih,
                                                              perihdist    = self.target.perihdist, 
                                                              eccentricity = self.target.eccentricity,
                                                              epochofperih = self.target.epochofperih,
                                                              )
            else:
                raise Exception("Unsupported orbital element scheme %s" % self.target.scheme)
        else:
            raise Exception("No mapping to POND pointing for type %s" % str(type(self.target)))
        
        

        # 3) Construct the Observations
        observations = []

        mapping = create_camera_mapping("camera_mappings.dat")


        for i, molecule in enumerate(self.molecules):
            mol_summary_msg = "Building molecule %d/%d (%dx%.03d %s)" % (i+1,
                                                            len(self.molecules),
                                                            molecule.exposure_count,
                                                            molecule.exposure_time,
                                                            molecule.filter)
            log.debug(mol_summary_msg)
            ur_log.debug(mol_summary_msg, self.tracking_number)

            specific_camera = resolve_instrument(molecule.instrument_name, site,
                                                 observatory, telescope, mapping)

            msg = "Instrument resolved as '%s'" % specific_camera
            log.debug(msg)
            ur_log.debug(msg, self.tracking_number)

            if not molecule.defocus:
                molecule.defocus = 0.0


            # Create a Standard molecule if that was specified
            if molecule.type.upper() == 'STANDARD':
                msg = "Creating a STANDARD molecule"
                ur_log.debug(msg, self.tracking_number)
                mol_type = molecule_classes['STANDARD']

            # Otherwise, default to creating an Expose molecule
            else:
                # Note if an unsupported type was provided
                if not molecule.type.upper() == 'EXPOSE':
                    msg = "Unsupported molecule type %s provided; defaulting to EXPOSE" % molecule.type
                    log.warn(msg)
                    ur_log.warn(msg, self.tracking_number)
                else:
                    msg = "Creating an EXPOSE molecule"
                    ur_log.debug(msg, self.tracking_number)

                mol_type = molecule_classes['EXPOSE']


            # Build the specified molecule
            obs = mol_type.build(
                                # Meta data
                                tracking_num = self.tracking_number,
                                request_num  = self.request_number,
                                tag          = self.proposal.tag_id,
                                user         = self.proposal.observer_name,
                                proposal     = self.proposal.proposal_id,
                                group        = self.group_id,
                                # Observation details
                                exp_cnt      = molecule.exposure_count,
                                exp_time     = molecule.exposure_time,
                                # TODO: Allow bin_x and bin_y
                                bin          = molecule.bin_x,
                                inst_name    = specific_camera,
                                filters      = molecule.filter,
                                pointing     = pond_pointing,
                                priority     = molecule.priority,
                                defocus      = molecule.defocus,
                              )

            # Resolve the Autoguider if necessary
            if molecule.ag_mode != 'OFF':
                specific_ag = resolve_autoguider(molecule.ag_name, specific_camera,
                                                 site, observatory, telescope, mapping)
                obs.ag_name = specific_ag

                msg = "Autoguider resolved as '%s'" % specific_ag
                log.debug(msg)
                ur_log.debug(msg, self.tracking_number)

            observations.append(obs)

        # 4) Add the Observations to the Block
        for obs in observations:
            pond_block.add_molecule(obs)

        self.pond_block = pond_block

        return pond_block


    def split_location(self):
        '''
            If the location is of the form telescope.observatory.site, then
            extract those separate components and return them. Otherwise, return
            the full location in the place of each component without splitting.

            Examples:  '0m4a.aqwb.coj' -> (0m4a, aqwb, coj)
                       'Maui'          -> (Maui, Maui, Maui)
        '''
        # Split on full stops (sometimes obscurely also known as periods)
        DELIMITER = '.'

        # Number of sections making up the full location string
        N_COMPONENTS = 3

        separated = tuple(self.location.split(DELIMITER))

        if len(separated) == N_COMPONENTS:
            return separated

        # Separation wasn't possible. Selling a house is all about:
        return (self.location, self.location, self.location)


def send_blocks_to_pond(blocks, dry_run=False):
    pond_blocks = []
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
        except (IncompleteBlockError, InstrumentResolutionError) as e:
            msg = "Request %s (UR %s) -> POND block conversion impossible:" % (
                                                               block.request_number,
                                                               block.tracking_number)
            log.error(msg)
            ur_log.error(msg)
            log.error(e)
            ur_log.error(e, block.tracking_number)


    if not dry_run:
        try:
            PondBlock.save_blocks(pond_blocks)
        except BlockSaveException as e:
            log.error(e)

    return



def make_simple_pond_block(compound_reservation, semester_start):
    '''Create a minimal POND block, with no molecule information. This is not
       useful for realistic requests, but helpful for debugging and simulation.'''

    dt_start, dt_end = get_cr_datetimes(compound_reservation, semester_start)

    pond_block = PondBlock.build(
                                    start       = dt_start,
                                    end         = dt_end,
                                    site        = compound_reservation.resource,
                                    observatory = compound_reservation.resource,
                                    telescope   = compound_reservation.resource,
                                    priority    = compound_reservation.priority
                                )
    return pond_block


def make_simple_pond_schedule(schedule, semester_start):
    '''Given a set of Reservations, construct simple POND blocks corresponding to
       them. This is helpful for debugging and simulation.'''

    pond_blocks = []

    for resource_reservations in schedule.values():
        for res in resource_reservations:
            pond_block = make_pond_block(res, semester_start)
            pond_blocks.append(pond_block)

    return pond_blocks


def build_block(reservation, request, compound_request, semester_start):
    res_start, res_end = get_reservation_datetimes(reservation, semester_start)
    block = Block(
                   location           = reservation.scheduled_resource,
                   start              = res_start,
                   end                = res_end,
                   group_id           = compound_request.group_id,
                   tracking_number    = compound_request.tracking_number,
                   request_number     = request.request_number,
                   # Hard-code all scheduler output to a highish number, for now
                   priority           = 30,
                   max_airmass        = request.constraints.max_airmass,
                   min_lunar_distance = request.constraints.min_lunar_distance,
                   max_lunar_phase    = request.constraints.max_lunar_phase,
                   max_seeing         = request.constraints.max_seeing,
                   min_transparency   = request.constraints.min_transparency,
                 )

    block.add_proposal(compound_request.proposal)
    for molecule in request.molecules:
        block.add_molecule(molecule)
    block.add_target(request.target)

    log.debug("Constructing block: RN=%s TN=%s, %s <-> %s, priority %s",
                                     block.request_number, block.tracking_number,
                                     block.start, block.end, block.priority)

    return block


@timeit
def send_schedule_to_pond(schedule, semester_start, dry_run=False):
    '''Convert a kernel schedule into POND blocks, and send them to the POND.'''

    blocks = []
    for resource_name in schedule:
        for reservation in schedule[resource_name]:
            block = build_block(reservation, reservation.request,
                                reservation.compound_request, semester_start)

            blocks.append(block)

    send_blocks_to_pond(blocks, dry_run)

    # Summarise what was supposed to have been sent
    # TODO: Get this from send_blocks_to_pond, since some blocks might not make it
    # The sorting is just a way to iterate through the output in a human-readable way
    n_submitted_total = 0
    for resource_name in sorted(schedule, key=lambda x: x[::-1]):
        n_submitted = len(schedule[resource_name])
        _, block_str = pl(n_submitted, 'block')
        msg = "%d %s to %s..." % (n_submitted, block_str, resource_name)
        log_info_dry_run(msg, dry_run)
        n_submitted_total += n_submitted


    return n_submitted_total


@timeit
def blacklist_running_blocks(ur_list, tels, start, end):
    running_at_tel  = get_network_running_blocks(tels, start, end)

    all_running_blocks = []
    for run_dict in running_at_tel.values():
        running_blocks = run_dict['running']
        all_running_blocks += running_blocks

    log.info("Before applying running blacklist, %d schedulable %s", *pl(len(ur_list), 'UR'))
    log.info("%d %s in the running blacklist", *pl(len(all_running_blocks), 'UR'))
    for block in all_running_blocks:
        msg = "UR %s has a running block (id=%d, finishing at %s)" % (
                                                     block.tracking_num_set()[0],
                                                     block.id,
                                                     block.end
                                                   )
        log.debug(msg)

    all_tns         = [ur.tracking_number for ur in ur_list]
    running_tns     = [block.tracking_num_set()[0] for block in all_running_blocks]
    schedulable_tns = set(all_tns) - set(running_tns)
    schedulable_urs = [ur for ur in ur_list if ur.tracking_number in schedulable_tns]

    log.info("After running blacklist, %d schedulable %s", *pl(len(schedulable_urs), 'UR'))

    return schedulable_urs, running_at_tel


def get_network_running_blocks(tels, start, end):
    n_running_total = 0
    running_at_tel  = {}
    for full_tel_name, tel in tels.iteritems():
        tel_name, obs_name, site_name = full_tel_name.split('.')
        log.debug("Acquiring running blocks and first availability at %s",
                                                          full_tel_name)

        if tel.events:
            cutoff, running = start, []
        else:
            cutoff, running = get_running_blocks(start, end, site_name,
                                                 obs_name, tel_name)

        running_at_tel[full_tel_name] = {
                                          'cutoff'  : cutoff,
                                          'running' : running
                                        }

        n_running    = len(running)
        _, block_str = pl(n_running, 'block')
        log.debug("Found %d running %s at %s", n_running, block_str, full_tel_name)
        n_running_total += n_running

    log.info("Network-wide, found %d running %s", n_running_total, block_str)

    return running_at_tel


@retry_or_reraise(max_tries=6, delay=10)
def get_running_blocks(start, end, site, obs, tel):
    schedule  = Schedule.get(start=start, end=end, site=site,
                             observatory=obs, telescope=tel,
                             canceled_blocks=False)
    cutoff_dt = schedule.end_of_overlap(start)

    running = [b for b in schedule.blocks if b.start < cutoff_dt and
                                             b.tracking_num_set()]

    return cutoff_dt, running


@retry_or_reraise(max_tries=6, delay=10)
def get_deletable_blocks(start, end, site, obs, tel):
    # Only retrieve blocks which have not been cancelled
    schedule  = Schedule.get(start=start, end=end, site=site,
                             observatory=obs, telescope=tel,
                             canceled_blocks=False)

    cutoff_dt = schedule.end_of_overlap(start)
    to_delete = [b for b in schedule.blocks if b.start >= cutoff_dt and
                                               b.tracking_num_set()]

    log.info("Retrieved %d blocks from %s.%s.%s (%s <-> %s)", len(schedule.blocks),
                                                              tel, obs, site,
                                                              start, end)
    log.info("%d/%d were placed by the scheduler and will be deleted", len(to_delete),
                                                                       len(schedule.blocks))
    if to_delete:
        to_delete_nums = [b.tracking_num_set() for b in to_delete]
        log.debug("Deleting: %s", to_delete_nums)

    return to_delete


@timeit
def cancel_schedule(tels, start, end, dry_run=False):
    all_to_delete = []
    for full_tel_name in tels:
        tel, obs, site = full_tel_name.split('.')
        log.info("Cancelling schedule at %s, from %s to %s", full_tel_name,
                                                             start, end)

        to_delete = get_deletable_blocks(start, end, site, obs, tel)

        n_to_delete = len(to_delete)
        all_to_delete.extend(to_delete)

        _, block_str = pl(n_to_delete, 'block')
        msg = "%d %s at %s" % (n_to_delete, block_str, full_tel_name)
        if dry_run:
            msg = "Dry-run: Would have cancelled " + msg
        else:
            msg = "Cancelled " + msg
        log.info(msg)


    cancel_blocks(all_to_delete, dry_run)

    return len(all_to_delete)


def cancel_blocks(to_delete, dry_run=False):

    if dry_run:
        return len(to_delete)

    ids = [b.id for b in to_delete]
    try:
        PondBlock.cancel_blocks(ids, reason="Superceded by new schedule", delete=True)
    except BlockCancelException as e:
        log.error(e)

    return len(to_delete)


class IncompleteBlockError(Exception):
    '''Raised when a block is missing required parameters.'''
    pass

class InstrumentResolutionError(Exception):
    '''Raised when no instrument can be determined for a given resource.'''
    pass

class PondFacadeException(Exception):
    '''Placeholder until POND client raises this exception on our behalf.'''
    pass
