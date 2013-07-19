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

from adaptive_scheduler.model2         import Proposal, Target
from adaptive_scheduler.utils          import get_reservation_datetimes, timeit
from adaptive_scheduler.camera_mapping import create_camera_mapping
from adaptive_scheduler.printing       import pluralise as pl
from lcogtpond                         import pointing
from lcogtpond.block                   import Block as PondBlock
from lcogtpond.molecule                import Expose
from lcogtpond.schedule                import Schedule

# Set up and configure a module scope logger
import logging
log = logging.getLogger(__name__)

#TODO: Remove me
xx = 1

class Block(object):

    def __init__(self, location, start, end, group_id, tracking_number,
                 request_number, priority=0, max_airmass=None,
                 min_lunar_distance=None, max_lunar_phase=None,
                 max_seeing=None, min_transparency=None, store_in_db=True):
        # TODO: Extend to allow datetimes or epoch times (and convert transparently)
        self.location  = location
        self.start     = start
        self.end       = end
        self.group_id  = group_id
        self.tracking_number = tracking_number
        self.request_number = request_number
        self.priority  = priority
        self.max_airmass = max_airmass
        self.min_lunar_distance = min_lunar_distance
        self.max_lunar_phase = max_lunar_phase
        self.max_seeing = max_seeing
        self.min_transparency = min_transparency
        self.store_in_db = store_in_db

        self.proposal  = Proposal()
        self.molecules = []
        self.target    = Target()

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
        # Check we have everything we need
        missing_fields = self.list_missing_fields()
        if len(missing_fields) > 0:
            raise IncompleteBlockError(missing_fields)

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

        # 3) Construct the Observations
        observations = []

        # TODO: Move this somewhere better!
        # TODO: And clean up disgusting hacks
        generic_camera_names = ('SCICAM', 'FASTCAM')
        mapping = create_camera_mapping("camera_mappings.dat")


        for i, molecule in enumerate(self.molecules):
            log.debug("Building molecule %d/%d (%dx%.03d %s)", i+1,
                                                            len(self.molecules),
                                                            molecule.exposure_count,
                                                            molecule.exposure_time,
                                                            molecule.filter)
            specific_camera = molecule.instrument_name
            if molecule.instrument_name in generic_camera_names:
                tel_class = telescope[:-1]
                search = tel_class + '-' + molecule.instrument_name
                inst_match = mapping.find_by_camera_type_and_location(site,
                                                                      observatory,
                                                                      telescope,
                                                                      search)
                specific_camera = inst_match[0]['camera']
                log.debug("Instrument resolved as '%s'", specific_camera)

            obs = Expose.build(
                                # Meta data
                                tracking_num = self.tracking_number,
                                request_num = self.request_number,
                                tag = self.proposal.tag_id,
                                user = self.proposal.observer_name,
                                proposal = self.proposal.proposal_id,
                                group = self.group_id,
                                # Observation details
                                exp_cnt  = molecule.exposure_count,
                                exp_time = molecule.exposure_time,
                                # TODO: Allow bin_x and bin_y
                                bin = molecule.bin_x,
                                inst_name = specific_camera,
                                filters = molecule.filter,
                                pointing = pond_pointing,
                                priority = molecule.priority,
                              )

            # Resolve the Autoguider if necessary
            if molecule.ag_mode != 'OFF':
                # At this point the specific camera was either provided, or has
                # been resolved. We call the mapping again, because we won't have
                # one yet if the specific name was provided, and we need it to find
                # the AG...

                if not molecule.ag_name:
                    ag_match    = mapping.find_by_camera(specific_camera)
                    specific_ag = ag_match[0]['autoguider']

                    log.debug("Autoguider resolved as '%s'", specific_ag)
                else:
                    specific_ag = molecule.ag_name
                    log.debug("Using provided autoguider '%s'", specific_ag)

                obs.ag_name = specific_ag

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


    def send_to_pond(self):
        if not self.pond_block:
            self.create_pond_block()

        msg = "Request %s (part of UR %s) to POND" % (self.request_number,
                                                      self.tracking_number)
        if self.store_in_db:
            self.pond_block.save()
            msg = "Sent " + msg
        else:
            msg = "Dry-run: Would have sent " + msg

        log.debug(msg)
        #TODO: Remove me
        global xx
        fh = open(str(xx) + '.tmp', 'a')
        fh.write(msg + '\n')
        fh.close()

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


@timeit
def send_schedule_to_pond(schedule, semester_start, dry_run=False):
    '''Convert a kernel schedule into POND blocks, and send them to the POND.'''

    n_submitted_total = 0
    for resource_name in schedule:
        n_submitted = len(schedule[resource_name])
        _, block_str = pl(n_submitted, 'block')
        #import ipdb; ipdb.set_trace()

        msg = "%d %s to %s..." % (n_submitted, block_str, resource_name)
        if dry_run:
            msg = "Dry-run: Would be submitting " + msg
        else:
            msg = "Submitting " + msg
        log.info(msg)

        for res in schedule[resource_name]:
            res_start, res_end = get_reservation_datetimes(res, semester_start)
            block = Block(
                           location           = res.scheduled_resource,
                           start              = res_start,
                           end                = res_end,
                           group_id           = res.compound_request.group_id,
                           tracking_number    = res.compound_request.tracking_number,
                           request_number     = res.request.request_number,
                           # Hard-code all scheduler output to a highish number for now
                           priority           = 30,
                           max_airmass        = res.request.constraints.max_airmass,
                           min_lunar_distance = res.request.constraints.min_lunar_distance,
                           max_lunar_phase    = res.request.constraints.max_lunar_phase,
                           max_seeing         = res.request.constraints.max_seeing,
                           min_transparency   = res.request.constraints.min_transparency,
                           store_in_db        = not dry_run,
                         )

            block.add_proposal(res.compound_request.proposal)
            for molecule in res.request.molecules:
                block.add_molecule(molecule)
            block.add_target(res.request.target)

            log.debug("Constructing block: RN=%s TN=%s, %s <-> %s, priority %s",
                                             block.request_number, block.tracking_number,
                                             block.start, block.end, block.priority)
            pond_block = block.send_to_pond()

        n_submitted_total += n_submitted

    #TODO: Remove me
    global xx
    xx += 1

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
    running_at_tel = {}
    for full_tel_name in tels:
        tel, obs, site = full_tel_name.split('.')
        log.debug("Acquiring running blocks and first availability at %s",
                                                          full_tel_name)

        cutoff, running = get_running_blocks(start, end, site, obs, tel)
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


def get_running_blocks(start, end, site, obs, tel):
    schedule  = Schedule.get(start=start, end=end, site=site,
                             observatory=obs, telescope=tel,
                             canceled_blocks=False)
    cutoff_dt = schedule.end_of_overlap(start)

    running = [b for b in schedule.blocks if b.start < cutoff_dt and
                                             b.tracking_num_set()]

    return cutoff_dt, running


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
    log.debug([b.tracking_num_set() for b in to_delete])

    return to_delete

@timeit
def cancel_schedule(tels, start, end, dry_run=False):
    n_deleted_total = 0
    for full_tel_name in tels:
        tel, obs, site = full_tel_name.split('.')
        log.info("Cancelling schedule at %s, from %s to %s", full_tel_name,
                                                             start, end)

        n_deleted = cancel_schedule_at_resource(start, end,
                                                site, obs, tel, dry_run)

        _, block_str = pl(n_deleted, 'block')
        msg = "%d %s at %s" % (n_deleted, block_str, full_tel_name)
        if dry_run:
            msg = "Dry-run: Would have cancelled " + msg
        else:
            msg = "Cancelled " + msg
        log.info(msg)

        n_deleted_total += n_deleted

    return n_deleted_total


def cancel_schedule_at_resource(start, end, site, obs, tel, dry_run=False):
    to_delete = get_deletable_blocks(start, end, site, obs, tel)

    if not dry_run:
        for block in to_delete:
            block.cancel(reason="Superceded by new schedule", delete=True)

    return len(to_delete)


class IncompleteBlockError(Exception):
    '''Raised when a block is missing required parameters.'''

    def __init__(self, value):
        self.value = value
        self.msg = "The following fields are missing in this Block.\n"

    def __str__(self):
        return self.value

    def a__str__(self):
        message = "The following fields are missing in this Block.\n"
        for param_type in self.value:
            message += "%s:\n" % param_type

            for parameter in self.value[param_type]:
                message += "    %s\n" % parameter

        return message
