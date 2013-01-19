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
from adaptive_scheduler.utils          import get_reservation_datetimes
from adaptive_scheduler.camera_mapping import create_camera_mapping
from lcogtpond                         import pointing
from lcogtpond.block                   import Block as PondBlock
from lcogtpond.molecule                import Expose
from lcogtpond.schedule                import Schedule

class Block(object):

    def __init__(self, location, start, end, group_id,
                 tracking_number, request_number, priority=0):
        # TODO: Extend to allow datetimes or epoch times (and convert transparently)
        self.location  = location
        self.start     = start
        self.end       = end
        self.group_id  = group_id
        self.tracking_number = tracking_number
        self.request_number = request_number
        self.priority  = priority

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
        pond_block = PondBlock.build(
                                      start       = self.start,
                                      end         = self.end,
                                      site        = site,
                                      observatory = observatory,
                                      telescope   = telescope,
                                      priority    = self.priority
                                    )

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


        for molecule in self.molecules:
            specific_camera = molecule.instrument_name
            if molecule.instrument_name in generic_camera_names:
                tel_class = telescope[:-1]
                search = tel_class + '-' + molecule.instrument_name
                inst_match = mapping.find_by_camera_type_and_location(site,
                                                                      observatory,
                                                                      telescope,
                                                                      search)
                specific_camera = inst_match[0]['camera']

            obs = Expose.build(
                                # Meta data
                                tracking_num = self.tracking_number,
                                request_num = self.request_number,
                                tag = self.proposal.tag_id,
                                user = self.proposal.user_id,
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

                ag_match    = mapping.find_by_camera(specific_camera)
                specific_ag = ag_match[0]['autoguider']

                print "Autoguider resolved as '%s'" % specific_ag
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

        self.pond_block.save()

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


def send_schedule_to_pond(schedule, semester_start):
    '''Convert a kernel schedule into POND blocks, and send them to the POND.'''

    for resource_name in schedule:
        for res in schedule[resource_name]:

            res_start, res_end = get_reservation_datetimes(res, semester_start)
            block = Block(
                           location        = res.scheduled_resource,
                           start           = res_start,
                           end             = res_end,
                           group_id        = res.compound_request.group_id,
                           tracking_number = res.compound_request.tracking_number,
                           request_number  = res.request.request_number,
                           priority        = res.priority,
                         )

            block.add_proposal(res.compound_request.proposal)
            for molecule in res.request.molecules:
                block.add_molecule(molecule)
            block.add_target(res.request.target)

            pond_block = block.send_to_pond()

    return


def cancel_schedule(start, end, site, obs, tel):
    schedule = Schedule.get(start=start, end=end, site=site, obs=obs, tel=tel)
    dt = datetime.utcnow()
    cutoff_dt = schedule.end_of_overlap(dt)
    to_delete = [b for b in schedule.blocks if b.start > cutoff_dt and
                                               b.tracking_num_set()]

    print "Retrieved %d blocks from %s.%s.%s (%s <-> %s), of which:" % (
                                                              len(schedule.blocks),
                                                              tel, obs, site,
                                                              start, end
                                                            )
    print "    %d/%d haven't yet begun" % (len(still_pending), len(schedule.blocks))
    print "    %d/%d were placed by the scheduler and will be deleted" % (
                                                                 len(to_delete),
                                                                 len(schedule.blocks)
                                                                )

    for block in to_delete:
        pass
        #block.cancel(reason="Superceded by new schedule")

    return


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
