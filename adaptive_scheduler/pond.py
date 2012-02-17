#!/usr/bin/env python

'''
pond.py - Facade to the pond_client library.

This module provides the scheduler's interface for constructing POND objects.
It maps objects across domains from 1) -> 2) (described below).

1) A complete scheduled observation in this facade needs the following:
    * A ScheduledBlock made up of
            * A Metadata object
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

from adaptive_scheduler.model import DataContainer
from lcogt.pond import pond_client


class ScheduledBlock(object):

    def __init__(self, location, start, end, priority=0):
        # TODO: Extend to allow datetimes or epoch times (and convert transparently)
        self.location  = location
        self.start     = start
        self.end       = end
        self.priority  = priority

        self.metadata  = Metadata()
        self.molecules = []
        self.target    = Target()


    def list_missing_fields(self):
        # Find the list of missing metadata fields
        meta_missing = self.metadata.list_missing_fields()

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

        if len(meta_missing) > 0:
            missing_fields['metadata'] = meta_missing

        if len(molecule_missing) > 0:
            missing_fields['molecule'] = molecule_missing

        if len(target_missing) > 0:
            missing_fields['target'] = target_missing


        return missing_fields


    def add_metadata(self, metadata):
        self.metadata = metadata

    def add_molecule(self, molecule):
        # TODO: Handle molecule priorities
        self.molecules.append(molecule)

    def add_target(self, target):
        self.target = target

    def create_pond_block(self, group_id):
        # Check we have everything we need
        missing_fields = self.list_missing_fields()
        if len(missing_fields) > 0:
            raise IncompleteScheduledBlockError(missing_fields)

        # Construct the POND objects...
        # 1) Create a POND ScheduledBlock
        site, observatory, telescope = self.split_location()
        pond_block = pond_client.ScheduledBlock(
                                                 start       = self.start,
                                                 end         = self.end,
                                                 site        = site,
                                                 observatory = observatory,
                                                 telescope   = telescope,
                                                 priority    = self.priority
                                                )

        # 2) Create a Group
        pond_group = pond_client.Group(tag_id   = self.metadata.tag,
                                       user_id  = self.metadata.user,
                                       prop_id  = self.metadata.proposal,
                                       group_id = group_id)

        # 3) Construct the Pointing
        DEFAULT_EQUINOX = 2000.0
        pond_pointing = Pointing.sidereal(
                                           name  = self.target.name,
                                           ra    = self.target.ra,
                                           dec    = self.target.dec,
                                           equinox = DEFAULT_EQUINOX,
                                          )

        # 4) Construct the Observations
        for molecule in self.molecules:
            pond_group.add_expose(
                                   cnt    = molecule.count,
                                   len    = ,
                                   bin    = molecule.binning,
                                   inst   = molecule.instrument_name,
                                   target = pond_pointing,
                                   filter = molecule.filter
                                  )
#TODO: Delete this
# Create a target
point_params = {'source_name':'test_target', 'ra':90.123, 'dec':34.4}
target = pond_client.Pointing.sidereal(**point_params)

# Add an exposure of the target to the scheduled block
expose_params = {'len':10000, 'cnt':1, 'bin':2, 'inst':'KB12',
                 'filter':'BSSL-UX-020', 'target':target}
expose = group.add_expose(**expose_params)
expose_priority = 3
block.add_obs(expose, expose_priority)



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
        pass




class Metadata(DataContainer):
    def list_missing_fields(self):
        req_fields = ('user', 'proposal', 'tag')
        missing_fields = []

        for field in req_fields:
            try:
                getattr(self, field)
            except:
                missing_fields.append(field)

        return missing_fields



class Target(DataContainer):
    def list_missing_fields(self):
        req_fields = ('type', 'ra', 'dec', 'epoch')
        missing_fields = []

        for field in req_fields:
            try:
                getattr(self, field)
            except:
                missing_fields.append(field)

        return missing_fields



class Molecule(DataContainer):
    #TODO: This is really an expose_n molecule, so should be specialised
    #TODO: Specialisation will be necessary once other molecules are scheduled

    def list_missing_fields(self):
        req_fields = ('type', 'count', 'binning', 'instrument_name', 'filter')
        missing_fields = []

        for field in req_fields:
            try:
                getattr(self, field)
            except:
                missing_fields.append(field)

        return missing_fields



class IncompleteScheduledBlockError(Exception):
    '''Raised when a block is missing required parameters.'''

    def __init__(self, value):
        self.value = value

    def __str__(self):
        message = "The following fields are missing in this ScheduledBlock.\n"
        for param_type in self.value:
            message += "%s:\n" % param_type

            for parameter in self.value[param_type]:
                message += "    %s\n" % parameter

        return message