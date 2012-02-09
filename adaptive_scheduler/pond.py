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


def ScheduledBlock(object):

    def __init__(self, location, start, end):
        self.location = location

        # TODO: Extend to allow datetimes or epoch times (and convert transparently)
        self.start    = start
        self.end      = end

        self.molecules = []


    def is_complete(self):
        pass

    def add_metadata(self, metadata):
        self.metadata = metadata

    def add_molecule(self, molecule):
        # TODO: Handle molecule priorities
        self.molecules.append(molecule)

    def create_pond_block(self):
        pass

    def send_to_pond(self):
        pass




def Metadata(DataContainer):
    pass

def Target(object):
    pass

def Molecule(object):
    pass
