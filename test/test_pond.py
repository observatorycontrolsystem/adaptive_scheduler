from __future__ import division

from nose.tools import assert_equal, raises

from adaptive_scheduler.pond import (Block, IncompleteBlockError)
from adaptive_scheduler.model import (Proposal, Molecule, Target)

from datetime import datetime


class TestPond(object):

    def setup(self):
        # Metadata missing proposal and tag parameters
        self.proposal = Proposal(user='Eric')

        # Molecule missing a binning parameter
        self.mol1     = Molecule(
                                  type            = 'expose_n',
                                  count           = 1,
                                  instrument_name = 'KB12',
                                  filter          = 'BSSL-UX-020'
                                 )



    def test_proposal_lists_missing_fields(self):
        missing  = self.proposal.list_missing_fields()

        assert_equal(missing, ['proposal_name', 'tag'])



    def test_scheduled_block_lists_missing_fields(self):

        scheduled_block = Block(
                                 location = '0m4a.aqwb.coj',
                                 start    = datetime(2012, 1, 1, 0, 0, 0),
                                 end      = datetime(2012, 1, 2, 0, 0, 0),
                                 group_id = 1
                                )

        scheduled_block.add_proposal(self.proposal)
        scheduled_block.add_molecule(self.mol1)


        missing = scheduled_block.list_missing_fields()

        assert_equal(missing, {'proposal'      : ['proposal_name', 'tag'],
                               'molecule'      : ['binning', 'duration'],
                               'target'        : ['ra', 'dec']  })


    @raises(IncompleteBlockError)
    def test_raises_error_on_incomplete_blocks(self):

        scheduled_block = Block(
                                 location = '0m4a.aqwb.coj',
                                 start    = datetime(2012, 1, 1, 0, 0, 0),
                                 end      = datetime(2012, 1, 2, 0, 0, 0),
                                 group_id = 1
                                )

        scheduled_block.create_pond_block()


    def test_a_valid_block_doesnt_raise_an_exception(self):


        scheduled_block = Block(
                                 location = '0m4a.aqwb.coj',
                                 start    = datetime(2012, 1, 1, 0, 0, 0),
                                 end      = datetime(2012, 1, 2, 0, 0, 0),
                                 group_id = 'related things'
                                )

        scheduled_block.add_proposal(
                                      Proposal(
                                                user          = 'Eric',
                                                proposal_name = 'Scheduler Testing',
                                                tag           = 'admin'
                                               )
                                     )

        scheduled_block.add_target(
                                    Target(
                                            name  = 'deneb',
                                            type  = 'sidereal',
                                            ra    = '20 41 25.91',
                                            dec   = '+45 16 49.22',
                                          )
                                   )

        scheduled_block.add_molecule(
                                      Molecule(
                                                type            = 'expose_n',
                                                count           = 1,
                                                binning         = 2,
                                                instrument_name = 'KB12',
                                                filter          = 'BSSL-UX-020',
                                                duration        = 30
                                               )
                                     )


        scheduled_block.create_pond_block()


    def test_split_location_extracts_components(self):

        scheduled_block = Block(
                                 location = '0m4a.aqwb.coj',
                                 start    = datetime(2012, 1, 1, 0, 0, 0),
                                 end      = datetime(2012, 1, 2, 0, 0, 0),
                                 group_id = 'related things'
                                )

        assert_equal(scheduled_block.split_location(), ('0m4a','aqwb','coj'))



    def test_split_location_duplicates_components_if_it_cant_split(self):

        scheduled_block = Block(
                                 location = 'Maui',
                                 start    = datetime(2012, 1, 1, 0, 0, 0),
                                 end      = datetime(2012, 1, 2, 0, 0, 0),
                                 group_id = 'related things'
                                )

        assert_equal(scheduled_block.split_location(), ('Maui','Maui','Maui'))
