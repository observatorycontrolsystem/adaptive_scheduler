from __future__ import division

from nose.tools import assert_equal, raises
from mock       import patch, Mock

from adaptive_scheduler.pond  import (Block, IncompleteBlockError, cancel_schedule)
from adaptive_scheduler.model2 import (Proposal, Molecule, Target)

from datetime import datetime



class TestPond(object):

    def setup(self):
        # Metadata missing proposal and tag parameters
        self.proposal = Proposal(user_name='Eric')

        # Molecule missing a binning parameter
        self.mol1 = Molecule(
                              type            = 'expose_n',
                              exposure_count  = 1,
                              instrument_name = 'KB12',
                              filter          = 'BSSL-UX-020',
                              ag_mode         = 'OFF'
                            )



    def test_proposal_lists_missing_fields(self):
        missing  = self.proposal.list_missing_fields()

        assert_equal(
                      missing,
                      ['proposal_id', 'user_id', 'tag_id', 'priority']
                    )



    def test_scheduled_block_lists_missing_fields(self):

        scheduled_block = Block(
                                 location = '0m4a.aqwb.coj',
                                 start    = datetime(2012, 1, 1, 0, 0, 0),
                                 end      = datetime(2012, 1, 2, 0, 0, 0),
                                 group_id = 1,
                                 tracking_number = 1,
                                 request_number = 1,
                               )

        scheduled_block.add_proposal(self.proposal)
        scheduled_block.add_molecule(self.mol1)


        missing = scheduled_block.list_missing_fields()

        assert_equal(missing['proposal'], ['proposal_id', 'user_id', 'tag_id', 'priority'])
        assert_equal(missing['molecule'], ['bin_x', 'bin_y', 'exposure_time', 'priority'])
        assert_equal(missing['target'], ['name', 'ra', 'dec'])


    @raises(IncompleteBlockError)
    def test_raises_error_on_incomplete_blocks(self):

        scheduled_block = Block(
                                 location = '0m4a.aqwb.coj',
                                 start    = datetime(2012, 1, 1, 0, 0, 0),
                                 end      = datetime(2012, 1, 2, 0, 0, 0),
                                 group_id = 1,
                                 tracking_number = 1,
                                 request_number = 1,
                                )

        scheduled_block.create_pond_block()


    def test_a_valid_block_doesnt_raise_an_exception(self):


        scheduled_block = Block(
                                 location = '0m4a.aqwb.coj',
                                 start    = datetime(2012, 1, 1, 0, 0, 0),
                                 end      = datetime(2012, 1, 2, 0, 0, 0),
                                 group_id = 'related things',
                                 tracking_number = '0000000001',
                                 request_number  = '0000000001',
                               )

        scheduled_block.add_proposal(
                                      Proposal(
                                                user_name      = 'Eric Saunders',
                                                user_id        = 'esaunders',
                                                proposal_name  = 'Scheduler Testing',
                                                proposal_id    = 'test',
                                                tag_id         = 'admin',
                                                priority       = 2,
                                              )
                                    )

        scheduled_block.add_target(
                                    Target(
                                            name  = 'deneb',
                                            type  = 'sidereal',
                                              #ra  = '20 41 25.91',
                                              #dec = '+45 16 49.22',
                                              ra  = 310.35795833333333,
                                              dec = 45.280338888888885
                                          )
                                  )

        scheduled_block.add_molecule(
                                      Molecule(
                                                type            = 'expose_n',
                                                exposure_count  = 1,
                                                bin_x           = 2,
                                                bin_y           = 2,
                                                instrument_name = 'KB12',
                                                filter          = 'BSSL-UX-020',
                                                exposure_time   = 30,
                                                priority        = 1,
                                                ag_mode         = 'OFF',
                                               )
                                     )


        scheduled_block.create_pond_block()


    def test_split_location_extracts_components(self):

        scheduled_block = Block(
                                 location = '0m4a.aqwb.coj',
                                 start    = datetime(2012, 1, 1, 0, 0, 0),
                                 end      = datetime(2012, 1, 2, 0, 0, 0),
                                 group_id = 'related things',
                                 tracking_number = 1,
                                 request_number = 1,
                               )

        assert_equal(scheduled_block.split_location(), ('0m4a','aqwb','coj'))



    def test_split_location_duplicates_components_if_it_cant_split(self):

        scheduled_block = Block(
                                 location = 'Maui',
                                 start    = datetime(2012, 1, 1, 0, 0, 0),
                                 end      = datetime(2012, 1, 2, 0, 0, 0),
                                 group_id = 'related things',
                                 tracking_number = 1,
                                 request_number = 1,
                               )

        assert_equal(scheduled_block.split_location(), ('Maui','Maui','Maui'))


    @patch('adaptive_scheduler.pond.get_deletable_blocks')
    def test_cancel_schedule(self, func_mock):
        block_mock = Mock()
        func_mock.return_value = [block_mock]
        junk = 'junk'

        cancel_schedule(junk, junk, junk, junk, junk)
        block_mock.cancel.assert_called_once_with(reason="Superceded by new schedule")

