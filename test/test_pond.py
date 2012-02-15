from __future__ import division

from nose.tools import assert_equal, raises

from adaptive_scheduler.pond import (Metadata, Molecule, ScheduledBlock,
                                     IncompleteScheduledBlockError)

from datetime import datetime


class TestPond(object):

    def setup(self):
        # Metadata missing proposal and tag parameters
        self.metadata = Metadata(user='Eric')

        # Molecule missing a binning parameter
        self.mol1     = Molecule(
                                  type            = 'expose_n',
                                  count           = 1,
                                  instrument_name = 'KB12',
                                  filter          = 'BSSL-UX-020'
                                 )



    def test_metadata_lists_missing_fields(self):
        missing  = self.metadata.list_missing_fields()

        assert_equal(missing, ['proposal', 'tag'])



    def test_scheduled_block_lists_missing_fields(self):

        scheduled_block = ScheduledBlock(
                                          location = '0m4a.aqwb.coj',
                                          start    = datetime(2012, 1, 1, 0, 0, 0),
                                          end      = datetime(2012, 1, 2, 0, 0, 0)
                                        )

        scheduled_block.add_metadata(self.metadata)
        scheduled_block.add_molecule(self.mol1)


        missing = scheduled_block.list_missing_fields()

        assert_equal(missing, {'metadata'      : ['proposal', 'tag'],
                               'molecule'      : ['binning'],
                               'target'        : ['type', 'ra', 'dec', 'epoch']  })


    @raises(IncompleteScheduledBlockError)
    def test_raises_error_on_incomplete_blocks(self):

        scheduled_block = ScheduledBlock(
                                          location = '0m4a.aqwb.coj',
                                          start    = datetime(2012, 1, 1, 0, 0, 0),
                                          end      = datetime(2012, 1, 2, 0, 0, 0)
                                        )

        scheduled_block.create_pond_block()
