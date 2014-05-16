from __future__ import division

from nose.tools import assert_equal, raises
from nose import SkipTest
from mock       import patch, Mock, MagicMock

from adaptive_scheduler.pond  import (Block, IncompleteBlockError,
                                      InstrumentResolutionError,
                                      PondFacadeException, PondMoleculeFactory,
                                      cancel_blocks,
                                      get_deletable_blocks, cancel_schedule,
                                      send_blocks_to_pond, build_block,
                                      send_schedule_to_pond, retry_or_reraise,
                                      resolve_instrument, resolve_autoguider,
                                      get_network_running_blocks, get_blocks_by_request,
                                      get_network_running_intervals,
    blacklist_running_blocks)
from adaptive_scheduler.model2 import (Proposal, Target,
                                       SiderealTarget, Request,
                                       UserRequest, Constraints,
                                       MoleculeFactory)
from adaptive_scheduler.kernel.reservation_v3 import Reservation_v3 as Reservation
from adaptive_scheduler.camera_mapping        import create_camera_mapping
from adaptive_scheduler.kernel.timepoint      import Timepoint
from adaptive_scheduler.kernel.intervals      import Intervals

import lcogtpond
from lcogtpond import block

from datetime import datetime, timedelta
import collections


def add_two_numbers(x, y):
    return x + y

@retry_or_reraise(max_tries=1, delay=1)
def decorated_add_two_numbers(x, y):
    return x + y

class TestRetryDecorator(object):

    def setup(self):
        fn        = add_two_numbers
        self.decorator = retry_or_reraise(max_tries=4, delay=1)

        self.decorated = self.decorator(fn)


    def test_happy_path_args(self):

        received = self.decorated(2, 3)

        assert_equal(received, 5)


    def test_happy_path_kwargs(self):
        fn = add_two_numbers
        received = self.decorated(x=2, y=3)

        assert_equal(received, 5)


    def test_happy_path_args_and_kwargs(self):
        fn = add_two_numbers
        received = self.decorated(2, y=3)

        assert_equal(received, 5)


    def test_decorated(self):
        received = decorated_add_two_numbers(2, 3)

        assert_equal(received, 5)


    @patch('time.sleep')
    def test_exception_sleep_and_retries_on_failure(self, sleep_mock):
        mock_fn = MagicMock(side_effect=KeyError('foo'))
        decorated = self.decorator(mock_fn)

        try:
            received = decorated(2, 3)
            assert False, 'Should have got a PondFacadeException here'
        except PondFacadeException as e:
            sleep_mock.assert_called_with(1)
            assert_equal(sleep_mock.call_count, 3)


class TestPondMoleculeFactory(object):

    def setup(self):
        self.proposal = Proposal(
                                  observer_name  = 'Eric Saunders',
                                  user_id        = 'esaunders',
                                  proposal_id    = 'Scheduler Testing',
                                  tag_id         = 'admin',
                                  priority       = 2,
                                )

        self.pond_coords = lcogtpond.pointing.ra_dec(
                                                      ra  = 10,
                                                      dec = 20
                                                    )
        self.pond_pointing = lcogtpond.pointing.sidereal(
                                                          name = 'star',
                                                          coord = self.pond_coords
                                                        )
        self.mol_factory = MoleculeFactory()

        self.valid_expose_mol = self.mol_factory.build(
                                    dict(
                                        type            = 'expose',
                                        exposure_count  = 1,
                                        bin_x           = 1,
                                        bin_y           = 1,
                                        instrument_name = '2m0-FLOYDS-SciCam',
                                        exposure_time   = 30,
                                        priority        = 1,
                                        filter          = 'B',
                                        ag_mode         = 'Optional',
                                        )
                                       )

        self.valid_standard_mol = self.mol_factory.build(
                                    dict(
                                        type            = 'standard',
                                        exposure_count  = 1,
                                        bin_x           = 1,
                                        bin_y           = 1,
                                        instrument_name = '2m0-FLOYDS-SciCam',
                                        exposure_time   = 30,
                                        priority        = 1,
                                        filter          = 'B',
                                        ag_mode         = 'Optional',
                                        )
                                       )

        self.valid_spectrum_mol = self.mol_factory.build(
                                    dict(
                                        type            = 'spectrum',
                                        exposure_count  = 1,
                                        bin_x           = 1,
                                        bin_y           = 1,
                                        instrument_name = '2m0-FLOYDS-SciCam',
                                        exposure_time   = 30,
                                        priority        = 1,
                                        spectra_slit    = 'slit_1.6as',
                                        ag_mode         = 'Optional',
                                        )
                                       )

        self.valid_arc_mol = self.mol_factory.build(
                                    dict(
                                        type            = 'arc',
                                        exposure_count  = 1,
                                        bin_x           = 1,
                                        bin_y           = 1,
                                        instrument_name = '2m0-FLOYDS-SciCam',
                                        exposure_time   = 30,
                                        priority        = 1,
                                        spectra_slit    = 'slit_1.6as',
                                        ag_mode         = 'Optional',
                                        )
                                       )

        self.valid_lamp_flat_mol = self.mol_factory.build(
                                    dict(
                                        type            = 'lamp_flat',
                                        exposure_count  = 1,
                                        bin_x           = 1,
                                        bin_y           = 1,
                                        instrument_name = '2m0-FLOYDS-SciCam',
                                        exposure_time   = 30,
                                        priority        = 1,
                                        spectra_slit    = 'slit_1.6as',
                                        ag_mode         = 'Optional',
                                        )
                                       )


    def test_expose_molecule_builds_ok(self):
        mf = PondMoleculeFactory(
                              tracking_number = '0000000001',
                              request_number  = '0000000002',
                              proposal        = self.proposal,
                              group_id        = 'potatoes'
                            )

        pond_mol = mf.build(self.valid_expose_mol, self.pond_pointing)

        assert_equal(type(pond_mol), lcogtpond.molecule.Expose)
        assert_equal(pond_mol.filters, 'B')


    def test_standard_molecule_builds_ok(self):
        mf = PondMoleculeFactory(
                              tracking_number = '0000000001',
                              request_number  = '0000000002',
                              proposal        = self.proposal,
                              group_id        = 'potatoes'
                            )

        pond_mol = mf.build(self.valid_standard_mol, self.pond_pointing)

        assert_equal(type(pond_mol), lcogtpond.molecule.Standard)
        assert_equal(pond_mol.filters, 'B')


    def test_spectrum_molecule_builds_ok(self):
        mf = PondMoleculeFactory(
                              tracking_number = '0000000001',
                              request_number  = '0000000002',
                              proposal        = self.proposal,
                              group_id        = 'potatoes'
                            )

        pond_mol = mf.build(self.valid_spectrum_mol, self.pond_pointing)

        assert_equal(type(pond_mol), lcogtpond.molecule.Spectrum)
        assert_equal(pond_mol._pb_obj.spectra_slit, 'slit_1.6as')


    def test_arc_molecule_builds_ok(self):
        mf = PondMoleculeFactory(
                              tracking_number = '0000000001',
                              request_number  = '0000000002',
                              proposal        = self.proposal,
                              group_id        = 'potatoes'
                            )

        pond_mol = mf.build(self.valid_arc_mol, 'dummy pointing')

        assert_equal(type(pond_mol), lcogtpond.molecule.Arc)
        assert_equal(pond_mol._pb_obj.spectra_slit, 'slit_1.6as')


    def test_lamp_flat_molecule_builds_ok(self):
        mf = PondMoleculeFactory(
                              tracking_number = '0000000001',
                              request_number  = '0000000002',
                              proposal        = self.proposal,
                              group_id        = 'potatoes'
                            )

        pond_mol = mf.build(self.valid_lamp_flat_mol, 'dummy pointing')

        assert_equal(type(pond_mol), lcogtpond.molecule.LampFlat)
        assert_equal(pond_mol._pb_obj.spectra_slit, 'slit_1.6as')



class TestPond(object):

    def setup(self):
        # Metadata missing proposal and tag parameters
        self.proposal = Proposal(observer_name='Eric Saunders')

        self.mapping = create_camera_mapping('camera_mappings.dat')

        self.mol_factory = MoleculeFactory()
        # Molecule missing a binning parameter (which is required)
        self.mol1 = self.mol_factory.build(
                                            dict(
                                              type            = 'expose',
                                              exposure_count  = 1,
                                              instrument_name = 'KB12',
                                              filter          = 'BSSL-UX-020',
                                              ag_mode         = 'OFF',
                                              defocus         = 0.0,
                                            )
                                           )

        self.valid_proposal = Proposal(
                                        observer_name  = 'Eric Saunders',
                                        user_id        = 'esaunders',
                                        proposal_id    = 'Scheduler Testing',
                                        tag_id         = 'admin',
                                        priority       = 2,
                                      )

        self.valid_target = SiderealTarget(
                                    name  = 'deneb',
                                    type  = 'sidereal',
                                    #ra  = '20 41 25.91',
                                    #dec = '+45 16 49.22',
                                    ra  = 310.35795833333333,
                                    dec = 45.280338888888885,
                                    rot_mode  = 'SKY',
                                    rot_angle = 0.0,
                                    acquire_mode  = 'OPTIONAL',
                                  )

        self.valid_expose_mol = self.mol_factory.build(
                                                      dict(
                                                        type            = 'expose',
                                                        exposure_count  = 1,
                                                        bin_x           = 2,
                                                        bin_y           = 2,
                                                        instrument_name = '1m0-SciCam-SBIG',
                                                        filter          = 'B',
                                                        exposure_time   = 30,
                                                        priority        = 1,
                                                        ag_mode         = 'Optional',
                                                        defocus         = 0.0,
                                                      )
                                                       )

        self.valid_standard_mol = self.mol_factory.build(
                                                      dict(
                                                        type            = 'standard',
                                                        exposure_count  = 1,
                                                        bin_x           = 2,
                                                        bin_y           = 2,
                                                        instrument_name = '1m0-SciCam-SBIG',
                                                        filter          = 'B',
                                                        exposure_time   = 30,
                                                        priority        = 1,
                                                        ag_mode         = 'Optional',
                                                        defocus         = 0.0,
                                                      )
                                                       )

        self.valid_spectrum_mol = self.mol_factory.build(
                                                      dict(
                                                        type            = 'spectrum',
                                                        exposure_count  = 1,
                                                        bin_x           = 1,
                                                        bin_y           = 1,
                                                        instrument_name = '2m0-FLOYDS-SciCam',
                                                        exposure_time   = 30,
                                                        priority        = 1,
                                                        spectra_slit    = 'slit_1.6as',
                                                        ag_mode         = 'Optional',
                                                      )
                                                       )

        self.valid_arc_mol = self.mol_factory.build(
                                                      dict(
                                                        type            = 'arc',
                                                        exposure_count  = 1,
                                                        bin_x           = 1,
                                                        bin_y           = 1,
                                                        instrument_name = '2m0-FLOYDS-SciCam',
                                                        exposure_time   = 30,
                                                        priority        = 1,
                                                        spectra_slit    = 'slit_1.6as',
                                                        ag_mode         = 'Optional',
                                                      )
                                                   )

        self.valid_lamp_flat_mol = self.mol_factory.build(
                                                      dict(
                                                        type            = 'lamp_flat',
                                                        exposure_count  = 1,
                                                        bin_x           = 1,
                                                        bin_y           = 1,
                                                        instrument_name = '2m0-FLOYDS-SciCam',
                                                        exposure_time   = 30,
                                                        priority        = 1,
                                                        spectra_slit    = 'slit_1.6as',
                                                        ag_mode         = 'Optional',
                                                     )
                                                        )

        self.one_metre_block = Block(
                                 location = '1m0a.doma.cpt',
                                 start    = datetime(2012, 1, 1, 0, 0, 0),
                                 end      = datetime(2012, 1, 2, 0, 0, 0),
                                 group_id = 'related things',
                                 tracking_number = '0000000001',
                                 request_number  = '0000000001',
                                 camera_mapping = self.mapping,
                               )

        self.two_metre_block = Block(
                                 location = '2m0a.clma.coj',
                                 start    = datetime(2012, 1, 1, 0, 0, 0),
                                 end      = datetime(2012, 1, 2, 0, 0, 0),
                                 group_id = 'related things',
                                 tracking_number = '0000000001',
                                 request_number  = '0000000001',
                                 camera_mapping = self.mapping,
                               )


    def create_pond_block(self, location='1m0a.doma.coj', start=datetime(2012, 1, 1, 0, 0, 0),
                          end=datetime(2012, 1, 2, 0, 0, 0), group_id='group',
                          tracking_number='0000000001', request_number='0000000001'):
        scheduled_block = Block(
                                 location=location,
                                 start=start,
                                 end=end,
                                 group_id=group_id,
                                 tracking_number=tracking_number,
                                 request_number=request_number,
                                 camera_mapping=self.mapping,
                               )

        scheduled_block.add_proposal(self.valid_proposal)
        scheduled_block.add_target(self.valid_target)
        scheduled_block.add_molecule(self.valid_expose_mol)

        return scheduled_block.create_pond_block()


    def test_proposal_lists_missing_fields(self):
        missing  = self.proposal.list_missing_fields()

        assert_equal(
                      missing,
                      ['proposal_id', 'user_id', 'tag_id', 'priority']
                    )


    def test_scheduled_block_lists_missing_fields(self):
        self.two_metre_block.add_proposal(self.proposal)
        self.two_metre_block.add_molecule(self.mol1)
        self.two_metre_block.add_target(SiderealTarget())

        missing = self.two_metre_block.list_missing_fields()

        assert_equal(missing['proposal'], ['proposal_id', 'user_id', 'tag_id', 'priority'])
        assert_equal(missing['molecule'], ['bin_x', 'bin_y', 'exposure_time', 'priority'])
        assert_equal(missing['target'], ['name', 'ra', 'dec'])


    @raises(IncompleteBlockError)
    def test_raises_error_on_incomplete_blocks(self):
        self.two_metre_block.create_pond_block()


    def test_a_valid_expose_block_doesnt_raise_an_exception(self):
        self.one_metre_block.add_proposal(self.valid_proposal)
        self.one_metre_block.add_molecule(self.valid_expose_mol)
        self.one_metre_block.add_target(self.valid_target)

        self.one_metre_block.create_pond_block()

        self.create_pond_block()

    def test_create_pond_block(self):
        received = self.create_pond_block()

    def test_create_pond_block_with_expose_mol(self):
        self.one_metre_block.add_proposal(self.valid_proposal)
        self.one_metre_block.add_molecule(self.valid_expose_mol)
        self.one_metre_block.add_target(self.valid_target)

        received = self.one_metre_block.create_pond_block()
        pond_mol = received.molecules[0]

        assert_equal(len(received.molecules), 1)
        assert_equal(type(pond_mol), lcogtpond.molecule.Expose)
        assert_equal(pond_mol.inst_name, 'kb70')
        assert_equal(pond_mol.ag_name, 'ef02')
        assert_equal(pond_mol.pointing.roll, 310.35795833333333)
        assert_equal(pond_mol.pointing.pitch, 45.280338888888885)


    def test_create_pond_block_with_standard_mol(self):
        self.one_metre_block.add_proposal(self.valid_proposal)
        self.one_metre_block.add_molecule(self.valid_standard_mol)
        self.one_metre_block.add_target(self.valid_target)

        received = self.one_metre_block.create_pond_block()
        pond_mol = received.molecules[0]

        assert_equal(len(received.molecules), 1)
        assert_equal(type(pond_mol), lcogtpond.molecule.Standard)
        assert_equal(pond_mol.inst_name, 'kb70')
        assert_equal(pond_mol.ag_name, 'ef02')
        assert_equal(pond_mol.pointing.roll, 310.35795833333333)
        assert_equal(pond_mol.pointing.pitch, 45.280338888888885)


    def test_create_pond_block_with_spectrum_mol(self):
        self.two_metre_block.add_proposal(self.valid_proposal)
        self.two_metre_block.add_molecule(self.valid_spectrum_mol)
        self.two_metre_block.add_target(self.valid_target)

        received = self.two_metre_block.create_pond_block()
        pond_mol = received.molecules[0]

        assert_equal(len(received.molecules), 1)
        assert_equal(type(pond_mol), lcogtpond.molecule.Spectrum)
        assert_equal(pond_mol.inst_name, 'floyds02')
        assert_equal(pond_mol.ag_name, 'kb37')
        assert_equal(pond_mol.pointing.roll, 310.35795833333333)
        assert_equal(pond_mol.pointing.pitch, 45.280338888888885)


    def test_create_pond_block_with_arc_mol(self):
        self.two_metre_block.add_proposal(self.valid_proposal)
        self.two_metre_block.add_molecule(self.valid_arc_mol)

        received = self.two_metre_block.create_pond_block()
        pond_mol = received.molecules[0]

        assert_equal(len(received.molecules), 1)
        assert_equal(type(pond_mol), lcogtpond.molecule.Arc)
        assert_equal(pond_mol.inst_name, 'floyds02')
        assert_equal(hasattr(pond_mol, 'ag_name'), False)
        assert_equal(hasattr(pond_mol, 'pointing'), False)


    def test_create_pond_block_with_lamp_flat_mol(self):
        self.two_metre_block.add_proposal(self.valid_proposal)
        self.two_metre_block.add_molecule(self.valid_lamp_flat_mol)

        received = self.two_metre_block.create_pond_block()
        pond_mol = received.molecules[0]

        assert_equal(len(received.molecules), 1)
        assert_equal(type(pond_mol), lcogtpond.molecule.LampFlat)
        assert_equal(pond_mol.inst_name, 'floyds02')
        assert_equal(hasattr(pond_mol, 'ag_name'), False)
        assert_equal(hasattr(pond_mol, 'pointing'), False)


    def test_resolve_instrument_pass_through_if_camera_specified(self):
        instrument_name = 'kb12'
        site, obs, tel  = ('lsc', 'doma', '1m0a')

        received = resolve_instrument(instrument_name, site, obs, tel, self.mapping)

        assert_equal(received, 'kb12')


    def test_scicam_instrument_resolves_to_a_specific_camera(self):
        instrument_name = '1M0-SCICAM-SINISTRO'
        site, obs, tel  = ('lsc', 'doma', '1m0a')

        received = resolve_instrument(instrument_name, site, obs, tel, self.mapping)

        assert_equal(received, 'fl02')


    @raises(InstrumentResolutionError)
    def test_no_matching_instrument_raises_an_exception(self):
        instrument_name = '1M0-SCICAM-SINISTRO'
        site, obs, tel  = ('looloo', 'doma', '1m0a')

        received = resolve_instrument(instrument_name, site, obs, tel, self.mapping)


    def test_resolve_autoguider_pass_through_if_camera_specified(self):
        ag_name         = 'kb12'
        inst_name       = 'abcd'
        site, obs, tel  = ('lsc', 'doma', '1m0a')

        received = resolve_autoguider(ag_name, inst_name, site, obs, tel, self.mapping)

        assert_equal(received, 'kb12')


    def test_scicam_autoguider_resolves_to_primary_instrument(self):
        ag_name         = '1M0-SCICAM-SINISTRO'
        inst_name       = 'abcd'
        site, obs, tel  = ('lsc', 'doma', '1m0a')

        received = resolve_autoguider(ag_name, inst_name, site, obs, tel, self.mapping)

        assert_equal(received, 'fl02')


    def test_no_autoguider_resolves_to_preferred_autoguider(self):
        ag_name         = None
        inst_name       = 'fl01'
        site, obs, tel  = ('bpl', 'doma', '1m0a')

        received = resolve_autoguider(ag_name, inst_name, site, obs, tel, self.mapping)

        assert_equal(received, 'ef12')


    @raises(InstrumentResolutionError)
    def test_no_matching_autoguider_raises_an_exception(self):
        ag_name         = None
        inst_name       = 'abcd'
        site, obs, tel  = ('looloo', 'doma', '1m0a')

        received = resolve_autoguider(ag_name, inst_name, site, obs, tel, self.mapping)

    @patch('lcogtpond.schedule.Schedule.get')
    def test_get_too_blocks(self, func_mock):
        too_block = self.create_pond_block(location='1m0a.doma.coj', tracking_number='0000000001')
        non_too_block = self.create_pond_block(location='1m0a.doma.elp', tracking_number='0000000002')

        cutoff_dt = datetime(2013, 8, 18, 0, 0, 0)
        fake_block = {
                      'elp' : [non_too_block],
                      'coj' : [too_block]
                      }
        block_list = TestPondInteractions.configure_mocks(func_mock, cutoff_dt, fake_block)

        ends_after = datetime(2013, 8, 18, 0, 0, 0)
        running_if_starts_before = datetime(2013, 8, 18, 0, 0, 0)
        starts_before = datetime(2013, 8, 18, 0, 0, 0)

        ur1 = UserRequest(
                           operator='single',
                           requests=None,
                           proposal=None,
                           tracking_number='0000000001',
                           group_id=None,
                           expires=None,
                         )

        tels = {
                 '1m0a.doma.elp' : [],
                 '1m0a.doma.coj' : []
               }
        start = datetime(2013, 10, 3)
        end = datetime(2013, 11, 3)

        too_blocks = get_blocks_by_request([ur1], tels, start, end)

        expected = {
                    '1m0a.doma.coj' : Intervals([Timepoint(too_block.start, 'start'),
                                                 Timepoint(too_block.end, 'end')])
                    }
        assert_equal(expected, too_blocks)

class TestPondInteractions(object):
    def setup(self):
        self.start = datetime(2013, 7, 18, 0, 0, 0)
        self.end   = datetime(2013, 9, 18, 0, 0, 0)
        self.site  = 'lsc'
        self.obs   = 'doma'
        self.tel   = '1m0a'

    def make_fake_block(self, start_dt, tracking_num_set):
        class FakeBlock(object):
            def __init__(self, start_dt, tracking_num_set):
                self.start = start_dt
                self._tracking_num_set = tracking_num_set

            def tracking_num_set(self):
                return self._tracking_num_set

            def __repr__(self):
                return "FakeBlock (%s, %s)" % (self.start, self.tracking_num_set())

        return FakeBlock(start_dt, tracking_num_set)


    @staticmethod
    def configure_mocks(func_mock, cutoff_dt, fake_blocks):
        def mapping(**kwargs):
            mock_schedule = Mock(spec=lcogtpond.schedule.Schedule)
            mock_schedule.blocks = fake_blocks if isinstance(fake_blocks, list) else fake_blocks[kwargs.get('site')]
            mock_schedule.end_of_overlap.return_value = cutoff_dt
            return mock_schedule
        
        func_mock.side_effect = mapping

        return

    @patch('lcogtpond.schedule.Schedule.get')
    def test_blacklist_running_blocks_no_running_blocks(self, func_mock):

        cutoff_dt       = datetime(2013, 8, 18, 0, 0, 0)
        fake_block_list = []
        block_list = self.configure_mocks(func_mock, cutoff_dt, fake_block_list)

        ends_after = datetime(2013, 8, 18, 0, 0, 0)
        running_if_starts_before = datetime(2013, 8, 18, 0, 0, 0)
        starts_before = datetime(2013, 8, 18, 0, 0, 0)

        ur1 = UserRequest(
                           operator='single',
                           requests=None,
                           proposal=None,
                           tracking_number='0000000001',
                           group_id=None,
                           expires=None,
                         )

        tel_mock1 = Mock()
        tel_mock2 = Mock()

        tel_mock1.events = [1, 2, 3]
        tel_mock2.events = []

        tels = {
                 '1m0a.doma.elp' : tel_mock1,
                 '1m0a.doma.coj' : tel_mock2
               }

        ur_list = [ur1]
        schedulable_urs = blacklist_running_blocks(ur_list, tels, ends_after, running_if_starts_before, starts_before)[0]

        assert_equal(ur_list, schedulable_urs)


    @patch('lcogtpond.schedule.Schedule.get')
    def test_blacklist_running_blocks_with_running_block(self, func_mock):

        cutoff_dt = datetime(2013, 8, 18, 0, 0, 0)

        block = self.make_fake_block(datetime(2013, 8, 18, 0, 0, 0) - timedelta(seconds=1), tracking_num_set=['0000000001'])
        block.end = datetime(2013, 8, 18, 0, 0, 0) + timedelta(seconds=100)
        block.id = 15


        fake_block_list = [block]
        block_list = self.configure_mocks(func_mock, cutoff_dt, fake_block_list)

        ends_after = datetime(2013, 8, 18, 0, 0, 0)
        running_if_starts_before = datetime(2013, 8, 18, 0, 0, 0)
        starts_before = datetime(2013, 8, 18, 0, 0, 0)

        ur1 = UserRequest(
                           operator='single',
                           requests=None,
                           proposal=None,
                           tracking_number='0000000001',
                           group_id=None,
                           expires=None,
                         )

        tel_mock1 = Mock()
        tel_mock2 = Mock()

        tel_mock1.events = [1, 2, 3]
        tel_mock2.events = []

        tels = {
                 '1m0a.doma.elp' : tel_mock1,
                 '1m0a.doma.coj' : tel_mock2
               }

        ur_list = [ur1]
        schedulable_urs = blacklist_running_blocks(ur_list, tels, ends_after, running_if_starts_before, starts_before)[0]

        assert_equal([], schedulable_urs)

    @patch('lcogtpond.block.Block.cancel_blocks')
    def test_cancel_blocks_not_called_when_dry_run(self, func_mock):
        dry_run = True
        FakeBlock = collections.namedtuple('FakeBlock', 'id')
        to_delete = [FakeBlock(id=id) for id in range(10)]

        cancel_blocks(to_delete, dry_run)
        assert_equal(func_mock.called, False)


    @patch('lcogtpond.block.Block.cancel_blocks')
    def test_cancel_blocks_called_when_dry_run_not_set(self, func_mock):
        dry_run = False
        reason = 'Superceded by new schedule'
        FakeBlock = collections.namedtuple('FakeBlock', 'id')
        ids = range(10)
        to_delete = [FakeBlock(id=id) for id in ids]

        cancel_blocks(to_delete, dry_run)
        func_mock.assert_called_once_with(ids, reason=reason, delete=True)


    @patch('lcogtpond.schedule.Schedule.get')
    def test_delete_blocks_that_exceed_cutoff(self, func_mock):
        cutoff_dt    = datetime(2013, 8, 18, 0, 0, 0)

        # Should be deleted (so return it)
        block_start1 = datetime(2013, 8, 19, 0, 0, 0)
        fake_block1  = self.make_fake_block(block_start1, tracking_num_set=True)

        # Should not be deleted (so don't return it)
        block_start2 = datetime(2013, 8, 17, 0, 0, 0)
        fake_block2  = self.make_fake_block(block_start2, tracking_num_set=True)

        # Should not be deleted (no tracking number)
        block_start3 = datetime(2013, 8, 17, 0, 0, 0)
        fake_block3  = self.make_fake_block(block_start2, tracking_num_set=False)

        # Should not be deleted (no tracking number)
        block_start4 = datetime(2013, 8, 19, 0, 0, 0)
        fake_block4  = self.make_fake_block(block_start2, tracking_num_set=False)

        block_list = self.configure_mocks(func_mock, cutoff_dt,
                                          [fake_block1, fake_block2])

        to_delete = get_deletable_blocks(self.start, self.end, self.site,
                                         self.obs, self.tel)

        assert_equal(to_delete, [fake_block1])


    @patch('adaptive_scheduler.pond.get_deletable_blocks')
    @patch('adaptive_scheduler.pond.cancel_blocks')
    def test_cancel_schedule(self, func_mock1, func_mock2):
        tels = ['1m0a.doma.lsc']
        dry_run = False

        delete_list = [1, 2, 3]

        func_mock2.return_value = delete_list

        n_deleted = cancel_schedule(tels, self.start, self.end, dry_run)

        func_mock2.assert_called_with(self.start, self.end, self.site, self.obs, self.tel)
        func_mock1.assert_called_with(delete_list, False)
        assert_equal(n_deleted, len(delete_list))



    @patch('lcogtpond.block.Block.save_blocks')
    @patch('adaptive_scheduler.pond.ur_log')
    def test_dont_send_blocks_if_dry_run(self, mock_func, mock_func2):
        dry_run = True

        blocks = {'foo' : [Mock()]}

        send_blocks_to_pond(blocks, dry_run)
        assert not mock_func.called, 'Dry run flag was ignored'


    @patch('lcogtpond.block.Block.save_blocks')
    def test_blocks_are_saved_to_pond(self, mock_func):
        dry_run = False

        mock_block = Mock(spec=Block)
        mock_pond_block = Mock()
        mock_block.create_pond_block.return_value = mock_pond_block
        mock_block.request_number  = '0000000001'
        mock_block.tracking_number = '0000000001'

        blocks = {'foo' : [mock_block]}

        send_blocks_to_pond(blocks, dry_run)

        mock_func.assert_called_with([mock_pond_block])



    @patch('adaptive_scheduler.pond.send_blocks_to_pond')
    @patch('adaptive_scheduler.pond.build_block')
    def test_dont_send_schedule_to_pond_if_dry_run(self, mock_func1, mock_func2):

        mock_res_list = [Mock(), Mock()]

        schedule = {
                     '1m0a.doma.lsc' : mock_res_list
                   }

        camera_mappings_file = 'camera_mappings.dat'

        # Choose a value that isn't True or False, since we only want to check the
        # value makes it through to the second mock
        dry_run = 123

        # Each time the mock is called, do this. This allows us to build up a list
        # to test.
        mock_func1.side_effect = lambda v,w,x,y,z : v

        mock_func2.return_value = ( {'1m0a.doma.lsc' : ['block 1', 'block 2']},
                                    {'1m0a.doma.lsc' : ['block 3']} )

        n_submitted_total = send_schedule_to_pond(schedule, self.start,
                                                  camera_mappings_file, dry_run)

        assert_equal(n_submitted_total, 2)
        mock_func2.assert_called_once_with(schedule, dry_run)

    @patch('adaptive_scheduler.pond.get_intervals')
    @patch('adaptive_scheduler.pond.get_running_blocks')
    def test_blocks_arent_running_if_weather(self, mock_func1, mock_func2):

        tel_mock1 = Mock()
        tel_mock2 = Mock()

        tel_mock1.events = [1, 2, 3]
        tel_mock2.events = []

        mock_func1.return_value = ("test", ["test"])

        def return_func(*args, **kwargs):
            if args[0]:
                return "interval"
            else:
                return "empty"

        mock_func2.side_effect = return_func

        tels = {
                 '1m0a.doma.lsc' : tel_mock1,
                 '1m0a.doma.cpt' : tel_mock2
               }
        start = datetime(2013, 10, 3)
        end = datetime(2013, 11, 3)
        now = start - timedelta(minutes=6)

        blocks = get_network_running_blocks(tels, now, start, end)
        received = get_network_running_intervals(blocks)

        expected = {
                    '1m0a.doma.lsc' : "empty",
                    '1m0a.doma.cpt' : "interval"
                    }

        assert_equal(received, expected)

    def test_build_block(self):
        raise SkipTest
        reservation = Reservation(
                                   priority = None,
                                   duration = 10,
                                   possible_windows_dict = {}
                                 )
        reservation.scheduled_start = 0

        proposal = Proposal()
        target   = SiderealTarget()

        compound_request = UserRequest(
                                            operator = 'single',
                                            requests = None,
                                            proposal = proposal,
                                            expires  = None,
                                            tracking_number = None,
                                            group_id = None
                                          )

        constraints = Constraints(
                                   max_airmass        = None,
                                   min_lunar_distance = None,
                                   max_lunar_phase    = None,
                                   max_seeing         = None,
                                   min_transparency   = None
                                 )

        request = Request(
                           target         = target,
                           molecules      = [],
                           windows        = None,
                           constraints    = constraints,
                           request_number = None
                           )

        camera_mappings_file = 'camera_mappings.dat'

        received = build_block(reservation, request, compound_request, self.start,
                               camera_mappings_file)
        missing = received.list_missing_fields()
        print "Missing %r fields" % missing
        1/0
