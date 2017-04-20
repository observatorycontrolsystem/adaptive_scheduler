from __future__ import division

from nose.tools import assert_equal,assert_almost_equal, raises
from nose import SkipTest
from mock       import patch, Mock, MagicMock

from adaptive_scheduler.pond  import (Block, IncompleteBlockError,
                                      InstrumentResolutionError,
                                      PondFacadeException, PondMoleculeFactory,
                                      build_block, retry_or_reraise,
                                      resolve_instrument, resolve_autoguider,
                                      PondScheduleInterface)
from adaptive_scheduler.model2 import (Proposal, Target, SatelliteTarget,
                                       SiderealTarget, Request,
                                       UserRequest, Constraints,
                                       MoleculeFactory)
from adaptive_scheduler.configdb_connections import ConfigDBInterface
from adaptive_scheduler.scheduler import ScheduleException
from adaptive_scheduler.kernel.reservation_v3 import Reservation_v3 as Reservation

from adaptive_scheduler.kernel.timepoint      import Timepoint
from adaptive_scheduler.kernel.intervals      import Intervals
import lcogtpond

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


class TestPondMoleculeFactory(object):

    def setup(self):
        self.proposal = Proposal(
                                  pi  = 'Eric Saunders',
                                  id    = 'Scheduler Testing',
                                  tag         = 'admin',
                                  tac_priority       = 2,
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

        self.valid_auto_focus_mol = self.mol_factory.build(
                                    dict(
                                        type            = 'auto_focus',
                                        exposure_count  = 1,
                                        bin_x           = 1,
                                        bin_y           = 1,
                                        instrument_name = 'kb76',
                                        exposure_time   = 30,
                                        priority        = 1,
                                        filter          = 'B',
                                        ag_mode         = 'Optional',
                                        )
                                       )


        self.valid_zero_pointing_mol = self.mol_factory.build(
                                    dict(
                                        type            = 'zero_pointing',
                                        exposure_count  = 1,
                                        bin_x           = 1,
                                        bin_y           = 1,
                                        instrument_name = 'kb76',
                                        exposure_time   = 30,
                                        priority        = 1,
                                        filter          = 'B',
                                        ag_mode         = 'Optional',
                                        )
                                       )


        self.valid_bias_mol = self.mol_factory.build(
                                    dict(
                                        type            = 'bias',
                                        exposure_count  = 1,
                                        bin_x           = 1,
                                        bin_y           = 1,
                                        instrument_name = 'kb76',
                                        exposure_time   = 30,
                                        priority        = 1,
                                        ag_mode         = 'Optional',
                                        )
                                       )


        self.valid_dark_mol = self.mol_factory.build(
                                    dict(
                                        type            = 'dark',
                                        exposure_count  = 1,
                                        bin_x           = 1,
                                        bin_y           = 1,
                                        instrument_name = 'kb76',
                                        exposure_time   = 30,
                                        priority        = 1,
                                        ag_mode         = 'Optional',
                                        )
                                       )


        self.valid_sky_flat_mol = self.mol_factory.build(
                                    dict(
                                        type            = 'sky_flat',
                                        exposure_count  = 1,
                                        bin_x           = 1,
                                        bin_y           = 1,
                                        instrument_name = 'kb76',
                                        exposure_time   = 30,
                                        priority        = 1,
                                        filter          = 'B',
                                        )
                                       )


        self.valid_spectrum_mol = self.mol_factory.build(
                                    dict(
                                        type                  = 'spectrum',
                                        exposure_count        = 1,
                                        bin_x                 = 1,
                                        bin_y                 = 1,
                                        instrument_name       = '2m0-FLOYDS-SciCam',
                                        exposure_time         = 30,
                                        priority              = 1,
                                        spectra_slit          = 'slit_1.6as',
                                        ag_mode               = 'Optional',
                                        acquire_mode          = 'Brightest',
                                        acquire_radius_arcsec = 10.2,
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
                              group_id        = 'potatoes',
                              submitter       = ''
                            )

        pond_mol = mf.build(self.valid_expose_mol, self.pond_pointing)

        assert_equal(type(pond_mol), lcogtpond.molecule.Expose)
        assert_equal(pond_mol.filters, 'B')


    def test_standard_molecule_builds_ok(self):
        mf = PondMoleculeFactory(
                              tracking_number = '0000000001',
                              request_number  = '0000000002',
                              proposal        = self.proposal,
                              group_id        = 'potatoes',
                              submitter       = ''
                            )

        pond_mol = mf.build(self.valid_standard_mol, self.pond_pointing)

        assert_equal(type(pond_mol), lcogtpond.molecule.Standard)
        assert_equal(pond_mol.filters, 'B')


    def test_auto_focus_molecule_builds_ok(self):
        mf = PondMoleculeFactory(
                              tracking_number = '0000000001',
                              request_number  = '0000000002',
                              proposal        = self.proposal,
                              group_id        = 'potatoes',
                              submitter       = ''
                            )

        pond_mol = mf.build(self.valid_auto_focus_mol, self.pond_pointing)

        assert_equal(type(pond_mol), lcogtpond.molecule.AutoFocus)
        assert_equal(pond_mol.filters, 'B')
        assert_equal(pond_mol.inst_name, 'kb76')


    def test_zero_pointing_molecule_builds_ok(self):
        mf = PondMoleculeFactory(
                              tracking_number = '0000000001',
                              request_number  = '0000000002',
                              proposal        = self.proposal,
                              group_id        = 'potatoes',
                              submitter       = ''
                            )

        pond_mol = mf.build(self.valid_zero_pointing_mol, self.pond_pointing)

        assert_equal(type(pond_mol), lcogtpond.molecule.ZeroPointing)
        assert_equal(pond_mol.filters, 'B')
        assert_equal(pond_mol.inst_name, 'kb76')


    def test_sky_flat_molecule_builds_ok(self):
        mf = PondMoleculeFactory(
                              tracking_number = '0000000001',
                              request_number  = '0000000002',
                              proposal        = self.proposal,
                              group_id        = 'potatoes',
                              submitter       = ''
                            )

        pond_mol = mf.build(self.valid_sky_flat_mol, self.pond_pointing)

        assert_equal(type(pond_mol), lcogtpond.molecule.SkyFlat)
        assert_equal(pond_mol.filters, 'B')
        assert_equal(pond_mol.inst_name, 'kb76')


    def test_bias_molecule_builds_ok(self):
        mf = PondMoleculeFactory(
                              tracking_number = '0000000001',
                              request_number  = '0000000002',
                              proposal        = self.proposal,
                              group_id        = 'potatoes',
                              submitter       = ''
                            )

        pond_mol = mf.build(self.valid_bias_mol, self.pond_pointing)

        assert_equal(type(pond_mol), lcogtpond.molecule.Bias)
        assert_equal(pond_mol.inst_name, 'kb76')


    def test_dark_molecule_builds_ok(self):
        mf = PondMoleculeFactory(
                              tracking_number = '0000000001',
                              request_number  = '0000000002',
                              proposal        = self.proposal,
                              group_id        = 'potatoes',
                              submitter       = ''
                            )

        pond_mol = mf.build(self.valid_dark_mol, self.pond_pointing)

        assert_equal(type(pond_mol), lcogtpond.molecule.Dark)
        assert_equal(pond_mol.inst_name, 'kb76')


    def test_spectrum_molecule_builds_ok(self):
        mf = PondMoleculeFactory(
                              tracking_number = '0000000001',
                              request_number  = '0000000002',
                              proposal        = self.proposal,
                              group_id        = 'potatoes',
                              submitter       = ''
                            )

        pond_mol = mf.build(self.valid_spectrum_mol, self.pond_pointing)

        assert_equal(type(pond_mol), lcogtpond.molecule.Spectrum)
        assert_equal(pond_mol._pb_obj.spectra_slit, 'slit_1.6as')
        assert_equal(pond_mol._pb_obj.acquire_mode, 1)
        assert_equal(pond_mol._pb_obj.acquire_radius_arcsec, 10.2)


    def test_arc_molecule_builds_ok(self):
        mf = PondMoleculeFactory(
                              tracking_number = '0000000001',
                              request_number  = '0000000002',
                              proposal        = self.proposal,
                              group_id        = 'potatoes',
                              submitter       = ''
                            )

        pond_mol = mf.build(self.valid_arc_mol, self.pond_pointing)

        assert_equal(type(pond_mol), lcogtpond.molecule.Arc)
        assert_equal(pond_mol._pb_obj.spectra_slit, 'slit_1.6as')


    def test_lamp_flat_molecule_builds_ok(self):
        mf = PondMoleculeFactory(
                              tracking_number = '0000000001',
                              request_number  = '0000000002',
                              proposal        = self.proposal,
                              group_id        = 'potatoes',
                              submitter       = ''
                            )

        pond_mol = mf.build(self.valid_lamp_flat_mol, self.pond_pointing)

        assert_equal(type(pond_mol), lcogtpond.molecule.LampFlat)
        assert_equal(pond_mol._pb_obj.spectra_slit, 'slit_1.6as')



class TestPond(object):

    def setup(self):
        # Metadata missing proposal and tag parameters
        self.proposal = Proposal(pi='Eric Saunders')

        self.configdb_interface = ConfigDBInterface(configdb_url='',
                                                    telescopes_file='test/telescopes.json',
                                                    active_instruments_file='test/active_instruments.json')

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
                                        pi  = 'Eric Saunders',
                                        id    = 'Scheduler Testing',
                                        tag        = 'admin',
                                        tac_priority       = 2,
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

        self.valid_satellite_target = SatelliteTarget(
                                    name  = 'a satellite',
                                    type  = 'satellite',
                                    rot_mode='',
                                    rot_angle=0.0,
                                    altitude  = 273.582271,
                                    azimuth = 35.734713,
                                    diff_pitch_rate  = -4.696643,
                                    diff_roll_rate = -34.7765912,
                                    diff_pitch_acceleration = -0.0050373,
                                    diff_roll_acceleration = 0.0009016,
                                    diff_epoch_rate = 57504.0423916,
                                  )

        self.valid_target_with_prop_motion = SiderealTarget(
                                    name  = 'deneb',
                                    type  = 'sidereal',
                                    #ra  = '20 41 25.91',
                                    #dec = '+45 16 49.22',
                                    ra  = 316.73026646,
                                    dec = 38.74205644,
                                    rot_mode  = 'SKY',
                                    rot_angle = 0.0,
                                    acquire_mode  = 'OPTIONAL',
                                    proper_motion_ra = 4106.90,
                                    proper_motion_dec = 3144.68,
                                    epoch = 2000.0,
                                    parallax = 549.30
                                  )

        self.valid_expose_mol = self.mol_factory.build(
                                                      dict(
                                                        type            = 'expose',
                                                        exposure_count  = 1,
                                                        bin_x           = 2,
                                                        bin_y           = 2,
                                                        instrument_name = '1m0-SciCam-SINISTRO',
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
                                                        instrument_name = '1m0-SciCam-SINISTRO',
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
                                                        acquire_mode          = 'Brightest',
                                                        acquire_radius_arcsec = 10.2,
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
                                 submitter = '',
                                 request_number  = '0000000001',
                                 configdb_interface = self.configdb_interface,
                               )

        self.two_metre_block = Block(
                                 location = '2m0a.clma.coj',
                                 start    = datetime(2012, 1, 1, 0, 0, 0),
                                 end      = datetime(2012, 1, 2, 0, 0, 0),
                                 group_id = 'related things',
                                 submitter= '',
                                 tracking_number = '0000000001',
                                 request_number  = '0000000001',
                                 configdb_interface = self.configdb_interface,
                               )


    def create_pond_block(self, location='1m0a.doma.coj', start=datetime(2012, 1, 1, 0, 0, 0),
                          end=datetime(2012, 1, 2, 0, 0, 0), group_id='group', submitter='mysubmitter',
                          tracking_number='0000000001', request_number='0000000001'):
        scheduled_block = Block(
                                 location=location,
                                 start=start,
                                 end=end,
                                 group_id=group_id,
                                 tracking_number=tracking_number,
                                 submitter=submitter,
                                 request_number=request_number,
                                 configdb_interface=self.configdb_interface,
                               )

        scheduled_block.add_proposal(self.valid_proposal)
        scheduled_block.add_target(self.valid_target)
        scheduled_block.add_molecule(self.valid_expose_mol)

        return scheduled_block.create_pond_block()


    def test_proposal_lists_missing_fields(self):
        missing  = self.proposal.list_missing_fields()

        assert_equal(
                      missing,
                      ['id', 'tag', 'tac_priority']
                    )


    def test_scheduled_block_lists_missing_fields(self):
        self.two_metre_block.add_proposal(self.proposal)
        self.two_metre_block.add_molecule(self.mol1)
        self.two_metre_block.add_target(SiderealTarget())

        missing = self.two_metre_block.list_missing_fields()

        assert_equal(missing['proposal'], ['id', 'tag', 'tac_priority'])
        assert_equal(missing['molecule'], ['bin_x', 'bin_y', 'exposure_time', 'priority'])
        assert_equal(missing['target'], ['name', 'ra', 'dec'])


    @patch('lcogtpond.schedule.Schedule.get')
    @raises(ScheduleException)
    def test_no_pond_connection_okay(self, func_mock):
        func_mock.side_effect = ScheduleException("bad")

        ur1 = UserRequest(
                           operator='single',
                           requests=None,
                           proposal=None,
                           tracking_number='0000000001',
                           group_id=None,
                           expires=None,
                           ipp_value=1.0,
                           observation_type="TARGET_OF_OPPORTUNITY",
                           submitter=''
                         )

        tels = {
                 '1m0a.doma.elp' : [],
                 '1m0a.doma.coj' : []
               }
        start = datetime(2013, 10, 3)
        end = datetime(2013, 11, 3)

        tracking_numbers = [ur1.tracking_number]
        pond_interface = PondScheduleInterface()
        too_blocks = pond_interface._get_intervals_by_telescope_for_tracking_numbers(tracking_numbers, tels, start, end)
        assert_equal({}, too_blocks)


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

    def test_create_pond_block_satellite_target(self):
        self.one_metre_block.add_proposal(self.valid_proposal)
        self.one_metre_block.add_molecule(self.valid_expose_mol)
        self.one_metre_block.add_target(self.valid_satellite_target)

        received = self.one_metre_block.create_pond_block()
        pond_mol = received.molecules[0]

        # check that the satellite fields are in the molecule
        assert_equal(pond_mol.pointing.roll, self.valid_satellite_target.azimuth)
        assert_equal(pond_mol.pointing.pitch, self.valid_satellite_target.altitude)
        assert_equal(pond_mol.pointing.diff_roll_rate, self.valid_satellite_target.diff_roll_rate)
        assert_equal(pond_mol.pointing.diff_pitch_rate, self.valid_satellite_target.diff_pitch_rate)
        assert_equal(pond_mol.pointing.diff_roll_accel, self.valid_satellite_target.diff_roll_acceleration)
        assert_equal(pond_mol.pointing.diff_pitch_accel, self.valid_satellite_target.diff_pitch_acceleration)
        assert_equal(pond_mol.pointing.diff_epoch_rate, self.valid_satellite_target.diff_epoch_rate)
        assert_equal(pond_mol.pointing.coord_sys, 2)
        assert_equal(pond_mol.pointing.coord_type, 0)
        assert_equal(pond_mol.pointing.ptype, 2)

    def test_create_pond_block_with_expose_mol(self):
        self.one_metre_block.add_proposal(self.valid_proposal)
        self.one_metre_block.add_molecule(self.valid_expose_mol)
        self.one_metre_block.add_target(self.valid_target)

        received = self.one_metre_block.create_pond_block()
        pond_mol = received.molecules[0]

        assert_equal(len(received.molecules), 1)
        assert_equal(type(pond_mol), lcogtpond.molecule.Expose)
        assert_equal(pond_mol.inst_name, 'fl16')
        assert_equal(pond_mol.ag_name, 'ef02')
        assert_equal(pond_mol.pointing.roll, 310.35795833333333)
        assert_equal(pond_mol.pointing.pitch, 45.280338888888885)

    def test_create_pond_block_with_proper_motion(self):
        self.one_metre_block.add_proposal(self.valid_proposal)
        self.one_metre_block.add_molecule(self.valid_expose_mol)
        self.one_metre_block.add_target(self.valid_target_with_prop_motion)

        received = self.one_metre_block.create_pond_block()
        pond_mol = received.molecules[0]

        # According to Rob's calculations, proper motion RA and dec should be as follows
        # See https://issues.lcogt.net/issues/8723 for more info
        converted_proper_motion_ra = 5.265450459478893
        converted_proper_motion_dec = 3.14468
        assert_equal(len(received.molecules), 1)
        assert_equal(type(pond_mol), lcogtpond.molecule.Expose)
        assert_equal(pond_mol.inst_name, 'fl16')
        assert_equal(pond_mol.ag_name, 'ef02')
        assert_almost_equal(pond_mol.pointing.parallax, .54930)
        assert_almost_equal(pond_mol.pointing.pro_mot_ra, converted_proper_motion_ra)
        assert_almost_equal(pond_mol.pointing.pro_mot_dec, converted_proper_motion_dec)
        assert_equal(pond_mol.pointing.epoch, 2000.0)

    def test_create_pond_block_with_standard_mol(self):
        self.one_metre_block.add_proposal(self.valid_proposal)
        self.one_metre_block.add_molecule(self.valid_standard_mol)
        self.one_metre_block.add_target(self.valid_target)

        received = self.one_metre_block.create_pond_block()
        pond_mol = received.molecules[0]

        assert_equal(len(received.molecules), 1)
        assert_equal(type(pond_mol), lcogtpond.molecule.Standard)
        assert_equal(pond_mol.inst_name, 'fl16')
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


    def test_create_pond_block_with_invalid_spectrum_mol(self):
        invalid_spectrum_molecule = self.valid_spectrum_mol
        del invalid_spectrum_molecule.acquire_mode
        del invalid_spectrum_molecule.acquire_radius_arcsec

        self.two_metre_block.add_proposal(self.valid_proposal)
        self.two_metre_block.add_molecule(invalid_spectrum_molecule)
        self.two_metre_block.add_target(self.valid_target)

        try:
            self.two_metre_block.create_pond_block()
            assert False, 'Expected an exception'
        except IncompleteBlockError as e:
            assert_equal(e.message['molecule'], ['acquire_mode', 'acquire_radius_arcsec'])


    def test_create_pond_block_with_arc_mol(self):
        self.two_metre_block.add_proposal(self.valid_proposal)
        self.two_metre_block.add_molecule(self.valid_arc_mol)
        self.two_metre_block.add_target(self.valid_target)

        received = self.two_metre_block.create_pond_block()
        pond_mol = received.molecules[0]

        assert_equal(len(received.molecules), 1)
        assert_equal(type(pond_mol), lcogtpond.molecule.Arc)
        assert_equal(pond_mol.inst_name, 'floyds02')
        assert_equal(hasattr(pond_mol, 'ag_name'), False)
        assert_equal(hasattr(pond_mol, 'pointing'), True)


    def test_create_pond_block_with_lamp_flat_mol(self):
        self.two_metre_block.add_proposal(self.valid_proposal)
        self.two_metre_block.add_molecule(self.valid_lamp_flat_mol)
        self.two_metre_block.add_target(self.valid_target)

        received = self.two_metre_block.create_pond_block()
        pond_mol = received.molecules[0]

        assert_equal(len(received.molecules), 1)
        assert_equal(type(pond_mol), lcogtpond.molecule.LampFlat)
        assert_equal(pond_mol.inst_name, 'floyds02')
        assert_equal(hasattr(pond_mol, 'ag_name'), False)
        assert_equal(hasattr(pond_mol, 'pointing'), True)


    def test_resolve_instrument_pass_through_if_camera_specified(self):
        instrument_name = 'kb12'
        site, obs, tel  = ('lsc', 'doma', '1m0a')

        received = resolve_instrument(instrument_name, site, obs, tel, self.configdb_interface)

        assert_equal(received, 'kb12')


    def test_scicam_instrument_resolves_to_a_specific_camera(self):
        instrument_name = '1M0-SCICAM-SINISTRO'
        site, obs, tel  = ('lsc', 'doma', '1m0a')

        received = resolve_instrument(instrument_name, site, obs, tel, self.configdb_interface)

        assert_equal(received, 'fl15')


    @raises(InstrumentResolutionError)
    def test_no_matching_instrument_raises_an_exception(self):
        instrument_name = '1M0-SCICAM-SINISTRO'
        site, obs, tel  = ('looloo', 'doma', '1m0a')

        received = resolve_instrument(instrument_name, site, obs, tel, self.configdb_interface)


    def test_resolve_autoguider_pass_through_if_camera_specified(self):
        ag_name         = 'ef06'
        inst_name       = 'fl15'
        site, obs, tel  = ('lsc', 'doma', '1m0a')

        received = resolve_autoguider(ag_name, inst_name, site, obs, tel, self.configdb_interface)

        assert_equal(received, 'ef06')


    def test_scicam_autoguider_resolves_to_primary_instrument(self):
        ag_name         = '1M0-SCICAM-SINISTRO'
        specific_inst_name       = 'fl15'
        site, obs, tel  = ('lsc', 'doma', '1m0a')

        received = resolve_autoguider(ag_name, specific_inst_name, site, obs, tel, self.configdb_interface)

        assert_equal(received, 'fl15')


    def test_no_autoguider_resolves_to_preferred_autoguider(self):
        ag_name         = None
        inst_name       = 'fl15'
        site, obs, tel  = ('lsc', 'doma', '1m0a')

        received = resolve_autoguider(ag_name, inst_name, site, obs, tel, self.configdb_interface)

        assert_equal(received, 'ef06')


    @raises(InstrumentResolutionError)
    def test_no_matching_autoguider_raises_an_exception(self):
        ag_name         = None
        inst_name       = 'abcd'
        site, obs, tel  = ('looloo', 'doma', '1m0a')

        received = resolve_autoguider(ag_name, inst_name, site, obs, tel, self.configdb_interface)

    @patch('lcogtpond.schedule.Schedule.get')
    def test_get_too_blocks(self, func_mock):
        too_block = self.create_pond_block(location='1m0a.doma.coj', tracking_number='0000000001')
        non_too_block = self.create_pond_block(location='1m0a.doma.elp', tracking_number='0000000002')

        cutoff_dt = datetime(2013, 8, 18, 0, 0, 0)
        fake_block = {
                      'elp' : [non_too_block],
                      'coj' : [too_block]
                      }
        TestPondInteractions.configure_mocks(func_mock, cutoff_dt, fake_block)

        ur1 = UserRequest(
                           operator='single',
                           requests=None,
                           proposal=None,
                           tracking_number='0000000001',
                           group_id=None,
                           expires=None,
                           ipp_value=1.0,
                           observation_type="TARGET_OF_OPPORTUNITY",
                           submitter=''
                         )

        tels = {
                 '1m0a.doma.elp' : [],
                 '1m0a.doma.coj' : []
               }
        start = datetime(2013, 10, 3)
        end = datetime(2013, 11, 3)

        tracking_numbers = [ur1.tracking_number]
        pond_interface = PondScheduleInterface()
        too_blocks = pond_interface._get_intervals_by_telescope_for_tracking_numbers(tracking_numbers, tels, start, end)

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
        self.configdb_interface = Mock()

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


    @patch('lcogtpond.block.Block.cancel_blocks')
    def test_cancel_blocks_called_when_dry_run_not_set(self, func_mock):
        reason = 'Superceded by new schedule'
        FakeBlock = collections.namedtuple('FakeBlock', 'id')
        ids = range(10)
        to_delete = [FakeBlock(id=id) for id in ids]

        pond_interface = PondScheduleInterface()
        pond_interface._cancel_blocks(to_delete, reason)
        func_mock.assert_called_once_with(to_delete, reason=reason, delete=True, port=None, host=None)


    @patch('adaptive_scheduler.pond.PondScheduleInterface._get_deletable_blocks')
    @patch('adaptive_scheduler.pond.PondScheduleInterface._cancel_blocks')
    def test_cancel_schedule(self, func_mock1, func_mock2):
        start_end_by_resource = {'1m0a.doma.lsc' : (self.start, self.end)}

        delete_list = [Mock(id=1), Mock(id=2), Mock(id=3)]

        func_mock2.return_value = delete_list
        
        pond_interface = PondScheduleInterface()
        n_deleted = pond_interface._cancel_schedule(start_end_by_resource, 'A good reason')

        func_mock2.assert_called_with(self.start, self.end, self.site, self.obs, self.tel)
        func_mock1.assert_called_with([1,2,3], 'A good reason')
        assert_equal(n_deleted, len(delete_list))



    @patch('lcogtpond.block.Block.save_blocks')
    @patch('adaptive_scheduler.pond.ur_log')
    def test_dont_send_blocks_if_dry_run(self, mock_func, mock_func2):
        dry_run = True

        blocks = {'foo' : [Mock()]}
        
        pond_interface = PondScheduleInterface()
        pond_interface._send_blocks_to_pond(blocks, dry_run)
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
        
        pond_interface = PondScheduleInterface()
        pond_interface._send_blocks_to_pond(blocks, dry_run)

        mock_func.assert_called_with([mock_pond_block], host=None, port=None)



    @patch('adaptive_scheduler.pond.PondScheduleInterface._send_blocks_to_pond')
    @patch('adaptive_scheduler.pond.build_block')
    def test_dont_send_schedule_to_pond_if_dry_run(self, mock_func1, mock_func2):

        mock_res_list = [Mock(), Mock()]

        schedule = {
                     '1m0a.doma.lsc' : mock_res_list
                   }

        # Choose a value that isn't True or False, since we only want to check the
        # value makes it through to the second mock
        dry_run = 123

        # Each time the mock is called, do this. This allows us to build up a list
        # to test.
        mock_func1.side_effect = lambda v,w,x,y,z : v

        mock_func2.return_value = ( {'1m0a.doma.lsc' : ['block 1', 'block 2']},
                                    {'1m0a.doma.lsc' : ['block 3']} )

        pond_interface = PondScheduleInterface()
        n_submitted_total = pond_interface._send_schedule_to_pond(schedule, self.start,
                                                  self.configdb_interface, dry_run)

        assert_equal(n_submitted_total, 2)
        mock_func2.assert_called_once_with(schedule, dry_run)
        

    def test_build_normal_block(self):
#         raise SkipTest
        reservation = Reservation(
                                   priority = None,
                                   duration = 10,
                                   possible_windows_dict = {}
                                 )
        reservation.scheduled_start = 0

        proposal = Proposal()
        target   = SiderealTarget()

        user_request = UserRequest(
                                            operator = 'single',
                                            requests = None,
                                            proposal = proposal,
                                            expires  = None,
                                            tracking_number = None,
                                            group_id = None,
                                            ipp_value = 1.0,
                                            observation_type = "NORMAL",
                                            submitter = ''
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

        received = build_block(reservation, request, user_request, self.start,
                               self.configdb_interface)
        missing = received.list_missing_fields()
        print "Missing %r fields" % missing
        
        assert_equal(received.is_too, False, "Should not be a ToO block")
        
    
    def test_build_too_block(self):
#         raise SkipTest
        reservation = Reservation(
                                   priority = None,
                                   duration = 10,
                                   possible_windows_dict = {}
                                 )
        reservation.scheduled_start = 0

        proposal = Proposal()
        target   = SiderealTarget()


        user_request = UserRequest(
                                            operator = 'single',
                                            requests = None,
                                            proposal = proposal,
                                            expires  = None,
                                            tracking_number = None,
                                            group_id = None,
                                            ipp_value = 1.0,
                                            observation_type = "TARGET_OF_OPPORTUNITY",
                                            submitter = ''
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
                           request_number = None,
                           )

        received = build_block(reservation, request, user_request, self.start,
                               self.configdb_interface)
        
        assert_equal(received.is_too, True, "Should be a ToO block")
