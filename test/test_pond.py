from __future__ import division

from nose.tools import assert_equal, assert_almost_equal, raises
from nose import SkipTest
from mock import patch, Mock, MagicMock

from adaptive_scheduler.pond import (InstrumentResolutionError, build_block, resolve_instrument, resolve_autoguider,
                                     PondScheduleInterface)
from adaptive_scheduler.model2 import (Proposal, Target, SatelliteTarget,
                                       SiderealTarget, Request,
                                       UserRequest, Constraints,
                                       MoleculeFactory)
from adaptive_scheduler.utils import datetime_to_normalised_epoch
from adaptive_scheduler.configdb_connections import ConfigDBInterface
from adaptive_scheduler.scheduler import ScheduleException
from adaptive_scheduler.kernel.reservation_v3 import Reservation_v3 as Reservation

from time_intervals.intervals import Intervals

from datetime import datetime
import responses
import os


class TestPond(object):

    def setup(self):
        # Metadata missing proposal and tag parameters
        self.proposal = Proposal(pi='Eric Saunders')

        self.configdb_interface = ConfigDBInterface(configdb_url='',
                                                    telescopes_file='test/telescopes.json',
                                                    active_instruments_file='test/active_instruments.json')

        self.valid_proposal = Proposal(
            pi='Eric Saunders',
            id='Scheduler Testing',
            tag='admin',
            tac_priority=2,
        )

        self.valid_target = SiderealTarget(
            name='deneb',
            type='sidereal',
            # ra  = '20 41 25.91',
            # dec = '+45 16 49.22',
            ra=310.35795833333333,
            dec=45.280338888888885,
            rot_mode='SKY',
            rot_angle=0.0,
            acquire_mode='OPTIONAL',
        )

        self.valid_expose_mol = dict(
            type='expose',
            exposure_count=1,
            bin_x=2,
            bin_y=2,
            instrument_name='1m0-SciCam-SINISTRO',
            filter='B',
            exposure_time=30,
            priority=1,
            ag_mode='Optional',
            defocus=0.0,
        )

    def create_pond_block(self, location='1m0a.doma.coj', start=datetime(2012, 1, 1, 0, 0, 0),
                          end=datetime(2012, 1, 2, 0, 0, 0), group_id='group', submitter='mysubmitter',
                          tracking_number='0000000001', request_number='0000000001'):
        reservation = Mock()
        reservation.scheduled_resource = location
        semester_start = datetime(2012, 1, 1)
        reservation.scheduled_start = datetime_to_normalised_epoch(start, semester_start)
        reservation.duration = (end - start).total_seconds()

        request = Mock()
        request.constraints = Mock()
        request.constraints.max_airmass = 2.0
        request.constraints.min_lunar_distance = 30.0
        request.constraints.max_lunar_phase = 0
        request.constraints.max_seeing = 0
        request.constraints.min_transparency = 0
        request.request_number = request_number
        request.target = self.valid_target
        request.molecules = [self.valid_expose_mol, ]

        user_request = Mock()
        user_request.proposal = self.valid_proposal
        user_request.submitter = submitter
        user_request.group_id = group_id,
        user_request.tracking_number = tracking_number

        scheduled_block = build_block(reservation, request, user_request, semester_start, self.configdb_interface)

        return scheduled_block

    def test_proposal_lists_missing_fields(self):
        missing = self.proposal.list_missing_fields()

        assert_equal(
            missing,
            ['id', 'tag', 'tac_priority']
        )

    @raises(ScheduleException)
    @responses.activate
    def test_no_pond_connection_okay(self):
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
            '1m0a.doma.elp': [],
            '1m0a.doma.coj': []
        }
        start = datetime(2013, 10, 3)
        end = datetime(2013, 11, 3)

        host = os.getenv('POND_HOST', 'ponddev.lco.gtn')
        get_endpoint = host + '/blocks/'
        responses.add(responses.GET, get_endpoint,
                      json={"error": 'failed to get pond blocks'}, status=500)

        tracking_numbers = [ur1.tracking_number]
        pond_interface = PondScheduleInterface(host=host)
        too_blocks = pond_interface._get_blocks_by_telescope_for_tracking_numbers(tracking_numbers, tels, start, end)
        assert_equal({}, too_blocks)

    def test_scicam_instrument_resolves_to_a_specific_camera(self):
        instrument_type = '1M0-SCICAM-SINISTRO'
        site, obs, tel = ('lsc', 'doma', '1m0a')
        received = resolve_instrument(instrument_type, site, obs, tel, self.configdb_interface)
        assert_equal(received, 'fl15')

    @raises(InstrumentResolutionError)
    def test_no_matching_instrument_raises_an_exception(self):
        instrument_type = '1M0-SCICAM-SINISTRO'
        site, obs, tel = ('looloo', 'doma', '1m0a')
        resolve_instrument(instrument_type, site, obs, tel, self.configdb_interface)

    def test_scicam_autoguider_resolves_to_primary_instrument(self):
        self_guide = True
        specific_inst_name = 'fl15'
        site, obs, tel = ('lsc', 'doma', '1m0a')
        received = resolve_autoguider(self_guide, specific_inst_name, site, obs, tel, self.configdb_interface)
        assert_equal(received, 'fl15')

    def test_no_autoguider_resolves_to_preferred_autoguider(self):
        self_guide = False
        inst_name = 'fl15'
        site, obs, tel = ('lsc', 'doma', '1m0a')
        received = resolve_autoguider(self_guide, inst_name, site, obs, tel, self.configdb_interface)
        assert_equal(received, 'ef06')

    @raises(InstrumentResolutionError)
    def test_no_matching_autoguider_raises_an_exception(self):
        self_guide = True
        inst_name = 'abcd'
        site, obs, tel = ('looloo', 'doma', '1m0a')
        resolve_autoguider(self_guide, inst_name, site, obs, tel, self.configdb_interface)


class TestPondInteractions(object):
    def setup(self):
        self.start = datetime(2013, 7, 18, 0, 0, 0)
        self.end = datetime(2013, 9, 18, 0, 0, 0)
        self.site = 'lsc'
        self.obs = 'doma'
        self.tel = '1m0a'
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

    @responses.activate
    def test_cancel_blocks_called_when_dry_run_not_set(self):
        reason = 'Superceded by new schedule'
        ids = range(10)
        host = os.getenv('POND_HOST', 'ponddev.lco.gtn')
        cancel_endpoint = host + '/blocks/cancel/'
        responses.add(responses.POST, cancel_endpoint, json={"canceled": "yay"}, status=200)

        pond_interface = PondScheduleInterface(host=host)
        pond_interface._cancel_blocks(ids, reason)

    @responses.activate
    def test_cancel_schedule(self):
        start_end_by_resource = {'1m0a.doma.lsc': [(self.start, self.end), ]}

        delete_list = [Mock(id=1), Mock(id=2), Mock(id=3)]
        host = os.getenv('POND_HOST', 'ponddev.lco.gtn')
        cancel_endpoint = host + '/blocks/cancel/'
        responses.add(responses.POST, cancel_endpoint,
                      json={"canceled": len(delete_list)}, status=200)

        pond_interface = PondScheduleInterface(host=host)
        n_deleted = pond_interface._cancel_schedule(start_end_by_resource, 'A good reason', True, True)

        assert_equal(n_deleted, len(delete_list))

    @patch('adaptive_scheduler.pond.build_block')
    def test_dont_send_schedule_to_pond_if_dry_run(self, mock_func1):
        mock_res_list = [Mock(), Mock()]

        schedule = {
            '1m0a.doma.lsc': mock_res_list,
            '1m0a.domb.lsc': [Mock()]
        }

        # Choose a value that isn't True or False, since we only want to check the
        # value makes it through to the second mock
        dry_run = 123

        mock_func1.return_value = {'1m0a.doma.lsc': ['block 1', 'block 2'],
                                   '1m0a.domb.lsc': ['block 3']}

        pond_interface = PondScheduleInterface()
        n_submitted_total = pond_interface._send_schedule_to_pond(schedule, self.start,
                                                                  self.configdb_interface, dry_run)

        assert_equal(n_submitted_total, 3)

    def test_build_normal_block(self):
        reservation = Reservation(
            priority=None,
            duration=10,
            possible_windows_dict={}
        )
        reservation.scheduled_start = 0
        reservation.scheduled_resource = '1m0a.doma.bpl'

        proposal = Proposal({'id': 'testPro', 'tag': 'tagPro', 'tac_priority': 39, 'pi': 'me'})
        target = SiderealTarget({'name': 'test', 'ra': 23.3, 'dec': 22.2})

        user_request = UserRequest(
            operator='single',
            requests=None,
            proposal=proposal,
            expires=None,
            tracking_number=333333,
            group_id=None,
            ipp_value=1.0,
            observation_type="NORMAL",
            submitter=''
        )

        constraints = Constraints(
            max_airmass=None,
            min_lunar_distance=None,
            max_lunar_phase=None,
            max_seeing=None,
            min_transparency=None
        )

        molecule = Mock()
        molecule.ag_mode = 'OFF'
        molecule.type = 'EXPOSE'
        molecule.mol_dict = {'ag_mode': 'OFF', 'expmeter_mode': 'OFF', 'expmeter_snr': 0, 'exposure_time': 30,
                             'exposure_count': 1}
        molecule.exposure_count = 1
        molecule.exposure_time = 30
        molecule.instrument_name = 'xx01'

        request = Request(
            target=target,
            molecules=[molecule],
            windows=None,
            constraints=constraints,
            request_number=22222
        )

        received = build_block(reservation, request, user_request, self.start,
                               self.configdb_interface)

        assert_equal(received['is_too'], False, "Should not be a ToO block")

    def test_build_too_block(self):
        reservation = Reservation(
            priority=None,
            duration=10,
            possible_windows_dict={}
        )
        reservation.scheduled_start = 0
        reservation.scheduled_resource = '1m0a.doma.bpl'

        proposal = Proposal({'id': 'testPro', 'tag': 'tagPro', 'tac_priority': 39, 'pi': 'me'})
        target = SiderealTarget({'name': 'test', 'ra': 23.3, 'dec': 22.2})

        user_request = UserRequest(
            operator='single',
            requests=None,
            proposal=proposal,
            expires=None,
            tracking_number=333333,
            group_id=None,
            ipp_value=1.0,
            observation_type="TARGET_OF_OPPORTUNITY",
            submitter=''
        )

        constraints = Constraints(
            max_airmass=None,
            min_lunar_distance=None,
            max_lunar_phase=None,
            max_seeing=None,
            min_transparency=None
        )

        molecule = Mock()
        molecule.ag_mode = 'OFF'
        molecule.type = 'EXPOSE'
        molecule.mol_dict = {'ag_mode': 'OFF', 'expmeter_mode': 'OFF', 'expmeter_snr': 0, 'exposure_time': 30,
                             'exposure_count': 1}
        molecule.exposure_count = 1
        molecule.exposure_time = 30
        molecule.instrument_name = 'xx01'

        request = Request(
            target=target,
            molecules=[molecule],
            windows=None,
            constraints=constraints,
            request_number=22222,
        )

        received = build_block(reservation, request, user_request, self.start,
                               self.configdb_interface)

        assert_equal(received['is_too'], True, "Should be a ToO block")
