from __future__ import division

from nose.tools import assert_equal, raises
from mock import patch, Mock

from adaptive_scheduler.observations import (InstrumentResolutionError, build_observation, resolve_instrument,
                                             resolve_autoguider, ObservationScheduleInterface)
from adaptive_scheduler.models import (Proposal, ICRSTarget, Request, RequestGroup)
from adaptive_scheduler.configdb_connections import ConfigDBInterface
from adaptive_scheduler.scheduler import ScheduleException
from adaptive_scheduler.kernel.reservation_v3 import Reservation_v3 as Reservation

from datetime import datetime
import responses
import os


class TestObservations(object):

    def setup(self):
        # Metadata missing proposal and tag parameters
        self.proposal = Proposal(pi='Eric Saunders')

        self.configdb_interface = ConfigDBInterface(configdb_url='', telescope_classes=[],
                                                    telescopes_file='test/telescopes.json',
                                                    active_instruments_file='test/active_instruments.json')

        self.valid_proposal = Proposal(
            pi='Eric Saunders',
            id='Scheduler Testing',
            tag='admin',
            tac_priority=2,
        )

        self.valid_target = ICRSTarget(
            name='deneb',
            type='ICRS',
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

    def test_proposal_lists_missing_fields(self):
        missing = self.proposal.list_missing_fields()

        assert_equal(
            missing,
            ['id', 'tag', 'tac_priority']
        )

    @raises(ScheduleException)
    @responses.activate
    def test_no_observation_portal_connection_okay(self):
        tels = {
            '1m0a.doma.elp': [],
            '1m0a.doma.coj': []
        }
        start = datetime(2013, 10, 3)
        end = datetime(2013, 11, 3)

        host = os.getenv('OBSERVATION_PORTAL_URL', 'http://observation-portal-dev.lco.gtn')
        get_endpoint = host + '/api/observations/'
        responses.add(responses.GET, get_endpoint,
                      json={"error": 'failed to get Observation Portal observations'}, status=500)

        observation_schedule_interface = ObservationScheduleInterface(host=host)
        rr_blocks = observation_schedule_interface._get_rr_observations_by_telescope(tels, start, end)
        assert_equal({}, rr_blocks)

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


class TestObservationInteractions(object):
    def setup(self):
        self.start = datetime(2013, 7, 18, 0, 0, 0)
        self.end = datetime(2013, 9, 18, 0, 0, 0)
        self.site = 'lsc'
        self.obs = 'doma'
        self.tel = '1m0a'
        self.configdb_interface = Mock()

    def make_fake_block(self, start_dt, request_group_id_set):
        class FakeBlock(object):
            def __init__(self, start_dt, request_group_id_set):
                self.start = start_dt
                self._request_group_id_set = request_group_id_set

            def request_group_id_set(self):
                return self._request_group_id_set

            def __repr__(self):
                return "FakeBlock (%s, %s)" % (self.start, self.request_group_id_set())

        return FakeBlock(start_dt, request_group_id_set)

    @responses.activate
    def test_cancel_blocks_called_when_dry_run_not_set(self):
        ids = list(range(10))
        host = os.getenv('OBSERVATION_PORTAL_URL', 'http://observation-portal-dev.lco.gtn')
        cancel_endpoint = host + '/api/observations/cancel/'
        responses.add(responses.POST, cancel_endpoint, json={"canceled": "yay"}, status=200)

        schedule_interface = ObservationScheduleInterface(host=host)
        schedule_interface._cancel_observations(ids)

    @responses.activate
    def test_cancel_schedule(self):
        start_end_by_resource = {'1m0a.doma.lsc': [(self.start, self.end), ]}

        delete_list = [Mock(id=1), Mock(id=2), Mock(id=3)]
        host = os.getenv('OBSERVATION_PORTAL_URL', 'http://observation-portal-dev.lco.gtn')
        cancel_endpoint = host + '/api/observations/cancel/'
        responses.add(responses.POST, cancel_endpoint,
                      json={"canceled": len(delete_list)}, status=200)

        schedule_interface = ObservationScheduleInterface(host=host)
        n_deleted = schedule_interface._cancel_schedule(start_end_by_resource, True, True, True)

        assert_equal(n_deleted, len(delete_list))

    @patch('adaptive_scheduler.observations.build_observation')
    def test_dont_send_schedule_to_observation_portal_if_dry_run(self, mock_func1):
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

        schedule_interface = ObservationScheduleInterface()
        n_submitted_total = schedule_interface._send_schedule_to_observation_portal(schedule, self.start,
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
        target = ICRSTarget({'name': 'test', 'ra': 23.3, 'dec': 22.2})

        request_group = RequestGroup(
            operator='single',
            requests=None,
            proposal=proposal,
            expires=None,
            rg_id=333333,
            is_staff=False,
            name=None,
            ipp_value=1.0,
            observation_type="NORMAL",
            submitter=''
        )

        configuration = Mock()
        configuration.guiding_config = {'mode': 'OFF'}
        configuration.type = 'EXPOSE'
        configuration.id = 11
        configuration.instrument_type = '1M0-TEST-SCICAM'
        configuration.constraints = {}
        configuration.target = target

        request = Request(
            configurations=[configuration],
            windows=None,
            request_id=22222
        )

        reservation.request = request
        reservation.request_group = request_group
        configdb_interface = Mock()
        configdb_interface.get_specific_instrument.return_value='xx01'
        received = build_observation(reservation, self.start, configdb_interface)

        assert_equal(received['request'], 22222)
        assert_equal(received['site'], 'bpl')
        assert_equal(received['enclosure'], 'doma')
        assert_equal(received['telescope'], '1m0a')
        assert_equal(received['configuration_statuses'][0]['configuration'], 11)
        assert_equal(received['configuration_statuses'][0]['instrument_name'], 'xx01')

    def test_build_rr_observation(self):
        reservation = Reservation(
            priority=None,
            duration=10,
            possible_windows_dict={}
        )
        reservation.scheduled_start = 0
        reservation.scheduled_resource = '1m0a.doma.bpl'

        proposal = Proposal({'id': 'testPro', 'tag': 'tagPro', 'tac_priority': 39, 'pi': 'me'})
        target = ICRSTarget({'name': 'test', 'ra': 23.3, 'dec': 22.2})

        request_group = RequestGroup(
            operator='single',
            requests=None,
            proposal=proposal,
            expires=None,
            rg_id=333333,
            is_staff=False,
            name=None,
            ipp_value=1.0,
            observation_type="RAPID_RESPONSE",
            submitter=''
        )

        configuration = Mock()
        configuration.guiding_config = {'mode': 'ON', 'optional': True}
        configuration.type = 'EXPOSE'
        configuration.instrument_type = '1M0-FAKE-SCICAM'
        configuration.constraints = {}
        configuration.id = 13
        configuration.target = target

        request = Request(
            configurations=[configuration],
            windows=None,
            request_id=22223,
        )

        reservation.request = request
        reservation.request_group = request_group
        configdb_interface = Mock()
        configdb_interface.get_specific_instrument.return_value = 'xx03'
        configdb_interface.get_autoguider_for_instrument.return_value='xx04'
        received = build_observation(reservation, self.start, configdb_interface)

        assert_equal(received['request'], 22223)
        assert_equal(received['site'], 'bpl')
        assert_equal(received['enclosure'], 'doma')
        assert_equal(received['telescope'], '1m0a')
        assert_equal(received['configuration_statuses'][0]['configuration'], 13)
        assert_equal(received['configuration_statuses'][0]['instrument_name'], 'xx03')
        assert_equal(received['configuration_statuses'][0]['guide_camera_name'], 'xx04')
