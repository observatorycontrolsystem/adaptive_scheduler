#!/usr/bin/python
from __future__ import division

import mock
from nose.tools import assert_equal, assert_in, raises, nottest, assert_almost_equal, assert_dict_equal
from datetime import datetime

# Import the modules to test
from adaptive_scheduler.model2 import (SiderealTarget, NonSiderealTarget,
                                       Proposal, ConfigurationFactory,
                                       Request, RequestGroup,
                                       Windows, Window, Constraints,
                                       ModelBuilder,
                                       RequestError)
from adaptive_scheduler.configdb_connections import ConfigDBInterface


class TestRequest(object):
    '''Unit tests for the adaptive scheduler Request object.'''

    def setup(self):
        self.target = SiderealTarget(
            name='deneb',
            # ra  = '20 41 25.91',
            # dec = '+45 16 49.22',
            ra=310.35795833333333,
            dec=45.280338888888885,
            epoch=2000,
        )

        self.telescope = dict(
            name='maui',
            latitude=20.7069444444,
            longitude=-156.258055556,
        )

        self.proposal = Proposal(
            proposal_name='LCOSchedulerTest',
            user='Eric Saunders',
            tag='admin',
            time_remaining=10,  # In hours
            priority=1
        )

        self.mol_factory = ConfigurationFactory()

        self.molecule = self.mol_factory.build(
            dict(
                type='expose',
                exposure_count=1,
                bin_x=2,
                bin_y=2,
                instrument_name='KB12',
                filter='BSSL-UX-020',
                exposure_time=20,
                priority=1
            )
        )
        self.constraints = Constraints({})

        self.semester_start = datetime(2011, 11, 1, 0, 0, 0)
        self.semester_end = datetime(2011, 11, 8, 0, 0, 0)
        self.windows = [(self.semester_start, self.semester_end)]

        self.duration = 60
        self.id = 1

    @raises(RequestError)
    def test_invalid_request_type_raises_exception(self):
        junk_res_type = 'chocolate'
        request = Request(target=self.target,
                          molecules=[self.molecule],
                          windows=self.windows,
                          constraints=self.constraints,
                          id=self.id,
                          )
        request_group = RequestGroup(operator=junk_res_type, requests=[request], group_id='Group 1', proposal=Proposal(),
                                    id=1, observation_type='NORMAL', ipp_value=1.0,
                                    expires=datetime(2999, 1, 1), submitter='')

    def test_valid_request_type_does_not_raise_exception(self):
        valid_res_type = 'and'
        request = Request(target=self.target,
                          molecules=[self.molecule],
                          windows=self.windows,
                          constraints=self.constraints,
                          id=self.id,
                          )
        request_group = RequestGroup(operator=valid_res_type, requests=[request], group_id='Group 1', proposal=Proposal(),
                                    id=1, observation_type='NORMAL', ipp_value=1.0,
                                    expires=datetime(2999, 1, 1), submitter='')


class TestRequestGroup(object):
    '''Unit tests for the adaptive scheduler RequestGroup object.'''

    def setup(self):
        pass

    @mock.patch('adaptive_scheduler.model2.event_bus.fire_event')
    def test_emit_user_feedback(self, mock_func):
        request_group_id = 5
        operator = 'single'
        ur = RequestGroup(
            operator=operator,
            requests=[],
            proposal=None,
            expires=None,
            id=request_group_id,
            group_id=None,
            ipp_value=1.0,
            observation_type='NORMAL',
            submitter=''
        )

        msg = 'Yo dude'
        tag = 'MeTag'
        timestamp = datetime(2013, 10, 15, 1, 1, 1)
        ur.emit_rg_feedback(msg, tag, timestamp)

        assert_equal(mock_func.called, True)

    def _build_request_group(self, base_priority=1.0, ipp_value=1.0):
        operator = 'single'

        proposal = Proposal(
            id='LCOSchedulerTest',
            pi='Eric Saunders',
            tag='admin',
            tac_priority=base_priority
        )

        self.mol_factory = ConfigurationFactory()
        molecule1 = self.mol_factory.build(
            dict(
                type='expose',
                exposure_count=1,
                bin_x=2,
                bin_y=2,
                instrument_name='KB12',
                filter='BSSL-UX-020',
                exposure_time=20,
                priority=1
            )
        )

        telescope = dict(
            name='maui',
            latitude=20.7069444444,
            longitude=-156.258055556,
        )
        window_dict = {
            'start': "2013-03-01T00:00:00Z",
            'end': "2013-03-01T00:30:00Z",
        }
        w = Window(
            window_dict=window_dict,
            resource=telescope['name']
        )
        windows = Windows()
        windows.append(w)

        r = Request(
            target=None,
            molecules=[molecule1],
            windows=windows,
            constraints=None,
            id='0000000003',
            duration=10
        )

        rg = RequestGroup(
            operator=operator,
            requests=[r],
            proposal=proposal,
            expires=None,
            id=4,
            ipp_value=ipp_value,
            observation_type='NORMAL',
            group_id=None,
            submitter='Eric Saunders'
        )

        return rg

    def _test_priority(self, base_priority, ipp_value):
        ur = self._build_request_group(base_priority=base_priority, ipp_value=ipp_value)
        assert_almost_equal(base_priority * ipp_value * ur.requests[0].get_duration() / 60.0, ur.get_priority(),
                            delta=(base_priority * ipp_value * ur.requests[0].get_duration() / 60.0) * 0.005)
        assert_equal(base_priority, ur.get_base_priority())
        assert_equal(ipp_value * base_priority, ur.get_ipp_modified_priority())

    def test_priority_ipp_1(self):
        self._test_priority(base_priority=1.0, ipp_value=1.0)

    def test_priority_ipp_1_5(self):
        self._test_priority(base_priority=20.0, ipp_value=1.5)

    def test_drop_empty_children(self):
        r_mock1 = mock.MagicMock()
        r_mock1.has_windows.return_value = True

        r_mock2 = mock.MagicMock()
        r_mock2.has_windows.return_value = False

        ur = RequestGroup(operator='many', requests=[r_mock1, r_mock2], group_id='Group 1', proposal=Proposal(),
                          id=1, observation_type='NORMAL', ipp_value=1.0, expires=datetime(2999, 1, 1),
                          submitter='')

        ur.drop_empty_children()

        assert_equal(len(ur.requests), 1)
        assert_equal(ur.requests[0], r_mock1)


class TestWindows(object):

    def setup(self):
        self.t1 = dict(
            name="Baltic"
        )
        self.t2 = dict(
            name="Sea"
        )

    def test_has_windows_windows(self):
        window_dict = {
            'start': "2013-03-01T00:00:00Z",
            'end': "2013-03-01T00:30:00Z",
        }
        w = Window(
            window_dict=window_dict,
            resource=self.t1['name']
        )
        windows = Windows()
        windows.append(w)

        assert_equal(windows.has_windows(), True)

    def test_has_windows_no_windows(self):
        windows = Windows()
        assert_equal(windows.has_windows(), False)

    def test_is_empty_has_windows_empty_on_one_resource(self):
        window_dict = {
            'start': "2013-03-01T00:00:00Z",
            'end': "2013-03-01T00:30:00Z",
        }
        w = Window(
            window_dict=window_dict,
            resource=self.t1['name']
        )
        w2 = Window(
            window_dict=window_dict,
            resource=self.t2['name']
        )

        windows = Windows()
        windows.append(w)
        windows.append(w2)
        windows.windows_for_resource[self.t2['name']] = []

        assert_equal(windows.has_windows(), True)
        assert_equal(windows.size(), 1)


class TestNonSiderealTarget(object):

    def setup(self):
        pass

    def test_minor_planet_has_required_fields(self):
        initial_data = {'scheme': 'MPC_MINOR_PLANET'}

        target = NonSiderealTarget(initial_data)

        assert_in('meandist', target.required_fields)

    def test_comet_has_required_fields(self):
        initial_data = {'scheme': 'MPC_COMET'}

        target = NonSiderealTarget(initial_data)

        assert_in('perihdist', target.required_fields)

    def test_accepts_lowercase_scheme(self):
        initial_data = {'scheme': 'mpc_minor_planet'}

        target = NonSiderealTarget(initial_data)

        assert_in('meandist', target.required_fields)

    def test_pond_format(self):
        initial_data = {
            'scheme': 'MPC_MINOR_PLANET',
            'name': 'test target',
            'epochofel': 22.2,
            'orbinc': 11.1,
            'longascnode': 21.4,
            'argofperih': 21.3,
            'meandist': 11.2,
            'eccentricity': 0.44,
            'meananom': 20.22
        }
        target = NonSiderealTarget(initial_data)
        pointing = target.in_pond_format()
        assert_equal(pointing['rot_mode'], 'SKY')
        assert_almost_equal(pointing['rot_angle'], 0.0)
        assert_equal(pointing['type'], 'NON_SIDEREAL')
        assert_almost_equal(pointing['eccentricity'], 0.44)


class TestMoleculeFactory(object):
    def setup(self):
        self.mol_factory = ConfigurationFactory()
        self.valid_expose_mol = dict(
            type='expose',
            exposure_count=1,
            bin_x=1,
            bin_y=1,
            instrument_name='2m0-FLOYDS-SciCam',
            exposure_time=30,
            priority=1,
            filter='B',
            ag_mode='Optional',
        )

        self.valid_spectrum_mol = dict(
            type='spectrum',
            exposure_count=1,
            bin_x=1,
            bin_y=1,
            instrument_name='2m0-FLOYDS-SciCam',
            exposure_time=30,
            priority=1,
            spectra_slit='slit_1.6as',
            ag_mode='Optional',
            acquire_mode='Brightest',
            acquire_radius_arcsec=10.2,
        )

    def test_expose_molecule_builds_okay(self):
        molecule = self.mol_factory.build(self.valid_expose_mol)
        assert_equal(molecule.list_missing_fields(), [])

    def test_expose_molecule_dict_is_saved(self):
        molecule = self.mol_factory.build(self.valid_expose_mol)
        assert_dict_equal(self.valid_expose_mol, molecule.mol_dict)

    def test_spectrum_molecule_builds_okay(self):
        molecule = self.mol_factory.build(self.valid_spectrum_mol)
        assert_equal(molecule.list_missing_fields(), [])

    def test_invalid_molecule_lists_missing_fields(self):
        invalid_expose = self.valid_expose_mol.copy()
        del invalid_expose['exposure_count']
        del invalid_expose['bin_x']

        molecule = self.mol_factory.build(invalid_expose)
        assert_in('bin_x', molecule.list_missing_fields())
        assert_in('exposure_count', molecule.list_missing_fields())


class TestModelBuilder(object):

    def setup(self):
        self.target = {
            'name': 'MY Target',
            'type': 'SIDEREAL',
        }
        self.molecules = [
            {
                'instrument_name': '1m0-SciCam-Sinistro',
                'type': 'expose',
                'filter': 'B',
            },
        ]
        self.location = {
            'telescope_class': '1m0',
        }
        self.windows = [
            {
                'start': datetime(2013, 1, 1),
                'end': datetime(2013, 1, 2),
            },
        ]
        self.constraints = {}
        self.id = 2
        self.state = 'PENDING'

        self.mb = ModelBuilder(mock.MagicMock(), ConfigDBInterface(configdb_url='',
                                                                   active_instruments_file='test/active_instruments.json',
                                                                   telescopes_file='test/telescopes.json'))

    def test_build_request_sinistro_resolves_to_lsc_subnetwork(self):
        location = self.location.copy()
        location['site'] = 'lsc'
        req_dict = {
            'target': self.target,
            'molecules': self.molecules,
            'location': location,
            'windows': self.windows,
            'constraints': self.constraints,
            'id': self.id,
            'duration': 10,
            'state': self.state,
        }

        request = self.mb.build_request(req_dict)
        assert_equal(set(['1m0a.doma.lsc', '1m0a.domb.lsc', '1m0a.domc.lsc']),
                     set(request.windows.windows_for_resource.keys()))

    def test_build_request_fl03_resolves_to_lsc_telescope(self):
        req_dict = {
            'target': self.target,
            'molecules': [
                {
                    'instrument_name': 'fl03',
                    'type': 'expose',
                    'filter': 'B',
                },
            ],
            'location': self.location,
            'windows': self.windows,
            'constraints': self.constraints,
            'id': self.id,
            'duration': 10,
            'state': self.state,
        }

        request = self.mb.build_request(req_dict)
        assert_equal(set(['1m0a.domb.lsc']),
                     set(request.windows.windows_for_resource.keys()))

    def test_build_request_slit_2as_resolves_to_coj_telescope(self):
        req_dict = {
            'target': self.target,
            'molecules': [
                {
                    'instrument_name': '2m0-FLOYDS-SciCam',
                    'type': 'arc',
                    'spectra_slit': 'slit_2.0as',
                },
            ],
            'location': {
                'telescope_class': '2m0',
            },
            'windows': self.windows,
            'constraints': self.constraints,
            'id': self.id,
            'duration': 10,
            'state': self.state,
        }

        request = self.mb.build_request(req_dict)
        assert_equal(set(['2m0a.clma.coj', '2m0a.clma.ogg']),
                     set(request.windows.windows_for_resource.keys()))

    @raises(RequestError)
    def test_dont_accept_weird_target_types(self):
        req_dict = {
            'target': {
                'name': 'Potato Target',
                'type': 'POTATOES',
            },
            'molecules': self.molecules,
            'location': self.location,
            'windows': self.windows,
            'constraints': self.constraints,
            'id': self.id,
            'duration': 10,
            'state': self.state,
        }

        request = self.mb.build_request(req_dict)

    @raises(RequestError)
    def test_dont_accept_molecules_with_different_instruments(self):
        req_dict = {
            'target': self.target,
            'molecules': [
                {
                    'instrument_name': 'SciCam',
                    'type': 'expose',
                },
                {
                    'instrument_name': '1m0-SciCam-Sinistro',
                    'type': 'expose',
                },
            ],
            'location': self.location,
            'windows': self.windows,
            'constraints': self.constraints,
            'id': self.id,
            'duration': 10,
            'state': self.state,
        }

        request = self.mb.build_request(req_dict)

    @raises(RequestError)
    def test_dont_accept_cameras_not_present_on_a_subnetwork(self):
        req_dict = {
            'target': self.target,
            'molecules': [
                {
                    'instrument_name': 'POTATOES',
                    'type': 'expose',
                    'filter': 'B',
                },
            ],
            'location': self.location,
            'windows': self.windows,
            'constraints': self.constraints,
            'id': self.id,
            'duration': 10,
            'state': self.state,
        }

        request = self.mb.build_request(req_dict)

    @raises(RequestError)
    def test_dont_accept_filters_not_present_on_a_subnetwork(self):
        req_dict = {
            'target': self.target,
            'molecules': [
                {
                    'instrument_name': '1m0-SciCam-Sinistro',
                    'type': 'expose',
                    'filter': 'fake',
                },
            ],
            'location': self.location,
            'windows': self.windows,
            'constraints': self.constraints,
            'id': self.id,
            'duration': 10,
            'state': self.state,
        }

        request = self.mb.build_request(req_dict)

    @mock.patch('adaptive_scheduler.model2.ModelBuilder.get_semester_details')
    @mock.patch('adaptive_scheduler.model2.ModelBuilder.get_proposal_details')
    def test_build_request_observation_type_normal(self, mock_proposal, mock_semester):
        mock_semester.return_value = {'id': '2013A', 'start': datetime(2013, 1, 1), 'end': datetime(2014, 1, 1)}
        mock_proposal.return_value = Proposal({'id': 'TestProposal', 'pi': '', 'tag': '', 'tac_priority': 10})
        req_dict = {
            'target': self.target,
            'molecules': self.molecules,
            'location': self.location,
            'windows': self.windows,
            'constraints': self.constraints,
            'id': self.id,
            'duration': 10,
            'state': self.state,
        }

        cr_dict = {
            'proposal': 'TestProposal',
            'expires': '2014-10-29 12:12:12',
            'group_id': '',
            'id': '1',
            'ipp_value': '1.0',
            'operator': 'many',
            'requests': [req_dict, ],
            'observation_type': 'NORMAL',
        }

        request_group_model, invalid_requests = self.mb.build_request_group(cr_dict)
        assert_equal(request_group_model.observation_type, 'NORMAL')

    @mock.patch('adaptive_scheduler.model2.ModelBuilder.get_semester_details')
    @mock.patch('adaptive_scheduler.model2.ModelBuilder.get_proposal_details')
    def test_build_request_observation_type_rapid_response(self, mock_proposal, mock_semester):
        mock_semester.return_value = {'id': '2013A', 'start': datetime(2013, 1, 1), 'end': datetime(2014, 1, 1)}
        mock_proposal.return_value = Proposal({'id': 'TestProposal', 'pi': '', 'tag': '', 'tac_priority': 10})
        req_dict = {
            'target': self.target,
            'molecules': self.molecules,
            'location': self.location,
            'windows': self.windows,
            'constraints': self.constraints,
            'id': self.id,
            'duration': 10,
            'state': self.state,
        }

        cr_dict = {
            'proposal': 'TestProposal',
            'expires': '2014-10-29 12:12:12',
            'group_id': '',
            'id': '1',
            'ipp_value': '1.0',
            'operator': 'many',
            'requests': [req_dict, ],
            'observation_type': 'RAPID_RESPONSE',
        }

        request_group_model, invalid_requests = self.mb.build_request_group(cr_dict)
        assert_equal(request_group_model.observation_type, 'RAPID_RESPONSE')

    @raises(RequestError)
    @mock.patch('adaptive_scheduler.model2.ModelBuilder.get_semester_details')
    @mock.patch('adaptive_scheduler.model2.ModelBuilder.get_proposal_details')
    def test_dont_accept_unsupported_observation_type(self, mock_proposal, mock_semester):
        mock_semester.return_value = {'id': '2013A', 'start': datetime(2013, 1, 1), 'end': datetime(2014, 1, 1)}
        mock_proposal.return_value = Proposal({'id': 'TestProposal', 'pi': '', 'tag': '', 'tac_priority': 10})
        req_dict = {
            'target': self.target,
            'molecules': self.molecules,
            'location': self.location,
            'windows': self.windows,
            'constraints': self.constraints,
            'id': self.id,
            'duration': 10.0,
            'state': self.state,
        }

        cr_dict = {
            'proposal': 'TestProposal',
            'expires': '2014-10-29 12:12:12',
            'group_id': '',
            'id': '1',
            'ipp_value': '1.0',
            'operator': 'many',
            'requests': [req_dict, ],
            'observation_type': 'ABNORMAL',
        }

        request_group_model, invalid_requests = self.mb.build_request_group(cr_dict)

    @mock.patch('adaptive_scheduler.model2.ModelBuilder.get_semester_details')
    @mock.patch('adaptive_scheduler.model2.ModelBuilder.get_proposal_details')
    def test_build_request_group_returns_invalid_request_groups(self, mock_proposal, mock_semester):
        mock_semester.return_value = {'id': '2013A', 'start': datetime(2013, 1, 1), 'end': datetime(2014, 1, 1)}
        mock_proposal.return_value = Proposal({'id': 'TestProposal', 'pi': '', 'tag': '', 'tac_priority': 10})
        bad_req_dict = {
            'target': {
                'name': 'Potato Target',
                'type': 'POTATOES',
            },
            'molecules': self.molecules,
            'location': self.location,
            'windows': self.windows,
            'constraints': self.constraints,
            'id': '2',
            'duration': 10,
            'state': self.state,
        }

        good_req_dict = {
            'target': self.target,
            'molecules': [
                {
                    'instrument_name': '1M0-SCICAM-SINISTRO',
                    'type': 'expose',
                    'filter': 'B',
                },
            ],
            'location': self.location,
            'windows': self.windows,
            'constraints': self.constraints,
            'id': '3',
            'duration': 10,
            'state': self.state,
        }

        cr_dict = {
            'proposal': 'TestProposal',
            'expires': '2014-10-29 12:12:12',
            'group_id': '',
            'id': '1',
            'ipp_value': '1.0',
            'operator': 'many',
            'observation_type': 'NORMAL',
            'submitter': '',
            'requests': [bad_req_dict, good_req_dict]
        }

        request_group_model, invalid_requests = self.mb.build_request_group(cr_dict)

        assert_equal(1, len(request_group_model.requests))
        assert_equal(1, len(invalid_requests))
        assert_equal(bad_req_dict, invalid_requests[0])
