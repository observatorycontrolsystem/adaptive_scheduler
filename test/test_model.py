#!/usr/bin/python
from __future__ import division

import mock
from nose.tools import assert_equal, assert_in, raises, nottest, assert_almost_equal
from datetime import datetime

# Import the modules to test
from adaptive_scheduler.model      import (SiderealTarget, NonSiderealTarget,
                                           Proposal, MoleculeFactory,
                                           Request, UserRequest,
                                           Windows, Window, Constraints,
                                           ModelBuilder,
                                           RequestError)
from adaptive_scheduler.configdb_connections import ConfigDBInterface


class TestRequest(object):
    '''Unit tests for the adaptive scheduler Request object.'''

    def setup(self):
        self.target = SiderealTarget(
                                      name  = 'deneb',
                                      #ra  = '20 41 25.91',
                                      #dec = '+45 16 49.22',
                                      ra  = 310.35795833333333,
                                      dec = 45.280338888888885,
                                      epoch = 2000,
                                     )

        self.telescope = dict(
                                    name      = 'maui',
                                    latitude  = 20.7069444444,
                                    longitude = -156.258055556,
                                  )

        self.proposal = Proposal(
                                  proposal_name  = 'LCOSchedulerTest',
                                  user           = 'Eric Saunders',
                                  tag            = 'admin',
                                  time_remaining = 10,               # In hours
                                  priority       = 1
                                )

        self.mol_factory = MoleculeFactory()

        self.molecule = self.mol_factory.build(
                                                dict(
                                                  type            = 'expose',
                                                  exposure_count  = 1,
                                                  bin_x           = 2,
                                                  bin_y           = 2,
                                                  instrument_name = 'KB12',
                                                  filter          = 'BSSL-UX-020',
                                                  exposure_time   = 20,
                                                  priority        = 1
                                                )
                                              )
        self.constraints = Constraints({})

        self.semester_start = datetime(2011, 11, 1, 0, 0, 0)
        self.semester_end   = datetime(2011, 11, 8, 0, 0, 0)
        self.windows = [(self.semester_start, self.semester_end)]

        self.duration = 60
        self.request_number = '0000000001'

    @raises(RequestError)
    def test_invalid_request_type_raises_exception(self):
        junk_res_type = 'chocolate'
        request = Request(target         = self.target,
                          molecules      = [self.molecule],
                          windows        = self.windows,
                          constraints    = self.constraints,
                          request_number = self.request_number,
                          )
        user_request = UserRequest(operator=junk_res_type, requests=[request], group_id='Group 1', proposal=Proposal(),
                                   tracking_number=1, observation_type='NORMAL', ipp_value=1.0,
                                   expires=datetime(2999,1,1), submitter='')


    def test_valid_request_type_does_not_raise_exception(self):
        valid_res_type = 'and'
        request = Request(target         = self.target,
                          molecules      = [self.molecule],
                          windows        = self.windows,
                          constraints    = self.constraints,
                          request_number = self.request_number,
                          )
        user_request = UserRequest(operator=valid_res_type, requests=[request], group_id='Group 1', proposal=Proposal(),
                                   tracking_number=1, observation_type='NORMAL', ipp_value=1.0,
                                   expires=datetime(2999,1,1), submitter='')


class TestUserRequest(object):
    '''Unit tests for the adaptive scheduler UserRequest object.'''

    def setup(self):
        pass

    @mock.patch('adaptive_scheduler.model2.event_bus.fire_event')
    def test_emit_user_feedback(self, mock_func):
        tracking_number = '0000000005'
        operator = 'single'
        ur = UserRequest(
                          operator = operator,
                          requests = [],
                          proposal = None,
                          expires  = None,
                          tracking_number = tracking_number,
                          group_id = None,
                          ipp_value = 1.0,
                          observation_type='NORMAL',
                          submitter=''
                         )

        msg = 'Yo dude'
        tag = 'MeTag'
        timestamp = datetime(2013, 10, 15, 1, 1, 1)
        ur.emit_user_feedback(msg, tag, timestamp)

        assert_equal(mock_func.called, True)

    def _build_user_request(self, base_priority=1.0, ipp_value=1.0):
        tracking_number = '0000000005'
        operator = 'single'

        proposal = Proposal(
                             id  = 'LCOSchedulerTest',
                             pi           = 'Eric Saunders',
                             tag            = 'admin',
                             tac_priority       = base_priority
                           )

        self.mol_factory = MoleculeFactory()
        molecule1 = self.mol_factory.build(
                            dict(
                                  type            = 'expose',
                                  exposure_count  = 1,
                                  bin_x           = 2,
                                  bin_y           = 2,
                                  instrument_name = 'KB12',
                                  filter          = 'BSSL-UX-020',
                                  exposure_time   = 20,
                                  priority        = 1
                                )
                            )

        telescope = dict(
            name='maui',
            latitude=20.7069444444,
            longitude=-156.258055556,
        )
        window_dict = {
                        'start' : "2013-03-01T00:00:00Z",
                        'end'   : "2013-03-01T00:30:00Z",
                      }
        w = Window(
                    window_dict = window_dict,
                    resource    = telescope['name']
                  )
        windows = Windows()
        windows.append(w)

        r = Request(
                     target = None,
                     molecules = [molecule1],
                     windows = windows,
                     constraints = None,
                     request_number = '0000000003',
                     duration = 10
                   )

        ur = UserRequest(
                          operator = operator,
                          requests = [r],
                          proposal = proposal,
                          expires  = None,
                          tracking_number = '000000004',
                          ipp_value = ipp_value,
                          observation_type='NORMAL',
                          group_id = None,
                          submitter='Eric Saunders'
                         )

        return ur

    def _test_priority(self, base_priority, ipp_value):
        ur = self._build_user_request(base_priority=base_priority, ipp_value=ipp_value)
        assert_almost_equal(base_priority*ipp_value*ur.requests[0].get_duration() / 60.0, ur.get_priority(),
                            delta=(base_priority*ipp_value*ur.requests[0].get_duration() / 60.0)*0.005)
        assert_equal(base_priority, ur.get_base_priority())
        assert_equal(ipp_value*base_priority, ur.get_ipp_modified_priority())

    def test_priority_ipp_1(self):
        self._test_priority(base_priority=1.0, ipp_value=1.0)

    def test_priority_ipp_1_5(self):
        self._test_priority(base_priority=20.0, ipp_value=1.5)

    def test_drop_empty_children(self):
        r_mock1 = mock.MagicMock()
        r_mock1.has_windows.return_value = True

        r_mock2 = mock.MagicMock()
        r_mock2.has_windows.return_value = False

        ur = UserRequest(operator='many', requests=[r_mock1, r_mock2], group_id='Group 1', proposal=Proposal(),
                         tracking_number=1, observation_type='NORMAL', ipp_value=1.0, expires=datetime(2999,1,1),
                         submitter='')

        ur.drop_empty_children()

        assert_equal(len(ur.requests), 1)
        assert_equal(ur.requests[0], r_mock1)


# class TestLocationExpander(object):
#
#     def setup(self):
#         self.telescopes = (
#                             '0m4a.aqwa.bpl.0m4',
#                             '0m4b.aqwa.bpl.0m4',
#                             '1m0a.doma.elp.1m0',
#                             '1m0a.doma.bpl.1m0',
#                             '2m0a.clma.ogg.2m0',
#                             '2m0a.clma.coj.2m0',
#                           )
#
#         self.le = _LocationExpander(self.telescopes)
#
#
#     def test_expand_locations_no_filtering_if_empty_dict(self):
#         dict_repr = {
#                       'telescope_class' : None,
#                       'site'            : None,
#                       'observatory'     : None,
#                       'telescope'       : None
#                     }
#
#         received = self.le.expand_locations(dict_repr)
#
#         # We expect the full telescope list, with the telescope classes trimmed off
#         expected = [ '.'.join(loc.split('.')[:-1]) for loc in self.telescopes ]
#
#         assert_equal(received, expected)
#
#
#     def test_expand_locations_no_class_match(self):
#         dict_repr = {
#                       'telescope_class' : '40m0',
#                       'site'            : None,
#                       'observatory'     : None,
#                       'telescope'       : None
#                     }
#
#         received = self.le.expand_locations(dict_repr)
#         expected = []
#
#         assert_equal(received, expected)
#
#
#     def test_expand_locations_no_site_match(self):
#         dict_repr = {
#                       'telescope_class' : '1m0',
#                       'site'            : 'ogg',
#                       'observatory'     : None,
#                       'telescope'       : None
#                     }
#
#         received = self.le.expand_locations(dict_repr)
#         expected = []
#
#         assert_equal(received, expected)
#
#
#     def test_expand_locations_class_only(self):
#         dict_repr = {
#                       'telescope_class' : '1m0',
#                       'site'            : None,
#                       'observatory'     : None,
#                       'telescope'       : None
#                     }
#
#         received = self.le.expand_locations(dict_repr)
#         expected = [
#                      '1m0a.doma.elp',
#                      '1m0a.doma.bpl',
#                    ]
#
#         assert_equal(received, expected)
#
#
#     def test_expand_locations_class_and_site(self):
#         dict_repr = {
#                       'telescope_class' : '1m0',
#                       'site'            : 'elp',
#                       'observatory'     : None,
#                       'telescope'       : None
#                     }
#
#         received = self.le.expand_locations(dict_repr)
#         expected = [
#                      '1m0a.doma.elp',
#                    ]
#
#         assert_equal(received, expected)
#
#
#     def test_expand_locations_class_and_site_and_obs(self):
#         dict_repr = {
#                       'telescope_class' : '0m4',
#                       'site'            : 'bpl',
#                       'observatory'     : 'aqwa',
#                       'telescope'       : None
#                     }
#
#         received = self.le.expand_locations(dict_repr)
#         expected = [
#                      '0m4a.aqwa.bpl',
#                      '0m4b.aqwa.bpl',
#                    ]
#
#         assert_equal(received, expected)
#
#
#     def test_expand_locations_class_and_site_and_obs_and_tel(self):
#         dict_repr = {
#                       'telescope_class' : '0m4',
#                       'site'            : 'bpl',
#                       'observatory'     : 'aqwa',
#                       'telescope'       : '0m4b'
#                     }
#
#         received = self.le.expand_locations(dict_repr)
#         expected = [
#                      '0m4b.aqwa.bpl',
#                    ]
#
#         assert_equal(received, expected)


class TestWindows(object):

    def setup(self):
        self.t1 = dict(
                             name = "Baltic"
                           )
        self.t2 = dict(
                             name = "Sea"
                           )


    def test_has_windows_windows(self):
        window_dict = {
                        'start' : "2013-03-01T00:00:00Z",
                        'end'   : "2013-03-01T00:30:00Z",
                      }
        w = Window(
                    window_dict = window_dict,
                    resource    = self.t1['name']
                  )
        windows = Windows()
        windows.append(w)

        assert_equal(windows.has_windows(), True)


    def test_has_windows_no_windows(self):
        windows = Windows()
        assert_equal(windows.has_windows(), False)


    def test_is_empty_has_windows_empty_on_one_resource(self):
        window_dict = {
                        'start' : "2013-03-01T00:00:00Z",
                        'end'   : "2013-03-01T00:30:00Z",
                      }
        w = Window(
                    window_dict = window_dict,
                    resource    = self.t1['name']
                  )
        w2 = Window(
                     window_dict = window_dict,
                     resource    = self.t2['name']
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
        initial_data = { 'scheme' : 'MPC_MINOR_PLANET' }

        target = NonSiderealTarget(initial_data)

        assert_in('meandist', target.required_fields)


    def test_comet_has_required_fields(self):
        initial_data = { 'scheme' : 'MPC_COMET' }

        target = NonSiderealTarget(initial_data)

        assert_in('perihdist', target.required_fields)


    def test_accepts_lowercase_scheme(self):
        initial_data = { 'scheme' : 'mpc_minor_planet' }

        target = NonSiderealTarget(initial_data)

        assert_in('meandist', target.required_fields)



class TestModelBuilder(object):

    def setup(self):
        self.target = {
                        'type' : 'SIDEREAL',
                      }
        self.molecules = [
                           {
                             'instrument_name' : '1m0-SciCam-Sinistro',
                             'type'            : 'expose',
                             'filter'          : 'B',
                           },
                         ]
        self.location = {
                          'telescope_class' : '1m0',
                        }
        self.windows = [
                         {
                           'start' : datetime(2013, 1, 1),
                           'end'   : datetime(2013, 1, 2),
                         },
                       ]
        self.constraints = {}
        self.request_number = '0000000002'
        self.state          = 'PENDING'


        self.mb = ModelBuilder(mock.MagicMock(), ConfigDBInterface(configdb_url='',
                                                                   active_instruments_file='test/active_instruments.json',
                                                                   telescopes_file='test/telescopes.json'))

    def test_build_request_sinistro_resolves_to_lsc_subnetwork(self):
        location = self.location.copy()
        location['site'] = 'lsc'
        req_dict = {
                     'target'         : self.target,
                     'molecules'      : self.molecules,
                     'location'       : location,
                     'windows'        : self.windows,
                     'constraints'    : self.constraints,
                     'id'             : self.request_number,
                     'duration'       : 10,
                     'state'          : self.state,
                   }

        request = self.mb.build_request(req_dict)
        assert_equal(set(['1m0a.doma.lsc', '1m0a.domb.lsc', '1m0a.domc.lsc']),
                     set(request.windows.windows_for_resource.keys()))


    def test_build_request_fl03_resolves_to_lsc_telescope(self):
        req_dict = {
                     'target'         : self.target,
                     'molecules' : [
                                     {
                                       'instrument_name' : 'fl03',
                                       'type'            : 'expose',
                                       'filter'          : 'B',
                                     },
                                   ],
                     'location'       : self.location,
                     'windows'        : self.windows,
                     'constraints'    : self.constraints,
                     'id'             : self.request_number,
                     'duration'       : 10,
                     'state'          : self.state,
                   }

        request = self.mb.build_request(req_dict)
        assert_equal(set(['1m0a.domb.lsc']),
                     set(request.windows.windows_for_resource.keys()))

    def test_build_request_slit_2as_resolves_to_coj_telescope(self):
        req_dict = {
                     'target'         : self.target,
                     'molecules' : [
                                     {
                                       'instrument_name' : '2m0-FLOYDS-SciCam',
                                       'type'            : 'arc',
                                       'spectra_slit'    : 'slit_2.0as',
                                     },
                                   ],
                     'location'       : {
                                          'telescope_class' : '2m0',
                                        },
                     'windows'        : self.windows,
                     'constraints'    : self.constraints,
                     'id'             : self.request_number,
                     'duration'       : 10,
                     'state'          : self.state,
                   }

        request = self.mb.build_request(req_dict)
        assert_equal(set(['2m0a.clma.coj', '2m0a.clma.ogg']),
                     set(request.windows.windows_for_resource.keys()))


    @raises(RequestError)
    def test_dont_accept_weird_target_types(self):
        req_dict = {
                     'target' : {
                                  'type' : 'POTATOES',
                                },
                     'molecules'      : self.molecules,
                     'location'       : self.location,
                     'windows'        : self.windows,
                     'constraints'    : self.constraints,
                     'id' : self.request_number,
                     'duration': 10,
                     'state'          : self.state,
                   }

        request = self.mb.build_request(req_dict)


    @raises(RequestError)
    def test_dont_accept_molecules_with_different_instruments(self):
        req_dict = {
                     'target'         : self.target,
                     'molecules' : [
                                     {
                                       'instrument_name' : 'SciCam',
                                       'type'            : 'expose',
                                     },
                                     {
                                       'instrument_name' : '1m0-SciCam-Sinistro',
                                       'type'            : 'expose',
                                     },
                                   ],
                     'location'       : self.location,
                     'windows'        : self.windows,
                     'constraints'    : self.constraints,
                     'id' : self.request_number,
                     'duration': 10,
                     'state'          : self.state,
                   }

        request = self.mb.build_request(req_dict)


    @raises(RequestError)
    def test_dont_accept_cameras_not_present_on_a_subnetwork(self):
        req_dict = {
                     'target'         : self.target,
                     'molecules' : [
                                     {
                                       'instrument_name' : 'POTATOES',
                                       'type'            : 'expose',
                                       'filter'          : 'B',
                                     },
                                   ],
                     'location'       : self.location,
                     'windows'        : self.windows,
                     'constraints'    : self.constraints,
                     'id' : self.request_number,
                     'duration': 10,
                     'state'          : self.state,
                   }

        request = self.mb.build_request(req_dict)

    @raises(RequestError)
    def test_dont_accept_filters_not_present_on_a_subnetwork(self):
        req_dict = {
                     'target'         : self.target,
                     'molecules' : [
                                     {
                                       'instrument_name' : '1m0-SciCam-Sinistro',
                                       'type'            : 'expose',
                                       'filter'          : 'fake',
                                     },
                                   ],
                     'location'       : self.location,
                     'windows'        : self.windows,
                     'constraints'    : self.constraints,
                     'id' : self.request_number,
                     'duration': 10,
                     'state'          : self.state,
                   }

        request = self.mb.build_request(req_dict)

    @mock.patch('adaptive_scheduler.model2.ModelBuilder.get_semester_details')
    @mock.patch('adaptive_scheduler.model2.ModelBuilder.get_proposal_details')
    def test_build_request_observation_type_normal(self, mock_proposal, mock_semester):
        mock_semester.return_value = {'id': '2013A', 'start': datetime(2013, 1, 1), 'end': datetime(2014, 1, 1)}
        mock_proposal.return_value = Proposal({'id': 'TestProposal', 'pi': '', 'tag': '', 'tac_priority': 10})
        req_dict = {
                     'target'         : self.target,
                     'molecules'      : self.molecules,
                     'location'       : self.location,
                     'windows'        : self.windows,
                     'constraints'    : self.constraints,
                     'id'             : self.request_number,
                     'duration'       : 10,
                     'state'          : self.state,
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

        user_request_model, invalid_requests = self.mb.build_user_request(cr_dict)
        assert_equal(user_request_model.observation_type, 'NORMAL')

    @mock.patch('adaptive_scheduler.model2.ModelBuilder.get_semester_details')
    @mock.patch('adaptive_scheduler.model2.ModelBuilder.get_proposal_details')
    def test_build_request_observation_type_target_of_opportunity(self, mock_proposal, mock_semester):
        mock_semester.return_value = {'id': '2013A', 'start': datetime(2013, 1, 1), 'end': datetime(2014, 1, 1)}
        mock_proposal.return_value = Proposal({'id': 'TestProposal', 'pi': '', 'tag': '', 'tac_priority': 10})
        req_dict = {
                     'target'         : self.target,
                     'molecules'      : self.molecules,
                     'location'       : self.location,
                     'windows'        : self.windows,
                     'constraints'    : self.constraints,
                     'id'             : self.request_number,
                     'duration'       : 10,
                     'state'          : self.state,
                   }

        cr_dict = {
            'proposal': 'TestProposal',
            'expires': '2014-10-29 12:12:12',
            'group_id': '',
            'id': '1',
            'ipp_value': '1.0',
            'operator': 'many',
            'requests': [req_dict, ],
            'observation_type': 'TARGET_OF_OPPORTUNITY',
        }

        user_request_model, invalid_requests = self.mb.build_user_request(cr_dict)
        assert_equal(user_request_model.observation_type, 'TARGET_OF_OPPORTUNITY')


    @raises(RequestError)
    @mock.patch('adaptive_scheduler.model2.ModelBuilder.get_semester_details')
    @mock.patch('adaptive_scheduler.model2.ModelBuilder.get_proposal_details')
    def test_dont_accept_unsupported_observation_type(self, mock_proposal, mock_semester):
        mock_semester.return_value = {'id': '2013A', 'start': datetime(2013, 1, 1), 'end': datetime(2014, 1, 1)}
        mock_proposal.return_value = Proposal({'id': 'TestProposal', 'pi': '', 'tag': '', 'tac_priority': 10})
        req_dict = {
                     'target'         : self.target,
                     'molecules'      : self.molecules,
                     'location'       : self.location,
                     'windows'        : self.windows,
                     'constraints'    : self.constraints,
                     'id'             : self.request_number,
                     'duration'       : 10.0,
                     'state'          : self.state,
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

        user_request_model, invalid_requests = self.mb.build_user_request(cr_dict)

    @mock.patch('adaptive_scheduler.model2.ModelBuilder.get_semester_details')
    @mock.patch('adaptive_scheduler.model2.ModelBuilder.get_proposal_details')
    def test_build_user_request_returns_invalid_user_requests(self, mock_proposal, mock_semester):
        mock_semester.return_value = {'id': '2013A', 'start': datetime(2013, 1, 1), 'end': datetime(2014, 1, 1)}
        mock_proposal.return_value = Proposal({'id': 'TestProposal', 'pi': '', 'tag': '', 'tac_priority': 10})
        bad_req_dict = {
                     'target' : {
                                  'type' : 'POTATOES',
                                },
                     'molecules'      : self.molecules,
                     'location'       : self.location,
                     'windows'        : self.windows,
                     'constraints'    : self.constraints,
                     'id'             : '2',
                     'duration'       : 10,
                     'state'          : self.state,
                   }
        
        good_req_dict = {
                     'target'         : self.target,
                     'molecules' : [
                                     {
                                       'instrument_name' : '1M0-SCICAM-SINISTRO',
                                       'type'            : 'expose',
                                       'filter'          : 'B',
                                     },
                                   ],
                     'location'       : self.location,
                     'windows'        : self.windows,
                     'constraints'    : self.constraints,
                     'id'             : '3',
                     'duration'       : 10,
                     'state'          : self.state,
                   }

        cr_dict = {
                   'proposal' : 'TestProposal',
                   'expires' : '2014-10-29 12:12:12',
                   'group_id' : '',
                   'id' : '1',
                   'ipp_value' : '1.0',
                   'operator' : 'many',
                   'observation_type': 'NORMAL',
                   'submitter': '',
                   'requests' : [bad_req_dict, good_req_dict]
                   }
        
        user_request_model, invalid_requests = self.mb.build_user_request(cr_dict)
        
        assert_equal(1, len(user_request_model.requests))
        assert_equal(1, len(invalid_requests))
        assert_equal(bad_req_dict, invalid_requests[0])
