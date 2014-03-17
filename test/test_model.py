#!/usr/bin/python
from __future__ import division

import mock
from nose.tools import assert_equal, assert_in, raises, nottest
from datetime import datetime

# Import the modules to test
from adaptive_scheduler.model2      import ( build_telescope_network,
                                             SiderealTarget, NonSiderealTarget,
                                             Telescope,
                                             Proposal, Molecule,
                                             Request, CompoundRequest, UserRequest,
                                             Windows, Window, Constraints,
                                             Spectrograph,
                                             _LocationExpander, ModelBuilder,
                                             RequestError)

class TestTelescopeNetwork(object):

    def setup(self):
        self.tel_name1 = '2m0a.clma.ogg'
        self.tel_name2 = '1m0a.doma.ogg'
        self.tel_data = [
                            {
                                'name'      : self.tel_name1,
                                'tel_class' : '2m0',
                            },
                            {
                                'name'      : self.tel_name2,
                                'tel_class' : '1m0',
                            },
                        ]

        self.tel_network = build_telescope_network(tel_dicts=self.tel_data)


    def test_get_telescope(self):
        assert_equal(self.tel_network.get_telescope(self.tel_name1),
                     Telescope(self.tel_data[0]))


    def test_get_telescopes_at_location(self):
        dict_repr = {
                      'telescope_class' : '1m0a',
                      'site'            : 'ogg',
                      'observatory'     : None,
                      'telescope'       : None
                    }

        received = self.tel_network.get_telescopes_at_location(dict_repr)
        expected = [ Telescope(self.tel_data[1]) ]
        assert_equal(received, expected)




class TestDuration(object):
    '''Unit tests for duration of URs, CRs, and Rs.'''

    def setup(self):
        self.molecule1 = Molecule(
                                  type            = 'expose_n',
                                  exposure_count  = 1,
                                  bin_x           = 2,
                                  bin_y           = 2,
                                  instrument_name = 'KB12',
                                  filter          = 'BSSL-UX-020',
                                  exposure_time   = 20,
                                  priority        = 1
                                )
        self.molecule2 = Molecule(
                                  type            = 'expose_n',
                                  exposure_count  = 10,
                                  bin_x           = 1,
                                  bin_y           = 1,
                                  instrument_name = 'KB12',
                                  filter          = 'BSSL-UX-020',
                                  exposure_time   = 20,
                                  priority        = 1
                                )
        self.molecule3 = Molecule(
                                  type            = 'expose_n',
                                  exposure_count  = 300,
                                  bin_x           = 2,
                                  bin_y           = 2,
                                  instrument_name = 'KB12',
                                  filter          = 'BSSL-UX-020',
                                  exposure_time   = 42,
                                  priority        = 1
                                )
        self.complex_mols = [
                              Molecule(
                                exposure_time   = 180.0,
                                exposure_count  = 3,
                                filter          = 'B',
                                bin_x           = 2,
                                bin_y           = 2,
                              ),
                              Molecule(
                                exposure_time  = 120.0,
                                exposure_count = 3,
                                filter         = 'V',
                                bin_x          = 2,
                                bin_y          = 2,
                              ),
                              Molecule(
                                exposure_time  = 120.0,
                                exposure_count = 3,
                                filter         = 'R',
                                bin_x          = 2,
                                bin_y          = 2,
                              ),
                           ]
        constraints = Constraints({})
        self.request1  = Request(
                                  target          = None,
                                  molecules       = [self.molecule1],
                                  windows         = None,
                                  constraints     = constraints,
                                  request_number  = None,
                                  instrument_type = '1m0-SCICAM-SBIG',
                                  observation_type = 'NORMAL',
                                )
        self.request2  = Request(
                                  target          = None,
                                  molecules       = [self.molecule2],
                                  windows         = None,
                                  constraints     = constraints,
                                  request_number  = None,
                                  instrument_type = '1m0-SCICAM-SBIG',
                                  observation_type = 'NORMAL',
                                )
        self.request3  = Request(
                                  target          = None,
                                  molecules       = [self.molecule3],
                                  windows         = None,
                                  constraints     = constraints,
                                  request_number  = None,
                                  instrument_type = '1m0-SCICAM-SBIG',
                                  observation_type = 'NORMAL',
                                )
        self.request4  = Request(
                                  target          = None,
                                  molecules       = self.complex_mols,
                                  windows         = None,
                                  constraints     = constraints,
                                  request_number  = None,
                                  instrument_type = '1m0-SCICAM-SBIG',
                                  observation_type = 'NORMAL',
                                )

    def test_get_simple_duration(self):
        assert_equal(self.request1.get_duration(), 141.0)

    def test_get_medium_duration(self):
        assert_equal(self.request2.get_duration(), 910.0)

    def test_long_exposure_seq(self):
        assert_equal(self.request3.get_duration(), 17355.0)

    def test_get_complex_duration(self):
        assert_equal(self.request4.get_duration(), 1535.0)



class TestSpectrographDuration(object):
    '''Unit tests for duration of URs, CRs, and Rs.'''

    def setup(self):
        self.lamp_molecule = Molecule(
                                      type            = 'lamp_flat',
                                      exposure_count  = 1,
                                      bin_x           = 1,
                                      bin_y           = 1,
                                      instrument_name = 'floyds',
                                      filter          = '',
                                      exposure_time   = 60,
                                      priority        = 1
                                    )
        self.arc_molecule = Molecule(
                                      type            = 'arc',
                                      exposure_count  = 1,
                                      bin_x           = 1,
                                      bin_y           = 1,
                                      instrument_name = 'floyds',
                                      filter          = '',
                                      exposure_time   = 30,
                                      priority        = 2
                                    )
        self.arc_exp2_molecule = Molecule(
                                      type            = 'arc',
                                      exposure_count  = 2,
                                      bin_x           = 1,
                                      bin_y           = 1,
                                      instrument_name = 'floyds',
                                      filter          = '',
                                      exposure_time   = 30,
                                      priority        = 2
                                    )
        self.arc_exp2_bin2_molecule = Molecule(
                                      type            = 'arc',
                                      exposure_count  = 2,
                                      bin_x           = 2,
                                      bin_y           = 2,
                                      instrument_name = 'floyds',
                                      filter          = '',
                                      exposure_time   = 30,
                                      priority        = 2
                                    )
        self.spectrum_molecule = Molecule(
                                          type            = 'spectrum',
                                          exposure_count  = 1,
                                          bin_x           = 1,
                                          bin_y           = 1,
                                          instrument_name = 'floyds',
                                          filter          = '',
                                          spectra_slit    = 'slit_6as',
                                          exposure_time   = 1800,
                                          priority        = 3
                                        )
        self.target_acq_on = SiderealTarget(
                                              name  = 'deneb',
                                              ra    = 310.35795833333333,
                                              dec   = 45.280338888888885,
                                              epoch = 2000,
                                              acquire_mode = 'ON',
                                            )
        self.target_acq_off = SiderealTarget(
                                              name  = 'deneb',
                                              ra    = 310.35795833333333,
                                              dec   = 45.280338888888885,
                                              epoch = 2000,
                                              acquire_mode = 'OFF',
                                            )
        self.target_acq_maybe = SiderealTarget(
                                                 name  = 'deneb',
                                                 ra    = 310.35795833333333,
                                                 dec   = 45.280338888888885,
                                                 epoch = 2000,
                                                 acquire_mode = 'MAYBE',
                                               )
        self.spectrograph = Spectrograph()

    def test_acquire_is_off(self):
        mols = [self.spectrum_molecule]
        received = self.spectrograph.get_duration(mols, self.target_acq_off)
        assert_equal(received, 1976)

    def test_acquire_is_maybe(self):
        mols = [self.spectrum_molecule]
        received = self.spectrograph.get_duration(mols, self.target_acq_maybe)
        assert_equal(received, 2066)

    def test_acquire_is_on(self):
        mols = [self.spectrum_molecule]
        received = self.spectrograph.get_duration(mols, self.target_acq_on)
        assert_equal(received, 2066)

    def test_lamp_flat_arc_target_sequence(self):
        mols = [self.lamp_molecule, self.arc_molecule, self.spectrum_molecule]
        received = self.spectrograph.get_duration(mols, self.target_acq_on)
        assert_equal(received, 2267)

    def test_lamp_flat_arc_sequence(self):
        mols = [self.lamp_molecule, self.arc_molecule]
        received = self.spectrograph.get_duration(mols, self.target_acq_on)
        assert_equal(received, 321)

    def test_double_lamp_arc_four_changes(self):
        mols = [self.lamp_molecule, self.arc_molecule,
                self.lamp_molecule, self.arc_molecule]
        received = self.spectrograph.get_duration(mols, self.target_acq_on)
        assert_equal(received, 522)

    def test_double_lamp_arc_three_changes(self):
        mols = [self.lamp_molecule, self.arc_molecule,
                self.arc_molecule, self.lamp_molecule]
        received = self.spectrograph.get_duration(mols, self.target_acq_on)
        assert_equal(received, 492)

    def test_arc_two_exposures(self):
        mols = [self.arc_exp2_molecule]
        received = self.spectrograph.get_duration(mols, self.target_acq_off)
        assert_equal(received, 261)

    def test_arc_two_exposures_bin_two_no_acquisition(self):
        mols = [self.arc_exp2_bin2_molecule]
        received = self.spectrograph.get_duration(mols, self.target_acq_off)
        assert_equal(received, 224)

    def test_arc_two_exposures_bin_two_with_acquisition(self):
        mols = [self.arc_exp2_bin2_molecule]
        received = self.spectrograph.get_duration(mols, self.target_acq_on)
        assert_equal(received, 224)


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

        self.telescope = Telescope(
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

        self.molecule = Molecule(
                                  type            = 'expose_n',
                                  exposure_count  = 1,
                                  bin_x           = 2,
                                  bin_y           = 2,
                                  instrument_name = 'KB12',
                                  filter          = 'BSSL-UX-020',
                                  exposure_time   = 20,
                                  priority        = 1
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
                          observation_type = 'NORMAL')
        compound_request = CompoundRequest(junk_res_type, [request])


    def test_valid_request_type_does_not_raise_exception(self):
        valid_res_type = 'and'
        request = Request(target         = self.target,
                          molecules      = [self.molecule],
                          windows        = self.windows,
                          constraints    = self.constraints,
                          request_number = self.request_number,
                          observation_type = 'NORMAL')
        compound_request = CompoundRequest(valid_res_type, [request])


    def test_null_instrument_has_a_name(self):
        request = Request(target         = self.target,
                          molecules      = [self.molecule],
                          windows        = self.windows,
                          constraints    = self.constraints,
                          request_number = self.request_number,
                          observation_type = 'NORMAL')
        assert_equal(request.get_instrument_type(), 'NULL-INSTRUMENT')

    def test_configured_instrument_has_a_name(self):
        request = Request(target          = self.target,
                          molecules       = [self.molecule],
                          windows         = self.windows,
                          constraints     = self.constraints,
                          request_number  = self.request_number,
                          instrument_type = '1M0-SCICAM-SINISTRO',
                          observation_type = 'NORMAL')

        assert_equal(request.get_instrument_type(), '1M0-SCICAM-SINISTRO')


class TestCompoundRequest(object):
    '''Unit tests for the adaptive scheduler CompoundRequest object.'''

    def setup(self):
        pass


    def test_drop_empty_children(self):
        r_mock1 = mock.MagicMock()
        r_mock1.has_windows.return_value = True

        r_mock2 = mock.MagicMock()
        r_mock2.has_windows.return_value = False

        cr = CompoundRequest(
                              operator='many',
                              requests=[r_mock1, r_mock2]
                            )

        cr.drop_empty_children()

        assert_equal(len(cr.requests), 1)
        assert_equal(cr.requests[0], r_mock1)


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
                          group_id = None
                         )

        msg = 'Yo dude'
        tag = 'MeTag'
        timestamp = datetime(2013, 10, 15, 1, 1, 1)
        ur.emit_user_feedback(msg, tag, timestamp)

        assert_equal(mock_func.called, True)


class TestLocationExpander(object):

    def setup(self):
        self.telescopes = (
                            '0m4a.aqwa.bpl.0m4',
                            '0m4b.aqwa.bpl.0m4',
                            '1m0a.doma.elp.1m0',
                            '1m0a.doma.bpl.1m0',
                            '2m0a.clma.ogg.2m0',
                            '2m0a.clma.coj.2m0',
                          )

        self.le = _LocationExpander(self.telescopes)


    def test_expand_locations_no_filtering_if_empty_dict(self):
        dict_repr = {
                      'telescope_class' : None,
                      'site'            : None,
                      'observatory'     : None,
                      'telescope'       : None
                    }

        received = self.le.expand_locations(dict_repr)

        # We expect the full telescope list, with the telescope classes trimmed off
        expected = [ '.'.join(loc.split('.')[:-1]) for loc in self.telescopes ]

        assert_equal(received, expected)


    def test_expand_locations_no_class_match(self):
        dict_repr = {
                      'telescope_class' : '40m0',
                      'site'            : None,
                      'observatory'     : None,
                      'telescope'       : None
                    }

        received = self.le.expand_locations(dict_repr)
        expected = []

        assert_equal(received, expected)


    def test_expand_locations_no_site_match(self):
        dict_repr = {
                      'telescope_class' : '1m0',
                      'site'            : 'ogg',
                      'observatory'     : None,
                      'telescope'       : None
                    }

        received = self.le.expand_locations(dict_repr)
        expected = []

        assert_equal(received, expected)


    def test_expand_locations_class_only(self):
        dict_repr = {
                      'telescope_class' : '1m0',
                      'site'            : None,
                      'observatory'     : None,
                      'telescope'       : None
                    }

        received = self.le.expand_locations(dict_repr)
        expected = [
                     '1m0a.doma.elp',
                     '1m0a.doma.bpl',
                   ]

        assert_equal(received, expected)


    def test_expand_locations_class_and_site(self):
        dict_repr = {
                      'telescope_class' : '1m0',
                      'site'            : 'elp',
                      'observatory'     : None,
                      'telescope'       : None
                    }

        received = self.le.expand_locations(dict_repr)
        expected = [
                     '1m0a.doma.elp',
                   ]

        assert_equal(received, expected)


    def test_expand_locations_class_and_site_and_obs(self):
        dict_repr = {
                      'telescope_class' : '0m4',
                      'site'            : 'bpl',
                      'observatory'     : 'aqwa',
                      'telescope'       : None
                    }

        received = self.le.expand_locations(dict_repr)
        expected = [
                     '0m4a.aqwa.bpl',
                     '0m4b.aqwa.bpl',
                   ]

        assert_equal(received, expected)


    def test_expand_locations_class_and_site_and_obs_and_tel(self):
        dict_repr = {
                      'telescope_class' : '0m4',
                      'site'            : 'bpl',
                      'observatory'     : 'aqwa',
                      'telescope'       : '0m4b'
                    }

        received = self.le.expand_locations(dict_repr)
        expected = [
                     '0m4b.aqwa.bpl',
                   ]

        assert_equal(received, expected)


class TestWindows(object):

    def setup(self):
        self.t1 = Telescope(
                             name = "Baltic"
                           )
        self.t2 = Telescope(
                             name = "Sea"
                           )


    def test_has_windows_windows(self):
        window_dict = {
                        'start' : "2013-03-01 00:00:00",
                        'end'   : "2013-03-01 00:30:00",
                      }
        w = Window(
                    window_dict = window_dict,
                    resource    = self.t1
                  )
        windows = Windows()
        windows.append(w)

        assert_equal(windows.has_windows(), True)


    def test_has_windows_no_windows(self):
        windows = Windows()
        assert_equal(windows.has_windows(), False)


    def test_is_empty_has_windows_empty_on_one_resource(self):
        window_dict = {
                        'start' : "2013-03-01 00:00:00",
                        'end'   : "2013-03-01 00:30:00",
                      }
        w = Window(
                    window_dict = window_dict,
                    resource    = self.t1
                  )
        w2 = Window(
                     window_dict = window_dict,
                     resource    = self.t2
                   )

        windows = Windows()
        windows.append(w)
        windows.append(w2)
        windows.windows_for_resource[self.t2.name] = []

        assert_equal(windows.has_windows(), True)
        assert_equal(windows.size(), 1)



class TestTelescope(object):

    def __init__(self):
        pass


    def test_telescope_has_empty_events_list(self):
        telescope = Telescope()

        assert_equal(telescope.events, [])



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
                           },
                         ]
        self.location = {
                          'telescope_class' : '1m0',
                          'site'            : None,
                          'observatory'     : None,
                          'telescope'       : None,
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

        self.mb = ModelBuilder('telescopes.dat', 'camera_mappings.dat')

    def test_build_request_sinistro_resolves_to_lsc_subnetwork(self):
        req_dict = {
                     'target'         : self.target,
                     'molecules'      : self.molecules,
                     'location'       : self.location,
                     'windows'        : self.windows,
                     'constraints'    : self.constraints,
                     'request_number' : self.request_number,
                     'state'          : self.state,
                     'observation_type' : 'NORMAL',
                   }

        request = self.mb.build_request(req_dict)
        assert_equal(request.instrument.type, '1M0-SCICAM-SINISTRO')
        assert_equal(set(['1m0a.doma.lsc', '1m0a.domb.lsc', '1m0a.domc.lsc']),
                     set(request.windows.windows_for_resource.keys()))


    def test_build_request_scicam_maps_to_sbig(self):
        req_dict = {
                     'target'         : self.target,
                     'molecules' : [
                                     {
                                       'instrument_name' : 'SciCam',
                                     },
                                   ],
                     'location'       : self.location,
                     'windows'        : self.windows,
                     'constraints'    : self.constraints,
                     'request_number' : self.request_number,
                     'state'          : self.state,
                     'observation_type' : 'NORMAL',
                   }

        request = self.mb.build_request(req_dict)
        assert_equal(request.instrument.type, '1M0-SCICAM-SBIG')
        assert_equal(set(['1m0a.doma.coj', '1m0a.domb.coj',
                          '1m0a.doma.cpt', '1m0a.domb.cpt', '1m0a.domc.cpt',
                          '1m0a.doma.elp'
                         ]),
                     set(request.windows.windows_for_resource.keys()))


    def test_build_request_fl03_resolves_to_lsc_telescope(self):
        req_dict = {
                     'target'         : self.target,
                     'molecules' : [
                                     {
                                       'instrument_name' : 'fl03',
                                     },
                                   ],
                     'location'       : self.location,
                     'windows'        : self.windows,
                     'constraints'    : self.constraints,
                     'request_number' : self.request_number,
                     'state'          : self.state,
                     'observation_type' : 'NORMAL',
                   }

        request = self.mb.build_request(req_dict)
        assert_equal(request.instrument.type, '1M0-SCICAM-SINISTRO')
        assert_equal(set(['1m0a.domb.lsc']),
                     set(request.windows.windows_for_resource.keys()))

    def test_build_request_observation_type_normal(self):
        req_dict = {
                     'target'         : self.target,
                     'molecules'      : self.molecules,
                     'location'       : self.location,
                     'windows'        : self.windows,
                     'constraints'    : self.constraints,
                     'request_number' : self.request_number,
                     'state'          : self.state,
                     'observation_type' : 'NORMAL',
                   }

        request = self.mb.build_request(req_dict)
        assert_equal(request.observation_type, 'NORMAL')

    def test_build_request_observation_type_target_of_opportunity(self):
        req_dict = {
                     'target'         : self.target,
                     'molecules'      : self.molecules,
                     'location'       : self.location,
                     'windows'        : self.windows,
                     'constraints'    : self.constraints,
                     'request_number' : self.request_number,
                     'state'          : self.state,
                     'observation_type' : 'TARGET_OF_OPPORTUNITY',
                   }

        request = self.mb.build_request(req_dict)
        assert_equal(request.observation_type, 'TARGET_OF_OPPORTUNITY')


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
                     'request_number' : self.request_number,
                     'state'          : self.state,
                     'observation_type' : 'NORMAL',
                   }

        request = self.mb.build_request(req_dict)


    @raises(RequestError)
    def test_dont_accept_molecules_with_different_instruments(self):
        req_dict = {
                     'target'         : self.target,
                     'molecules' : [
                                     {
                                       'instrument_name' : 'SciCam',
                                     },
                                     {
                                       'instrument_name' : '1m0-SciCam-Sinistro',
                                     },
                                   ],
                     'location'       : self.location,
                     'windows'        : self.windows,
                     'constraints'    : self.constraints,
                     'request_number' : self.request_number,
                     'state'          : self.state,
                     'observation_type' : 'NORMAL',
                   }

        request = self.mb.build_request(req_dict)


    @raises(RequestError)
    def test_dont_accept_cameras_not_present_on_a_subnetwork(self):
        req_dict = {
                     'target'         : self.target,
                     'molecules' : [
                                     {
                                       'instrument_name' : 'POTATOES',
                                     },
                                   ],
                     'location'       : self.location,
                     'windows'        : self.windows,
                     'constraints'    : self.constraints,
                     'request_number' : self.request_number,
                     'state'          : self.state,
                     'observation_type' : 'NORMAL',
                   }

        request = self.mb.build_request(req_dict)

    @raises(RequestError)
    def test_dont_accept_unsupported_observation_type(self):
        req_dict = {
                     'target'         : self.target,
                     'molecules'      : self.molecules,
                     'location'       : self.location,
                     'windows'        : self.windows,
                     'constraints'    : self.constraints,
                     'request_number' : self.request_number,
                     'state'          : self.state,
                     'observation_type' : 'ABNORMAL',
                   }

        request = self.mb.build_request(req_dict)
