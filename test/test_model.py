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
                                             Request, CompoundRequest, Windows,
                                             Window, Constraints,
                                             _LocationExpander )
from adaptive_scheduler.exceptions import InvalidRequestError


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
                                target         = None,
                                molecules      = [self.molecule1],
                                windows        = None,
                                constraints    = constraints,
                                request_number = None)
        self.request2  = Request(
                                target         = None,
                                molecules      = [self.molecule2],
                                windows        = None,
                                constraints    = constraints,
                                request_number = None)
        self.request3  = Request(
                                target         = None,
                                molecules      = [self.molecule3],
                                windows        = None,
                                constraints    = constraints,
                                request_number = None)
        self.request4  = Request(
                                target         = None,
                                molecules      = self.complex_mols,
                                windows        = None,
                                constraints    = constraints,
                                request_number = None)

    def test_get_simple_duration(self):
        assert_equal(self.request1.get_duration(), 141.0)

    def test_get_medium_duration(self):
        assert_equal(self.request2.get_duration(), 910.0)

    def test_long_exposure_seq(self):
        assert_equal(self.request3.get_duration(), 17355.0)

    def test_get_complex_duration(self):
        assert_equal(self.request4.get_duration(), 1535.0)


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

        self.semester_start = datetime(2011, 11, 1, 0, 0, 0)
        self.semester_end   = datetime(2011, 11, 8, 0, 0, 0)
        self.windows = [(self.semester_start, self.semester_end)]

        self.duration = 60
        self.request_number = '0000000001'



    @raises(InvalidRequestError)
    def test_invalid_request_type_raises_exception(self):
        junk_res_type = 'chocolate'
        constraints = Constraints({})
        request = Request(target         = self.target,
                          molecules      = [self.molecule],
                          windows        = self.windows,
                          constraints    = constraints,
                          request_number = self.request_number)
        compound_request = CompoundRequest(junk_res_type, [request])


    def test_valid_request_type_does_not_raise_exception(self):
        valid_res_type = 'and'
        constraints = Constraints({})
        request = Request(target         = self.target,
                          molecules      = [self.molecule],
                          windows        = self.windows,
                          constraints    = constraints,
                          request_number = self.request_number)
        compound_request = CompoundRequest(valid_res_type, [request])



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


