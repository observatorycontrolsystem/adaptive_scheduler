#!/usr/bin/env python

'''
test_moving_object_utils.py - summary line

description

Author: Eric Saunders
December 2013
'''

from adaptive_scheduler.moving_object_utils import (pond_pointing_from_scheme,
                                                    InvalidElements)
from adaptive_scheduler.model              import NonSiderealTarget

from nose.tools import assert_equal, raises

class TestMovingObjectUtils(object):

    def setup(self):
        self.elements = {
                          'name'         : 'Kilia',
                          'epochofel'    : 56600.0,
                          'orbinc'       : 7.22565,
                          'longascnode'  : 173.25052,
                          'argofperih'   : 47.60658,
                          'meandist'     : 2.4050925,
                          'eccentricity' : 0.0943494,
                          'meananom'     : 54.47380,
                          'scheme'       : 'MPC_MINOR_PLANET',
                        }

    def test_minor_planet_pond_pointing_has_expected_field(self):

        target = NonSiderealTarget(self.elements)

        pointing = pond_pointing_from_scheme(target)

        assert_equal(pointing.meandist, self.elements['meandist'])
        assert_equal(pointing.perihdist, 0.0)


    @raises(InvalidElements)
    def test_invalid_scheme_raises_exception(self):
        self.elements['scheme'] = 'nonsense'
        target = NonSiderealTarget(self.elements)

        pointing = pond_pointing_from_scheme(target)


