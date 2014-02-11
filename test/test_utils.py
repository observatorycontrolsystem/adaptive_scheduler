#!/usr/bin/python
from __future__ import division

from nose.tools import assert_equal
from datetime   import datetime, timedelta
import calendar

# Import the modules to test
from adaptive_scheduler.utils import (normalise, unnormalise, datetime_to_epoch,
                                      epoch_to_datetime, datetime_to_normalised_epoch,
                                      normalised_epoch_to_datetime, split_location)

class TestSplitLocation(object):
    def setup(self):
        pass

    def test_split_location_extracts_components(self):

        location = '0m4a.aqwb.coj'
        assert_equal(split_location(location), ('0m4a','aqwb','coj'))


    def test_split_location_duplicates_components_if_it_cant_split(self):

        location = 'Maui'
        assert_equal(split_location(location), ('Maui','Maui','Maui'))



class TestDateEpochConversions(object):
    '''Unit tests for converting between normalised epoch times (used by the
       scheduling kernel) and datetimes (used by the real world).'''

    def setup(self):
        pass


    def test_normalise(self):
        # 12th January, 2012, 2:55:47
        value = 1326336947

        # 1st January 2012, 00:00:00
        dt_start = datetime(2012, 1, 1, 0, 0, 0)
        start    = 1325376000

        # The normalised range starts at zero
        assert_equal(normalise(start, start), 0)

        # Normalising gives you the number of seconds since start
        seconds_since_start = normalise(value, start)

        dt_seconds_since_start = timedelta(seconds = seconds_since_start)

        dt_expected_value = dt_start + dt_seconds_since_start

        assert_equal(start + seconds_since_start,
                     calendar.timegm(dt_expected_value.timetuple()))



    def test_unnormalise_is_the_inverse_of_normalise(self):
        value = 1326336947
        start = 1325376000

        # normalise -> unnormalise is invariant
        normed          = normalise(value, start)
        unnormed_normed = unnormalise(normed, start)
        assert_equal(unnormed_normed, value)

        # unnormalise -> normalise is invariant
        unnormed        = unnormalise(value, start)
        normed_unnormed = normalise(unnormed, start)
        assert_equal(normed_unnormed, value)


    def test_datetime_epoch_conversions_are_inverses(self):
        # 12th January, 2012, 2:55:47
        dt_value = datetime(2012, 1, 12, 2, 55, 47)
        value    = 1326336947

        # 1st January 2012, 00:00:00
        dt_start = datetime(2012, 1, 1, 0, 0, 0)
        start    = 1325376000

        # Check the low level conversion routines
        assert_equal(datetime_to_epoch(dt_value), value)
        assert_equal(epoch_to_datetime(value), dt_value)

        # Check conversion plus normalisation/unnormalisation
        normed_epoch_value = datetime_to_normalised_epoch(dt_value, dt_start)
        assert_equal(normalised_epoch_to_datetime(normed_epoch_value, start), dt_value)
