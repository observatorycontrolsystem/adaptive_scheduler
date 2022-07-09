#!/usr/bin/python
from __future__ import division

from datetime import datetime, timedelta
import calendar

# Import the modules to test
from adaptive_scheduler.utils import (merge_dicts, normalise, unnormalise,
                                      datetime_to_epoch, epoch_to_datetime,
                                      datetime_to_normalised_epoch,
                                      normalised_epoch_to_datetime, split_location,
                                      estimate_runtime, safe_unidecode)


class TestUnidecode:
    def test_unidecode_limits_length(self):
        long_str = 'this string has more than fifty characters in it......'
        decoded_str = safe_unidecode(long_str, 50)

        assert len(long_str) > 50
        assert len(decoded_str) == 50
        assert decoded_str == long_str[0:50]

    def test_unidecode_limits_length_with_unicode(self):
        uni_str = u'This is gr\xe6t'
        decoded_str = safe_unidecode(uni_str, len(uni_str))

        assert len(decoded_str) == len(uni_str)
        assert decoded_str[-1] != str(uni_str[-1])

    def test_unidecode_shortens_mystery_characters(self):
        uni_str = u"these \u2214\u220e\u2219 don't translate"
        decoded_str = safe_unidecode(uni_str, 100)

        assert '?' in decoded_str
        assert '[?]' not in decoded_str


class TestMergeDicts(object):
    def setup(self):
        self.d1 = {
            'k1': 'v1',
            'k2': 'v2',
        }
        self.d2 = {
            'k3': 'v3',
            'k4': 'v4',
        }
        self.d3 = {
            'k5': 'v5',
            'k6': 'v6',
        }

    def test_merge_one_dict_gives_identical_dict(self):
        expected = self.d1.copy()
        received = merge_dicts(self.d1)

        assert received == expected

    def test_merge_two_dicts(self):
        expected = {
            'k1': 'v1',
            'k2': 'v2',
            'k3': 'v3',
            'k4': 'v4',
        }
        received = merge_dicts(self.d1, self.d2)

        assert received == expected

    def test_merge_three_dicts(self):
        expected = {
            'k1': 'v1',
            'k2': 'v2',
            'k3': 'v3',
            'k4': 'v4',
            'k5': 'v5',
            'k6': 'v6',
        }
        received = merge_dicts(self.d1, self.d2, self.d3)

        assert received == expected


class TestSplitLocation(object):
    def setup(self):
        pass

    def test_split_location_extracts_components(self):
        location = '0m4a.aqwb.coj'
        assert split_location(location) == ('0m4a', 'aqwb', 'coj')

    def test_split_location_duplicates_components_if_it_cant_split(self):
        location = 'Maui'
        assert split_location(location) == ('Maui', 'Maui', 'Maui')


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
        start = 1325376000

        # The normalised range starts at zero
        assert normalise(start, start) == 0

        # Normalising gives you the number of seconds since start
        seconds_since_start = normalise(value, start)

        dt_seconds_since_start = timedelta(seconds=seconds_since_start)

        dt_expected_value = dt_start + dt_seconds_since_start

        assert (start + seconds_since_start ==
                     calendar.timegm(dt_expected_value.timetuple()))

    def test_unnormalise_is_the_inverse_of_normalise(self):
        value = 1326336947
        start = 1325376000

        # normalise -> unnormalise is invariant
        normed = normalise(value, start)
        unnormed_normed = unnormalise(normed, start)
        assert unnormed_normed == value

        # unnormalise -> normalise is invariant
        unnormed = unnormalise(value, start)
        normed_unnormed = normalise(unnormed, start)
        assert normed_unnormed == value

    def test_datetime_epoch_conversions_are_inverses(self):
        # 12th January, 2012, 2:55:47
        dt_value = datetime(2012, 1, 12, 2, 55, 47)
        value = 1326336947

        # 1st January 2012, 00:00:00
        dt_start = datetime(2012, 1, 1, 0, 0, 0)
        start = 1325376000

        # Check the low level conversion routines
        assert datetime_to_epoch(dt_value) == value
        assert epoch_to_datetime(value) == dt_value

        # Check conversion plus normalisation/unnormalisation
        normed_epoch_value = datetime_to_normalised_epoch(dt_value, dt_start)
        assert normalised_epoch_to_datetime(normed_epoch_value, start) == dt_value


class TestRuntimeEstimate(object):

    def test_runtime_estimate_exceeded(self):
        estimate = timedelta(seconds=300)
        actual = timedelta(seconds=301)
        backoff_rate = 2.0
        pad_percent = 5.0
        new_estimate = estimate_runtime(estimate, actual, backoff_rate=backoff_rate, pad_percent=pad_percent)

        assert new_estimate == timedelta(seconds=backoff_rate * actual.total_seconds())

    def test_runtime_estimate_equals_actual(self):
        estimate = timedelta(seconds=300)
        actual = timedelta(seconds=300)
        backoff_rate = 2.0
        pad_percent = 5.0
        new_estimate = estimate_runtime(estimate, actual, backoff_rate=backoff_rate, pad_percent=pad_percent)

        assert new_estimate == timedelta(seconds=actual.total_seconds() * (1 + pad_percent / 100.0))

    def test_runtime_estimate_less_than_actual_has_pad(self):
        estimate = timedelta(seconds=300)
        actual = timedelta(seconds=299)
        backoff_rate = 2.0
        pad_percent = 5.0
        new_estimate = estimate_runtime(estimate, actual, backoff_rate=backoff_rate, pad_percent=pad_percent)

        assert new_estimate == timedelta(seconds=actual.total_seconds() * (1 + pad_percent / 100.0))

    def test_runtime_estimate_less_than_actual(self):
        estimate = timedelta(seconds=300)
        actual = timedelta(seconds=200)
        backoff_rate = 2.0
        pad_percent = 5.0
        new_estimate = estimate_runtime(estimate, actual, backoff_rate=backoff_rate, pad_percent=pad_percent)

        assert new_estimate == timedelta(
            seconds=estimate.total_seconds() - (estimate.total_seconds() - actual.total_seconds()) / backoff_rate)
