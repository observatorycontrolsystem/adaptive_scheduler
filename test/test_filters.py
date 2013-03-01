#!/usr/bin/env python

from __future__ import division

from nose.tools import assert_equal, assert_not_equal, raises
from mock       import patch, Mock
from datetime   import datetime, timedelta
from copy       import deepcopy

from adaptive_scheduler.model2          import ( UserRequest, Request, Window,
                                                 Windows, Telescope )
from adaptive_scheduler.request_filters import (
                                                 filter_on_expiry,
                                                 filter_out_past_windows,
                                                 filter_out_future_windows,
                                                 truncate_lower_crossing_windows,
                                                 truncate_upper_crossing_windows,
                                                 filter_on_duration,
                                                 filter_on_type,
                                                 run_all_filters
                                               )


def get_windows_from_request(request, resource_name):
    return request.windows.windows_for_resource[resource_name]

def fake_get_duration(self):
    return timedelta(hours=1)

class TestExpiryFilter(object):

    def setup(self):
        self.past_expiry     = datetime(2013, 1, 1)
        self.current_time    = datetime(2013, 2, 27)
        self.future_expiry1  = datetime(2013, 7, 27)
        self.future_expiry2  = datetime(2013, 7, 28)


    def create_user_request(self, expiry_dt):
        ur1 = UserRequest(
                           operator = 'single',
                           requests = None,
                           proposal = None,
                           expires  = expiry_dt,
                           tracking_number = None,
                           group_id = None
                         )
        return ur1


    def test_ur_equality(self):
        ur1 = self.create_user_request(self.future_expiry1)
        ur2 = self.create_user_request(self.future_expiry1)
        ur3 = self.create_user_request(self.future_expiry2)

        assert_equal(ur1, ur2)
        assert_not_equal(ur1, ur3)


    @patch("adaptive_scheduler.request_filters.datetime")
    def test_unexpired_request_not_filtered(self, mock_datetime):
        mock_datetime.utcnow.return_value = self.current_time

        ur_list = [
                    self.create_user_request(self.future_expiry1),
                    self.create_user_request(self.future_expiry2),
                  ]

        received_ur_list = filter_on_expiry(ur_list)
        assert_equal(received_ur_list, ur_list)


    @patch("adaptive_scheduler.request_filters.datetime")
    def test_expired_request_is_filtered(self, mock_datetime):
        mock_datetime.utcnow.return_value = self.current_time

        ur_list = [
                    self.create_user_request(self.past_expiry),
                    self.create_user_request(self.future_expiry1),
                  ]
        expected_ur_list = deepcopy(ur_list)
        del(expected_ur_list)[0]

        received_ur_list = filter_on_expiry(ur_list)
        assert_equal(received_ur_list, expected_ur_list)



class TestWindowFilters(object):

    def setup(self):
        self.current_time    = datetime(2013, 2, 27)
        self.semester_end    = datetime(2013, 10, 1)
        self.resource_name = "Martin"


    def create_user_request(self, window_dicts, operator='and'):
        t1 = Telescope(
                        name = self.resource_name
                      )

        req_list = []
        for req_windows in window_dicts:
            window_list = []
            windows = Windows()
            for window_dict in req_windows:
                w = Window(
                            window_dict = window_dict,
                            resource    = t1
                          )
                windows.append(w)
                window_list.append(w)

            r  = Request(
                          target         = None,
                          molecules      = None,
                          windows        = windows,
                          request_number = None
                        )
            req_list.append(r)

        if len(req_list) == 1:
            operator = 'single'

        ur1 = UserRequest(
                           operator        = operator,
                           requests        = req_list,
                           proposal        = None,
                           expires         = None,
                           tracking_number = None,
                           group_id        = None
                         )

        return ur1, window_list


    @patch("adaptive_scheduler.request_filters.datetime")
    def test_filters_out_only_past_windows(self, mock_datetime):
        mock_datetime.utcnow.return_value = self.current_time

        window_dict1 = {
                         'start' : "2013-01-01 00:00:00",
                         'end'   : "2013-01-01 01:00:00",
                       }
        # Comes after self.current_time, so should not be filtered
        window_dict2 = {
                         'start' : "2013-06-01 00:00:00",
                         'end'   : "2013-06-01 01:00:00",
                       }
        windows = [ (window_dict1, window_dict2) ]
        ur1, window_list = self.create_user_request(windows)

        received_ur_list = filter_out_past_windows([ur1])

        request = received_ur_list[0].requests[0]
        received_windows = get_windows_from_request(request, self.resource_name)

        expected_window = window_list[1]
        assert_equal(received_windows, [expected_window])


    @patch("adaptive_scheduler.model2.semester_service")
    def test_filters_out_only_future_windows(self, mock_semester_service):
        mock_semester_service.get_semester_end.return_value = self.semester_end

        # Comes after self.current_time, so should not be filtered
        window_dict1 = {
                         'start' : "2013-06-01 00:00:00",
                         'end'   : "2013-06-01 01:00:00",
                       }
        # Comes after semester_end, so should be filtered
        window_dict2 = {
                         'start' : "2013-12-01 00:00:00",
                         'end'   : "2013-12-01 01:00:00",
                       }

        windows = [ (window_dict1, window_dict2) ]
        ur1, window_list = self.create_user_request(windows)

        received_ur_list = filter_out_future_windows([ur1])

        request = received_ur_list[0].requests[0]
        received_windows = get_windows_from_request(request, self.resource_name)

        expected_window = window_list[0]
        assert_equal(received_windows, [expected_window])


    @patch("adaptive_scheduler.request_filters.datetime")
    def test_truncates_lower_crossing_windows(self, mock_datetime):
        mock_datetime.utcnow.return_value = self.current_time

        # Crosses self.current time, so should be truncated
        window_dict1 = {
                         'start' : "2013-01-01 00:00:00",
                         'end'   : "2013-03-01 01:00:00",
                       }
        windows = [ (window_dict1,) ]
        ur1, window_list = self.create_user_request(windows)

        received_ur_list = truncate_lower_crossing_windows([ur1])

        request = received_ur_list[0].requests[0]
        received_windows = get_windows_from_request(request, self.resource_name)

        assert_equal(received_windows[0].start, self.current_time)


    @patch("adaptive_scheduler.model2.semester_service")
    def test_truncates_upper_crossing_windows(self, mock_semester_service):
        mock_semester_service.get_semester_end.return_value = self.semester_end

        # Crosses semester end, so should be truncated
        window_dict1 = {
                         'start' : "2013-06-01 00:00:00",
                         'end'   : "2013-11-01 01:00:00",
                       }
        windows = [ (window_dict1,) ]
        ur1, window_list = self.create_user_request(windows)
        ur1.expires = datetime(2013, 12, 1)
        received_ur_list = truncate_upper_crossing_windows([ur1])

        request = received_ur_list[0].requests[0]
        received_windows = get_windows_from_request(request, self.resource_name)

        assert_equal(received_windows[0].end, self.semester_end)


    def test_filter_on_duration_window_larger(self):

        # Window is larger than one hour
        window_dict1 = {
                         'start' : "2013-09-01 00:00:00",
                         'end'   : "2013-11-01 01:00:00",
                       }
        windows = [ (window_dict1,) ]
        ur1, window_list = self.create_user_request(windows)

        UserRequest.duration = property(fake_get_duration)

        received_ur_list = filter_on_duration([ur1])

        request = received_ur_list[0].requests[0]
        received_windows = get_windows_from_request(request, self.resource_name)

        assert_equal(received_windows, window_list)


    def test_filter_on_duration_window_smaller(self):

        # Window is smaller than one hour
        window_dict1 = {
                         'start' : "2013-09-01 00:00:00",
                         'end'   : "2013-09-01 00:30:00",
                       }
        windows = [ (window_dict1,) ]
        ur1, window_list = self.create_user_request(windows)

        UserRequest.duration = property(fake_get_duration)

        received_ur_list = filter_on_duration([ur1])

        request = received_ur_list[0].requests[0]
        received_windows = get_windows_from_request(request, self.resource_name)

        assert_equal(received_windows, [])


    def test_filter_on_type_AND_both_requests_have_windows(self):
        window_dict1 = {
                         'start' : "2013-03-01 00:00:00",
                         'end'   : "2013-03-01 00:30:00",
                       }
        window_dict2 = {
                         'start' : "2013-03-01 00:00:00",
                         'end'   : "2013-03-01 00:30:00",
                       }
        windows = [ (window_dict1,), (window_dict2,) ]
        ur1, window_list = self.create_user_request(windows, operator='and')

        received_ur_list = filter_on_type([ur1])
        assert_equal(len(received_ur_list), 1)


    def test_filter_on_type_AND_neither_request_has_windows(self):
        windows = [ (), () ]
        ur1, window_list = self.create_user_request(windows, operator='and')

        received_ur_list = filter_on_type([ur1])
        assert_equal(len(received_ur_list), 0)


    def test_filter_on_type_AND_one_request_has_no_windows(self):
        window_dict1 = {
                         'start' : "2013-03-01 00:00:00",
                         'end'   : "2013-03-01 00:30:00",
                       }
        windows = [ (window_dict1,), () ]
        ur1, window_list = self.create_user_request(windows, operator='and')

        received_ur_list = filter_on_type([ur1])

        assert_equal(len(received_ur_list), 0)


    def test_filter_on_type_AND_two_windows_one_request_has_no_windows(self):
        window_dict1 = {
                         'start' : "2013-03-01 00:00:00",
                         'end'   : "2013-03-01 00:30:00",
                       }
        window_dict2 = {
                         'start' : "2013-03-01 00:00:00",
                         'end'   : "2013-03-01 00:30:00",
                       }
        windows = [ (window_dict1, window_dict2), () ]
        ur1, window_list = self.create_user_request(windows, operator='and')

        received_ur_list = filter_on_type([ur1])
        assert_equal(len(received_ur_list), 0)


    def test_filter_on_type_ONEOF_both_requests_have_windows(self):
        window_dict1 = {
                         'start' : "2013-03-01 00:00:00",
                         'end'   : "2013-03-01 00:30:00",
                       }
        window_dict2 = {
                         'start' : "2013-03-01 00:00:00",
                         'end'   : "2013-03-01 00:30:00",
                       }
        windows = [ (window_dict1,), (window_dict2,) ]
        ur1, window_list = self.create_user_request(windows, operator='oneof')

        received_ur_list = filter_on_type([ur1])
        assert_equal(len(received_ur_list), 1)


    def test_filter_on_type_ONEOF_one_request_has_no_windows(self):
        window_dict1 = {
                         'start' : "2013-03-01 00:00:00",
                         'end'   : "2013-03-01 00:30:00",
                       }
        windows = [ (window_dict1,), () ]
        ur1, window_list = self.create_user_request(windows, operator='oneof')

        received_ur_list = filter_on_type([ur1])
        assert_equal(len(received_ur_list), 1)


    def test_filter_on_type_ONEOF_two_windows_one_request_has_no_windows(self):
        window_dict1 = {
                         'start' : "2013-03-01 00:00:00",
                         'end'   : "2013-03-01 00:30:00",
                       }
        window_dict2 = {
                         'start' : "2013-03-01 00:00:00",
                         'end'   : "2013-03-01 00:30:00",
                       }
        windows = [ (window_dict1, window_dict2), () ]
        ur1, window_list = self.create_user_request(windows, operator='oneof')

        received_ur_list = filter_on_type([ur1])
        assert_equal(len(received_ur_list), 1)


    def test_filter_on_type_ONEOF_neither_request_has_windows(self):
        windows = [ (), () ]
        ur1, window_list = self.create_user_request(windows, operator='oneof')

        received_ur_list = filter_on_type([ur1])
        assert_equal(len(received_ur_list), 0)


    def test_filter_on_type_SINGLE_no_window_one_request(self):
        windows = [ () ]
        ur1, window_list = self.create_user_request(windows, operator='single')

        received_ur_list = filter_on_type([ur1])
        assert_equal(len(received_ur_list), 0)


    def test_filter_on_type_SINGLE_two_windows_one_request(self):
        window_dict1 = {
                         'start' : "2013-03-01 00:00:00",
                         'end'   : "2013-03-01 00:30:00",
                       }
        window_dict2 = {
                         'start' : "2013-03-01 00:00:00",
                         'end'   : "2013-03-01 00:30:00",
                       }
        windows = [ (window_dict1, window_dict2) ]
        ur1, window_list = self.create_user_request(windows, operator='single')

        received_ur_list = filter_on_type([ur1])
        assert_equal(len(received_ur_list), 1)


    @patch("adaptive_scheduler.request_filters.datetime")
    def test_run_all_filters(self, mock_datetime):
        mock_datetime.utcnow.return_value = self.current_time
        window_dict1 = {
                         'start' : "2013-03-01 00:00:00",
                         'end'   : "2013-03-01 01:30:00",
                       }
        windows = [ (window_dict1,) ]
        ur1, window_list = self.create_user_request(windows, operator='single')
        ur1.expires = datetime(2013, 12, 1)
        received_ur_list = run_all_filters([ur1])

        assert_equal(len(received_ur_list), 1)
