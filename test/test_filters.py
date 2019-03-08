#!/usr/bin/env python

from __future__ import division

from nose.tools import assert_equal, assert_not_equal, raises
from mock       import patch, Mock
from datetime   import datetime, timedelta
from copy       import deepcopy

from adaptive_scheduler.model2          import (RequestGroup, Request, Windows)
import helpers

import adaptive_scheduler.request_filters
from adaptive_scheduler.request_filters import (
                                                 filter_out_windows_for_running_requests,
                                                 # filter_on_expiry,
                                                 filter_out_past_windows,
                                                 filter_out_future_windows,
                                                 truncate_lower_crossing_windows,
                                                 truncate_upper_crossing_windows,
                                                 filter_on_duration,
                                                 filter_on_type,
                                                 drop_empty_requests,
                                                 filter_on_pending,
                                                 run_all_filters,
                                               )


def get_windows_from_request(request, resource_name):
    return request.windows.windows_for_resource[resource_name]


class TestExpiryFilter(object):

    def setup(self):
        self.past_expiry     = datetime(2013, 1, 1)
        self.future_expiry1  = datetime(2013, 7, 27)
        self.future_expiry1  = datetime.utcnow() + timedelta(weeks=12)
        self.future_expiry2  = datetime.utcnow() + timedelta(weeks=13)


    def create_request_group(self, expiry_dt):
        rg = RequestGroup(
                           operator = 'single',
                           requests = None,
                           proposal = None,
                           expires  = expiry_dt,
                           id='0000000005',
                           group_id = None,
                           ipp_value=1.0,
                           observation_type='NORMAL',
                           submitter=''
                         )
        return rg


    def test_rg_equality(self):
        rg1 = self.create_request_group(self.future_expiry1)
        rg2 = self.create_request_group(self.future_expiry1)
        rg3 = self.create_request_group(self.future_expiry2)

        assert_equal(rg1, rg2)
        assert_not_equal(rg1, rg3)


    # def test_unexpired_request_not_filtered(self):
    #
    #     rg_list = [
    #                 self.create_user_request(self.future_expiry1),
    #                 self.create_user_request(self.future_expiry2),
    #               ]
    #
    #     received_rg_list = filter_on_expiry(rg_list)
    #     assert_equal(received_rg_list, rg_list)


    # def test_expired_request_is_filtered(self):
    #     rg_list = [
    #                 self.create_user_request(self.past_expiry),
    #                 self.create_user_request(self.future_expiry1),
    #               ]
    #     expected_rg_list = deepcopy(rg_list)
    #     del(expected_rg_list)[0]
    #
    #     received_rg_list = filter_on_expiry(rg_list)
    #     assert_equal(received_rg_list, expected_rg_list)



class TestWindowFilters(object):

    def setup(self):
        self.current_time    = datetime(2013, 2, 27)
        self.semester_end    = datetime(2013, 10, 1)
        self.resource_name = "Martin"
        adaptive_scheduler.request_filters.now = self.current_time

    def create_request_group(self, windows, operator='and', expires=None):
        return helpers.create_request_group(windows, operator, expires=expires)

    def test_filters_out_only_past_windows(self):

        window_dict1 = {
                         'start' : "2013-01-01T00:00:00Z",
                         'end'   : "2013-01-01T01:00:00Z",
                       }
        # Comes after self.current_time, so should not be filtered
        window_dict2 = {
                         'start' : "2013-06-01T00:00:00Z",
                         'end'   : "2013-06-01T01:00:00Z",
                       }
        windows = [ (window_dict1, window_dict2) ]
        rg1, window_list = self.create_request_group(windows)

        received_rg_list = filter_out_past_windows([rg1])

        request = received_rg_list[0].requests[0]
        received_windows = get_windows_from_request(request, self.resource_name)

        expected_window = [window_list[1]]
        assert_equal(received_windows, expected_window)


    def test_filters_out_only_past_windows_straddling_boundary(self):

        window_dict1 = {
                         'start' : "2013-02-26T11:30:00Z",
                         'end'   : "2013-02-27T00:30:00Z",
                       }
        # Comes after self.current_time, so should not be filtered
        window_dict2 = {
                         'start' : "2013-06-01T00:00:00Z",
                         'end'   : "2013-06-01T01:00:00Z",
                       }
        windows = [ (window_dict1, window_dict2) ]
        rg1, window_list = self.create_request_group(windows)

        received_rg_list = filter_out_past_windows([rg1])

        request = received_rg_list[0].requests[0]
        received_windows = get_windows_from_request(request, self.resource_name)

        expected_window = [window_list[0], window_list[1]]
        assert_equal(received_windows, expected_window)


    @patch("adaptive_scheduler.valhalla_connections.ObservationPortalInterface.get_semester_details")
    def test_filters_out_only_future_windows(self, mock_semester_service):
        mock_semester_service.return_value = {'id': '2015A', 'start': self.semester_end - timedelta(days=300),
                                              'end': self.semester_end}

        # Comes after self.current_time, so should not be filtered
        window_dict1 = {
                         'start' : "2013-06-01T00:00:00Z",
                         'end'   : "2013-06-01T01:00:00Z",
                       }
        # Comes after semester_end, so should be filtered
        window_dict2 = {
                         'start' : "2013-12-01T00:00:00Z",
                         'end'   : "2013-12-01T01:00:00Z",
                       }


        windows = [ (window_dict1, window_dict2) ]
        expire_time = datetime(2000, 1, 1)
        for window in windows[0]:
            end_time = datetime.strptime(window['end'], '%Y-%m-%dT%H:%M:%SZ')
            if  end_time > expire_time:
                expire_time = end_time
        expire_time = min(expire_time, self.semester_end)

        rg1, window_list = self.create_request_group(windows, expires=expire_time)

        received_rg_list = filter_out_future_windows([rg1])

        request = received_rg_list[0].requests[0]
        received_windows = get_windows_from_request(request, self.resource_name)

        expected_window = window_list[0]
        assert_equal(received_windows, [expected_window])


    @patch("adaptive_scheduler.valhalla_connections.ObservationPortalInterface.get_semester_details")
    def test_filters_out_only_future_windows2(self, mock_semester_service):
        mock_semester_service.return_value = {'id': '2015A', 'start': self.semester_end - timedelta(days=30),
                                              'end': self.semester_end}

        horizon = datetime(2013, 7, 1)
        # Comes after self.current_time, so should not be filtered
        window_dict1 = {
                         'start' : "2013-06-01T00:00:00Z",
                         'end'   : "2013-06-01T01:00:00Z",
                       }
        # Comes after effective horizon, so should be filtered
        window_dict2 = {
                         'start' : "2013-08-01T00:00:00Z",
                         'end'   : "2013-09-01T01:00:00Z",
                       }

        windows = [ (window_dict1, window_dict2) ]
        rg1, window_list = self.create_request_group(windows)

        received_rg_list = filter_out_future_windows([rg1], horizon=horizon)

        request = received_rg_list[0].requests[0]
        received_windows = get_windows_from_request(request, self.resource_name)

        expected_window = window_list[0]
        assert_equal(received_windows, [expected_window])


    def test_truncates_lower_crossing_windows(self):

        # Crosses self.current time, so should be truncated
        window_dict1 = {
                         'start' : "2013-01-01T00:00:00Z",
                         'end'   : "2013-03-01T01:00:00Z",
                       }
        windows = [ (window_dict1,) ]
        rg1, window_list = self.create_request_group(windows)

        received_rg_list = truncate_lower_crossing_windows([rg1])

        request = received_rg_list[0].requests[0]
        received_windows = get_windows_from_request(request, self.resource_name)

        assert_equal(received_windows[0].start, self.current_time)


    @patch("adaptive_scheduler.valhalla_connections.ObservationPortalInterface.get_semester_details")
    def test_truncates_upper_crossing_windows(self, mock_semester_service):
        mock_semester_service.return_value = {'id': '2015A', 'start': self.semester_end - timedelta(days=300),
                                              'end': self.semester_end}

        # Crosses semester end, so should be truncated
        window_dict1 = {
                         'start' : "2013-06-01T00:00:00Z",
                         'end'   : "2013-11-01T01:00:00Z",
                       }
        windows = [ (window_dict1,) ]
        window_end = datetime.strptime(window_dict1['end'], '%Y-%m-%dT%H:%M:%SZ')
        expire_time = min(window_end, self.semester_end)

        rg1, window_list = self.create_request_group(windows, expires=expire_time)
        rg1.expires = datetime(2013, 12, 1)
        received_rg_list = truncate_upper_crossing_windows([rg1])

        request = received_rg_list[0].requests[0]
        received_windows = get_windows_from_request(request, self.resource_name)

        assert_equal(received_windows[0].end, self.semester_end)


    @patch("adaptive_scheduler.valhalla_connections.ObservationPortalInterface.get_semester_details")
    def test_truncates_upper_crossing_windows_extra_horizon(self, mock_semester_service):
        mock_semester_service.return_value = {'id': '2015A', 'start': self.semester_end - timedelta(days=30),
                                              'end': self.semester_end}

        horizon = datetime(2013, 7, 1)
        # Crosses effective horizon, so should be truncated
        window_dict1 = {
                         'start' : "2013-06-01T00:00:00Z",
                         'end'   : "2013-11-01T01:00:00Z",
                       }
        windows = [ (window_dict1,) ]
        rg1, window_list = self.create_request_group(windows)
        rg1.expires = datetime(2013, 12, 1)


        received_rg_list = truncate_upper_crossing_windows([rg1], horizon=horizon)

        request = received_rg_list[0].requests[0]
        received_windows = get_windows_from_request(request, self.resource_name)

        assert_equal(received_windows[0].end, horizon)


    def test_filter_on_duration_window_larger_tdelta(self):

        # Window is larger than one hour
        window_dict1 = {
                         'start' : "2013-09-01T00:00:00Z",
                         'end'   : "2013-11-01T01:00:00Z",
                       }
        windows = [ (window_dict1,) ]
        rg1, window_list = self.create_request_group(windows)

        with patch.object(Request, 'duration') as mock_duration:
            mock_duration.__get__ = Mock(return_value=3600.0)
            received_rg_list = filter_on_duration([rg1])

        request = received_rg_list[0].requests[0]
        received_windows = get_windows_from_request(request, self.resource_name)

        assert_equal(received_windows, window_list)


    def test_filter_on_duration_window_larger_float(self):

        # Window is larger than one hour
        window_dict1 = {
                         'start' : "2013-09-01T00:00:00Z",
                         'end'   : "2013-11-01T01:00:00Z",
                       }
        windows = [ (window_dict1,) ]
        rg1, window_list = self.create_request_group(windows)

        with patch.object(Request, 'duration') as mock_duration:
            mock_duration.__get__ = Mock(return_value=3600.0)
            received_rg_list = filter_on_duration([rg1])

        request = received_rg_list[0].requests[0]
        received_windows = get_windows_from_request(request, self.resource_name)

        assert_equal(received_windows, window_list)


    def test_filter_on_duration_window_smaller_tdelta(self):

        # Window is smaller than one hour
        window_dict1 = {
                         'start' : "2013-09-01T00:00:00Z",
                         'end'   : "2013-09-01T00:30:00Z",
                       }
        windows = [ (window_dict1,) ]
        rg1, window_list = self.create_request_group(windows)

        with patch.object(Request, 'duration') as mock_duration:
            mock_duration.__get__ = Mock(return_value=3600.0)
            received_rg_list = filter_on_duration([rg1])

        received_rg_list = filter_on_duration([rg1])

        request = received_rg_list[0].requests[0]
        received_windows = get_windows_from_request(request, self.resource_name)

        assert_equal(received_windows, [])


    def test_filter_on_duration_window_smaller_float(self):

        # Window is smaller than one hour
        window_dict1 = {
                         'start' : "2013-09-01T00:00:00Z",
                         'end'   : "2013-09-01T00:30:00Z",
                       }
        windows = [ (window_dict1,) ]
        rg1, window_list = self.create_request_group(windows)

        with patch.object(Request, 'duration') as mock_duration:
            mock_duration.__get__ = Mock(return_value=3600.0)
            received_rg_list = filter_on_duration([rg1])

        request = received_rg_list[0].requests[0]
        received_windows = get_windows_from_request(request, self.resource_name)

        assert_equal(received_windows, [])


    def test_filter_on_duration_emits_user_feedback(self):

        # Trickery to gain access to the inner closure
        arg_list = []
        def recording_filter_executor(rg_list, filter_test):
            arg_list.append(rg_list)
            arg_list.append(filter_test)

            return rg_list

        rg_list = []
        filter_on_duration(rg_list, recording_filter_executor)

        w = Mock()
        w.start = datetime(2013, 10, 1)
        w.end   = datetime(2013, 10, 3)
        w.get_resource_name.return_value = 'elp'

        ur = Mock()

        r = Mock()
        r.id = 1
        r.duration = 5*24*3600

        arg_list[1](w, ur, r)
        expected_msg = "Request %d Window (at elp) 2013-10-01 00:00:00 -> 2013-10-03 00:00:00 too small for duration '5 days, 0:00:00'" % r.id
        expected_tag = 'WindowTooSmall'
        ur.emit_rg_feedback.assert_called_with(expected_msg, expected_tag)

    def test_filter_on_duration_no_user_feedback_if_ok(self):

        # Trickery to gain access to the inner closure
        arg_list = []
        def recording_filter_executor(rg_list, filter_test):
            arg_list.append(rg_list)
            arg_list.append(filter_test)

            return rg_list

        rg_list = []
        filter_on_duration(rg_list, recording_filter_executor)

        w = Mock()
        w.start = datetime(2013, 10, 1)
        w.end   = datetime(2013, 10, 3)

        rg = Mock()

        r = Mock()
        r.duration = 1*24*3600

        arg_list[1](w, rg, r)
        assert_equal(rg.emit_rg_feedback.called, False)

    def test_filter_on_type_AND_both_requests_have_windows(self):
        window_dict1 = {
                         'start' : "2013-03-01T00:00:00Z",
                         'end'   : "2013-03-01T00:30:00Z",
                       }
        window_dict2 = {
                         'start' : "2013-03-01T00:00:00Z",
                         'end'   : "2013-03-01T00:30:00Z",
                       }
        windows = [ (window_dict1,), (window_dict2,) ]
        rg1, window_list = self.create_request_group(windows, operator='and')

        running_request_ids = []
        received_rg_list = filter_on_type([rg1], running_request_ids)
        assert_equal(len(received_rg_list), 1)

    def test_filter_on_type_AND_neither_request_has_windows(self):
        windows = [ (), () ]
        rg1, window_list = self.create_request_group(windows, operator='and')

        running_request_ids = []
        received_rg_list = filter_on_type([rg1], running_request_ids)
        assert_equal(len(received_rg_list), 0)

    def test_filter_on_type_AND_one_request_has_no_windows(self):
        window_dict1 = {
                         'start' : "2013-03-01T00:00:00Z",
                         'end'   : "2013-03-01T00:30:00Z",
                       }
        windows = [(window_dict1,), () ]
        rg1, window_list = self.create_request_group(windows, operator='and')

        running_request_ids = []
        received_rg_list = filter_on_type([rg1], running_request_ids)

        assert_equal(len(received_rg_list), 0)

    def test_filter_on_type_AND_two_windows_one_request_has_no_windows(self):
        window_dict1 = {
                         'start' : "2013-03-01T00:00:00Z",
                         'end'   : "2013-03-01T00:30:00Z",
                       }
        window_dict2 = {
                         'start' : "2013-03-01T00:00:00Z",
                         'end'   : "2013-03-01T00:30:00Z",
                       }
        windows = [ (window_dict1, window_dict2), () ]
        rg1, window_list = self.create_request_group(windows, operator='and')

        running_request_ids = []
        received_rg_list = filter_on_type([rg1], running_request_ids)
        assert_equal(len(received_rg_list), 0)

    def test_filter_on_type_ONEOF_both_requests_have_windows(self):
        window_dict1 = {
                         'start' : "2013-03-01T00:00:00Z",
                         'end'   : "2013-03-01T00:30:00Z",
                       }
        window_dict2 = {
                         'start' : "2013-03-01T00:00:00Z",
                         'end'   : "2013-03-01T00:30:00Z",
                       }
        windows = [ (window_dict1,), (window_dict2,) ]
        rg1, window_list = self.create_request_group(windows, operator='oneof')

        running_request_ids = []
        received_rg_list = filter_on_type([rg1], running_request_ids)
        assert_equal(len(received_rg_list), 1)


    def test_filter_on_type_ONEOF_one_request_has_no_windows(self):
        window_dict1 = {
                         'start' : "2013-03-01T00:00:00Z",
                         'end'   : "2013-03-01T00:30:00Z",
                       }
        windows = [ (window_dict1,), () ]
        rg1, window_list = self.create_request_group(windows, operator='oneof')

        running_request_ids = []
        received_rg_list = filter_on_type([rg1], running_request_ids)
        assert_equal(len(received_rg_list), 1)


    def test_filter_on_type_ONEOF_two_windows_one_request_has_no_windows(self):
        window_dict1 = {
                         'start' : "2013-03-01T00:00:00Z",
                         'end'   : "2013-03-01T00:30:00Z",
                       }
        window_dict2 = {
                         'start' : "2013-03-01T00:00:00Z",
                         'end'   : "2013-03-01T00:30:00Z",
                       }
        windows = [ (window_dict1, window_dict2), () ]
        rg1, window_list = self.create_request_group(windows, operator='oneof')

        running_request_ids = []
        received_rg_list = filter_on_type([rg1], running_request_ids)
        assert_equal(len(received_rg_list), 1)


    def test_filter_on_type_ONEOF_neither_request_has_windows(self):
        windows = [ (), () ]
        rg1, window_list = self.create_request_group(windows, operator='oneof')

        running_request_ids = []
        received_rg_list = filter_on_type([rg1], running_request_ids)
        assert_equal(len(received_rg_list), 0)


    def test_filter_on_type_SINGLE_no_window_one_request(self):
        windows = [ () ]
        rg1, window_list = self.create_request_group(windows, operator='single')

        running_request_ids = []
        received_rg_list = filter_on_type([rg1], running_request_ids)
        assert_equal(len(received_rg_list), 0)


    def test_filter_on_type_SINGLE_two_windows_one_request(self):
        window_dict1 = {
                         'start' : "2013-03-01T00:00:00Z",
                         'end'   : "2013-03-01T00:30:00Z",
                       }
        window_dict2 = {
                         'start' : "2013-03-01T00:00:00Z",
                         'end'   : "2013-03-01T00:30:00Z",
                       }
        windows = [ (window_dict1, window_dict2) ]
        rg1, window_list = self.create_request_group(windows, operator='single')

        running_request_ids = []
        received_rg_list = filter_on_type([rg1], running_request_ids)
        assert_equal(len(received_rg_list), 1)


    def test_filter_on_type_SINGLE_no_windows_but_running(self):
        windows = [()]
        rg1, window_list = self.create_request_group(windows, operator='single')
        rg_list = [rg1]
        assert_equal(1, len(rg_list))
        assert_equal(1, len(rg_list[0].requests))
        assert_equal(0, rg_list[0].requests[0].windows.size())
        running_request_ids = [5]
        received_rg_list = filter_on_type([rg1], running_request_ids)
        assert_equal(len(received_rg_list), 1)

    def test_filter_on_type_NON_SINGLE_both_requests_no_windows_but_running(self):
        for operator in ('and', 'many', 'oneof'):
            windows = [(), ()]
            rg1, window_list = self.create_request_group(windows, operator=operator)
            rg_list = [rg1]
            assert_equal(1, len(rg_list))
            assert_equal(2, len(rg_list[0].requests))
            assert_equal(0, rg_list[0].requests[0].windows.size())
            assert_equal(0, rg_list[0].requests[1].windows.size())
            running_request_ids = [5, 6]
            received_rg_list = filter_on_type([rg1], running_request_ids)
            assert_equal(len(received_rg_list), 1, msg="Request Group should not be filtered for operator '%s'" % operator)
            assert_equal(2, len(rg_list[0].requests), msg="Requests should not be filtered from request group with operator '%s'" % operator)

    def test_filter_on_type_NON_SINGLE_both_one_request_no_windows_but_running(self):
        for operator in ('and', 'many', 'oneof'):
            window_dict1 = {
                             'start' : "2013-03-01T00:00:00Z",
                             'end'   : "2013-03-01T00:30:00Z",
                           }
            windows = [(), (window_dict1,)]
            rg1, window_list = self.create_request_group(windows, operator='and')
            rg_list = [rg1]
            assert_equal(1, len(rg_list))
            assert_equal(2, len(rg_list[0].requests))
            assert_equal(0, rg_list[0].requests[0].windows.size())
            assert_equal(1, rg_list[0].requests[1].windows.size())
            running_request_ids = [5]
            received_rg_list = filter_on_type([rg1], running_request_ids)
            assert_equal(len(received_rg_list), 1, msg="Request Group should not be filtered for operator '%s'" % operator)
            assert_equal(2, len(rg_list[0].requests), msg="Requests should not be filtered from request group with operator '%s'" % operator)

    def test_drop_empty_requests(self):
        request_id = 5
        r  = Request(
                      target         = None,
                      configurations= None,
                      windows        = Windows(),
                      constraints    = None,
                      id= request_id
                    )
        r2  = Request(
                      target         = None,
                      configurations= None,
                      windows        = Windows(),
                      constraints    = None,
                      id=9
                    )
        rg1 = RequestGroup(
                           operator        = 'single',
                           requests        = [r],
                           proposal        = None,
                           expires         = None,
                           id=1,
                           group_id        = None,
                           ipp_value       = 1.0,
                           observation_type= 'NORMAL',
                           submitter       = '',
                         )
        received = drop_empty_requests([rg1])
        assert_equal(received, [5])

    def test_filter_on_pending(self):
        request_id = 5
        r1  = Request(
                      target         = None,
                      configurations= None,
                      windows        = Windows(),
                      constraints    = None,
                      id= request_id,
                      state          = 'PENDING'
                    )
        r2  = Request(
                      target         = None,
                      configurations= None,
                      windows        = Windows(),
                      constraints    = None,
                      id=9,
                      state          = 'UNSCHEDULABLE'
                    )
        rg1 = RequestGroup(
                           operator        = 'single',
                           requests        = [r1, r2],
                           proposal        = None,
                           expires         = None,
                           id=1,
                           group_id        = None,
                           ipp_value       = 1.0,
                           observation_type= 'NORMAL',
                           submitter       = '',
                         )

        filter_on_pending([rg1])

        assert_equal(rg1.requests, [r1])

    def test_run_all_filters(self):
        window_dict1 = {
                         'start' : "2013-03-01T00:00:00Z",
                         'end'   : "2013-03-01T01:30:00Z",
                       }
        windows = [ (window_dict1,) ]
        rg1, window_list = self.create_request_group(windows, operator='single')
        rg1.expires = datetime(2013, 12, 1)
        running_request_ids = []
        with patch.object(Request, 'duration') as mock_duration:
            mock_duration.__get__ = Mock(return_value=3600.0)
            received_rg_list = run_all_filters([rg1], running_request_ids)

        assert_equal(len(received_rg_list), 1)

    def test_filter_out_windows_for_running_requests_single(self):
        window_dict1 = {
                         'start' : "2013-03-01T00:00:00Z",
                         'end'   : "2013-03-01T00:30:00Z",
                       }
        window_dict2 = {
                         'start' : "2013-03-01T00:00:00Z",
                         'end'   : "2013-03-01T00:30:00Z",
                       }
        windows = [ (window_dict1, window_dict2) ]
        rg1, window_list = self.create_request_group(windows, operator='single')

        running_request_ids = [5]
        rg_list = [rg1]
        assert_equal(1, len(rg_list))
        assert_equal(1, len(rg_list[0].requests))
        assert_equal(2, rg_list[0].requests[0].windows.size())
        received_rg_list = filter_out_windows_for_running_requests(rg_list, running_request_ids)
        assert_equal(1, len(received_rg_list))
        assert_equal(1, len(received_rg_list[0].requests))
        assert_equal(0, received_rg_list[0].requests[0].windows.size())

    def test_filter_out_windows_for_running_requests_many(self):
        window_dict1 = {
                         'start' : "2013-03-01T00:00:00Z",
                         'end'   : "2013-03-01T00:30:00Z",
                       }
        window_dict2 = {
                         'start' : "2013-03-01T00:00:00Z",
                         'end'   : "2013-03-01T00:30:00Z",
                       }
        windows = [ (window_dict1, window_dict2), (window_dict1, window_dict2) ]
        rg1, window_list = self.create_request_group(windows, operator='many')

        running_request_ids = [5]
        rg_list = [rg1]
        assert_equal(1, len(rg_list))
        assert_equal([5, 6], [r.id for r in rg_list[0].requests])
        assert_equal(2, len(rg_list[0].requests))
        assert_equal(2, rg_list[0].requests[0].windows.size())
        assert_equal(2, rg_list[0].requests[1].windows.size())
        received_rg_list = filter_out_windows_for_running_requests([rg1], running_request_ids)
        assert_equal(1, len(received_rg_list))
        assert_equal(2, len(received_rg_list[0].requests))
        for r in received_rg_list[0].requests:
            if r.id == 5:
                assert_equal(0, r.windows.size())
            if r.id == 6:
                assert_equal(2, r.windows.size())
