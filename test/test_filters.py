from __future__ import division

from nose.tools import assert_equal, assert_not_equal, raises
from mock       import patch, Mock
from datetime   import datetime
from copy       import deepcopy

from adaptive_scheduler.model2          import ( UserRequest, Request, Window,
                                                 Windows, Telescope )
from adaptive_scheduler.request_filters import ( filter_on_expiry,
                                                 filter_out_past_windows,
                                                 filter_out_future_windows,
                                                 truncate_lower_crossing_windows )


def get_windows_from_request(request, resource_name):
    return request.windows.windows_for_resource[resource_name]


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
        self.semester_end    = datetime(2013, 8, 1)


    def create_user_request(self, window_dicts, resource_name):
        t1 = Telescope(
                        name = resource_name
                      )

        window_list = []
        windows = Windows()
        for window_dict in window_dicts:
            w = Window(
                        window_dict = window_dict,
                        resource    = t1
                      )
            windows.append(w)
            window_list.append(w)


        r1  = Request(
                       target         = None,
                       molecules      = None,
                       windows        = windows,
                       request_number = None
                     )

        ur1 = UserRequest(
                           operator = 'single',
                           requests = [r1],
                           proposal = None,
                           expires  = None,
                           tracking_number = None,
                           group_id = None
                         )

        return ur1, window_list


    @patch("adaptive_scheduler.request_filters.datetime")
    def test_filters_out_only_past_windows(self, mock_datetime):
        mock_datetime.utcnow.return_value = self.current_time

        resource_name = "Martin"
        window_dict1 = {
                         'start' : "2013-01-01 00:00:00",
                         'end'   : "2013-01-01 01:00:00",
                       }
        # Comes after self.current_time, so should not be filtered
        window_dict2 = {
                         'start' : "2013-06-01 00:00:00",
                         'end'   : "2013-06-01 01:00:00",
                       }

        ur1, window_list = self.create_user_request([window_dict1, window_dict2], resource_name)

        received_ur_list = filter_out_past_windows([ur1])

        request = received_ur_list[0].requests[0]
        received_windows = get_windows_from_request(request, resource_name)

        expected_window = window_list[1]
        assert_equal(received_windows, [expected_window])



    @patch("adaptive_scheduler.request_filters.semester_service")
    def test_filters_out_only_future_windows(self, mock_semester_service):
        mock_semester_service.get_semester_end.return_value = self.semester_end

        resource_name = "Martin"
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

        ur1, window_list = self.create_user_request([window_dict1, window_dict2], resource_name)

        received_ur_list = filter_out_future_windows([ur1])

        request = received_ur_list[0].requests[0]
        received_windows = get_windows_from_request(request, resource_name)

        expected_window = window_list[0]
        assert_equal(received_windows, [expected_window])



    @patch("adaptive_scheduler.request_filters.datetime")
    def test_filters_out_only_future_windows(self, mock_datetime):
        mock_datetime.utcnow.return_value = self.current_time

        resource_name = "Martin"
        # Comes after self.current_time, so should not be filtered
        window_dict1 = {
                         'start' : "2013-01-01 00:00:00",
                         'end'   : "2013-03-01 01:00:00",
                       }
        ur1, window_list = self.create_user_request([window_dict1],
                                                    resource_name)

        received_ur_list = truncate_lower_crossing_windows([ur1])

        request = received_ur_list[0].requests[0]
        received_windows = get_windows_from_request(request, resource_name)

        assert_equal(received_windows[0].start, self.current_time)
