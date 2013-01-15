#!/usr/bin/python
from __future__ import division

from nose.tools import assert_equal

from adaptive_scheduler.model2 import (Telescope, Target, Request, Window, Windows,
                                       Molecule)
from adaptive_scheduler.utils import iso_string_to_datetime
from adaptive_scheduler.kernel_mappings import (construct_visibilities,
                                                rise_set_to_kernel_intervals,
                                                make_dark_up_kernel_intervals)

from datetime import datetime


class TestKernelMappings(object):

    def setup(self):
        self.start = datetime(2011, 11, 1, 0, 0, 0)
        self.end   = datetime(2011, 11, 3, 0, 0, 0)

        self.tels = {
                      '1m0a.doma.bpl' :
                                        Telescope(
                                                   name      = '1m0a.doma.bpl',
                                                   tel_class = '1m0',
                                                   latitude  = 34.433157,
                                                   longitude = -119.86308,
                                                   horizon   = 25,
                                                 )
                    }

        self.target = Target(
                              #ra  = '20 41 25.91',
                              #dec = '+45 16 49.22',
                              ra  = 310.35795833333333,
                              dec = 45.280338888888885
                            )

        self.mol = Molecule()


    def test_make_dark_up_kernel_intervals(self):
        window_dict = {
                        'start' : self.start,
                        'end'   : self.end
                      }
        resource_name = '1m0a.doma.bpl'
        resource = self.tels[resource_name]

        window = Window(window_dict, resource)
        dt_windows = Windows()
        dt_windows.append(window)

        req  = Request(
                       target    = self.target,
                       molecules = [self.mol],
                       windows   = dt_windows,
                       request_number = '1'
                      )

        visibility_from = construct_visibilities(self.tels, self.start, self.end)
        received = make_dark_up_kernel_intervals(req, visibility_from)

        format = '%Y-%m-%d %H:%M:%S.%f'
        rise_set_dark_intervals = (
                            datetime.strptime('2011-11-01 02:02:43.252928', format),
                            datetime.strptime('2011-11-01 07:52:00.564203', format),
                            datetime.strptime('2011-11-02 02:01:50.419578', format),
                            datetime.strptime('2011-11-02 07:48:04.692318', format)
                           )

        # Verify we get the intervals we expect
        for resource_name, received_intervals in received.iteritems():
            for i, received_tp in enumerate(received_intervals.timepoints):
                assert_equal(received_tp.time, rise_set_dark_intervals[i])


    def test_user_interval_is_honoured(self):
        # A one day user supplied window
        window_dict = {
                        'start' : datetime(2011, 11, 1, 6, 0, 0),
                        'end'   : datetime(2011, 11, 2, 6, 0, 0)
                      }
        resource_name = '1m0a.doma.bpl'
        resource = self.tels[resource_name]

        window = Window(window_dict, resource)
        dt_windows = Windows()
        dt_windows.append(window)

        req  = Request(
                        target     = self.target,
                        molecules  = [self.mol],
                        windows    = dt_windows,
                        request_number = '1'
                      )

        visibility_from = construct_visibilities(self.tels, self.start, self.end)

        received = make_dark_up_kernel_intervals(req, visibility_from)

        # The user windows constrain the available observing windows (compare to
        # previous test)
        format = '%Y-%m-%d %H:%M:%S.%f'
        rise_set_dark_intervals = (
                             datetime.strptime('2011-11-01 06:00:00.0', format),
                             datetime.strptime('2011-11-01 07:52:00.564203', format),
                             datetime.strptime('2011-11-02 02:01:50.419578', format),
                             datetime.strptime('2011-11-02 06:00:00.0', format),
                            )

        # Verify we get the intervals we expect
        for resource_name, received_intervals in received.iteritems():
            for i, received_tp in enumerate(received_intervals.timepoints):
                assert_equal(received_tp.time, rise_set_dark_intervals[i])



    def test_multiple_user_intervals_are_honoured(self):
        # A one day user supplied window
        windows = [
                   {
                     'start' : datetime(2011, 11, 1, 6, 0, 0),
                     'end'   : datetime(2011, 11, 1, 9, 0, 0)
                   },
                   {
                     'start' : datetime(2011, 11, 2, 1, 0, 0),
                     'end'   : datetime(2011, 11, 2, 4, 0, 0)
                   }
                ]

        dt_windows = Windows()
        resource_name = '1m0a.doma.bpl'
        for w in windows:
            dt_windows.append(Window(w, self.tels[resource_name]))

        req  = Request(
                       target     = self.target,
                       molecules  = [self.mol],
                       windows    = dt_windows,
                       request_number = '1'
                      )

        visibility_from = construct_visibilities(self.tels, self.start, self.end)

        received = make_dark_up_kernel_intervals(req, visibility_from)

        # The user windows constrain the available observing windows (compare to
        # previous tests)
        format = '%Y-%m-%d %H:%M:%S.%f'
        rise_set_dark_intervals = (
                            datetime.strptime('2011-11-01 06:00:00.0', format),
                            datetime.strptime('2011-11-01 07:52:00.564203', format),
                            datetime.strptime('2011-11-02 02:01:50.419578', format),
                            datetime.strptime('2011-11-02 04:00:00.0', format),
                           )

        # Verify we get the intervals we expect
        for resource_name, received_intervals in received.iteritems():
            for i, received_tp in enumerate(received_intervals.timepoints):
                assert_equal(received_tp.time, rise_set_dark_intervals[i])

