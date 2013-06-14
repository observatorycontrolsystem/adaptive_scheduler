#!/usr/bin/python
from __future__ import division

from nose.tools import assert_equal, assert_almost_equals

from adaptive_scheduler.model2 import (Telescope, Target, Request, Window, Windows,
                                       Molecule, Constraints)
from adaptive_scheduler.utils import (iso_string_to_datetime,
                                      datetime_to_epoch,
                                      normalised_epoch_to_datetime)
from adaptive_scheduler.kernel_mappings import (construct_visibilities,
                                                rise_set_to_kernel_intervals,
                                                make_dark_up_kernel_intervals,
                                                construct_global_availability,
                                                normalise_dt_intervals,
                                                set_airmass_limit)
from adaptive_scheduler.kernel.intervals import Intervals
from adaptive_scheduler.kernel.timepoint import Timepoint

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

        constraints = Constraints({})

        req  = Request(
                       target    = self.target,
                       molecules = [self.mol],
                       windows   = dt_windows,
                       constraints = constraints,
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

        constraints = Constraints({})
        req  = Request(
                        target     = self.target,
                        molecules  = [self.mol],
                        windows    = dt_windows,
                        constraints = constraints,
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

        constraints = Constraints({})
        req  = Request(
                       target     = self.target,
                       molecules  = [self.mol],
                       windows    = dt_windows,
                       constraints = constraints,
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


    def test_construct_global_availability(self):
        tel_name = '1m0a.doma.tst'
        sem_start = datetime(2012, 10, 1)

        # Resource is available from 3-7

        dt0 = datetime(2013, 3, 22, 3)
        dt1 = datetime(2013, 3, 22, 7)

        dt_resource_int = Intervals(
                            [
                              Timepoint(dt0, 'start'),
                              Timepoint(dt1, 'end'),
                            ]
                          )
        epoch_resource_int = normalise_dt_intervals(dt_resource_int, sem_start)
        resource_windows = {
                             tel_name : epoch_resource_int
                           }

        # Resource is unavailable from 4-5
        dt2 = datetime(2013, 3, 22, 4)
        dt3 = datetime(2013, 3, 22, 5)
        now = dt2
        running_at_tel = {
                           tel_name : {
                               'cutoff'  : dt3,
                               'running' : []
                            },
                         }

        # Expected available intervals after masking are
        # 3-4, 5-7
        received = construct_global_availability(now, sem_start, running_at_tel,
                                                 resource_windows)
        received_int = received[tel_name]
        assert_equal(len(received_int.timepoints), 4)
        r0 = normalised_epoch_to_datetime(received_int.timepoints[0].time,
                                          datetime_to_epoch(sem_start))
        r1 = normalised_epoch_to_datetime(received_int.timepoints[1].time,
                                          datetime_to_epoch(sem_start))
        r2 = normalised_epoch_to_datetime(received_int.timepoints[2].time,
                                          datetime_to_epoch(sem_start))
        r3 = normalised_epoch_to_datetime(received_int.timepoints[3].time,
                                          datetime_to_epoch(sem_start))
        assert_equal(r0, dt0)
        assert_equal(r1, dt2)
        assert_equal(r3, dt1)



    def test_set_airmass_limit_no_airmass(self):
        class Tel(object): pass

        t = Tel()
        t.horizon = 30

        expected = t.horizon
        assert_equal(set_airmass_limit(None, t), expected)


    def test_set_airmass_limit_airmass_worse_than_horizon(self):
        class Tel(object): pass

        t = Tel()
        t.horizon = 30

        airmass = 3

        expected = t.horizon
        assert_equal(set_airmass_limit(airmass, t), expected)


    def test_set_airmass_limit_airmass_better_than_horizon(self):
        class Tel(object): pass

        t = Tel()
        t.horizon = 30

        airmass = 1.2

        expected = 56.44
        assert_almost_equals(set_airmass_limit(airmass, t), expected, places=2)
