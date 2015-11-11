#!/usr/bin/python
from __future__ import division

from nose.tools import assert_equal, assert_almost_equals, assert_not_equal

from adaptive_scheduler.model2 import (Telescope, SiderealTarget, Request,
                                       CompoundRequest, UserRequest,
                                       Window, Windows, MoleculeFactory, Constraints)
from adaptive_scheduler.utils import (iso_string_to_datetime,
                                      datetime_to_epoch,
                                      normalised_epoch_to_datetime,
                                      convert_proper_motion)
from adaptive_scheduler.kernel_mappings import (construct_visibilities,
                                                construct_compound_reservation,
                                                construct_many_compound_reservation,
                                                rise_set_to_kernel_intervals,
                                                make_dark_up_kernel_intervals,
                                                construct_global_availability,
                                                normalise_dt_intervals,
                                                filter_on_scheduling_horizon)
from adaptive_scheduler.kernel.intervals import Intervals
from adaptive_scheduler.kernel.timepoint import Timepoint
from adaptive_scheduler.memoize import Memoize
from rise_set.sky_coordinates import RightAscension, Declination
from rise_set.angle import Angle
from rise_set.visibility import Visibility

from mock import Mock
from datetime import datetime




class TestKernelMappings(object):

    def setup(self):
        self.start = datetime(2011, 11, 1, 0, 0, 0)
        self.end   = datetime(2011, 11, 3, 0, 0, 0)

        self.mol_factory = MoleculeFactory()

        self.tels = {
                      '1m0a.doma.bpl' :
                                        Telescope(
                                                   name         = '1m0a.doma.bpl',
                                                   tel_class    = '1m0',
                                                   latitude     = 34.433157,
                                                   longitude    = -119.86308,
                                                   horizon      = 25,
                                                   ha_limit_neg = -12.0,
                                                   ha_limit_pos = 12.0,
                                                 )
                    }

        self.target = SiderealTarget(
                              #ra  = '20 41 25.91',
                              #dec = '+45 16 49.22',
                              ra  = 310.35795833333333,
                              dec = 45.280338888888885
                            )

        self.prop_mot_target = SiderealTarget(
                              #ra  = '20 41 25.91',
                              #dec = '+45 16 49.22',
                              ra  = 310.35795833333333,
                              dec = 45.280338888888885,
                              proper_motion_ra= 1000.0,
                              proper_motion_dec= 3200.0
                            )

        self.mol = self.mol_factory.build(
                                            dict(
                                                 type   = 'expose',
                                                 filter ='B',
                                                 bin_x  = 2,
                                                 bin_y  = 2,
                                                 exposure_count = 1,
                                                 exposure_time  = 30,
                                                )
                                               )


    def make_constrained_request(self, airmass=None,
                                 start=datetime(2011, 11, 1, 6, 0, 0),
                                 end=datetime(2011, 11, 2, 6, 0, 0)):
        # A one day user supplied window
        window_dict = {
                        'start' : start,
                        'end'   : end
                      }
        resource_name = '1m0a.doma.bpl'
        resource      = self.tels[resource_name]

        window     = Window(window_dict, resource)
        dt_windows = Windows()
        dt_windows.append(window)

        constraints = Constraints(max_airmass=airmass)
        req  = Request(
                        target         = self.target,
                        molecules      = [self.mol],
                        windows        = dt_windows,
                        constraints    = constraints,
                        request_number = '1',
                        instrument_type = '1M0-SCICAM-SBIG',
                      )

        return req


    def make_compound_request(self, requests, operator):
        cr = CompoundRequest(
                              operator = operator,
                              requests = requests
                            )

        return cr


    def make_user_request(self, requests, operator='single'):
        ur = UserRequest(operator, requests, 'Test Proposal',
                         datetime(2012, 1, 1), '1', 'test group id')
        
        return ur


    def make_intersection_dict(self):
        timepoints = [
                       Timepoint(
                                  time= datetime(2011, 11, 1, 6, 0, 0),
                                  type='start'
                                ),
                       Timepoint(
                                  time= datetime(2011, 11, 1, 7, 0, 0),
                                  type='end'
                                ),
                     ]
        intervals = Intervals(timepoints)

        intersection_dict = {
                              '1m0a.doma.coj' : intervals
                            }

        return intersection_dict


    def make_dt_intervals_list(self):
        dt_intervals_list = [
                              self.make_intersection_dict(),
                              self.make_intersection_dict(),
                            ]

        return dt_intervals_list

    def test_construct_compound_reservation(self):
        request           = self.make_constrained_request()
        requests          = [request, request]
        operator          = 'and'
        compound_request  = self.make_compound_request(requests, operator)
        dt_intervals_list = self.make_dt_intervals_list()
        sem_start         = self.start

        #TODO: Replace with cleaner mock patching
        compound_request.priority = 1

        received = construct_compound_reservation(compound_request,
                                                  dt_intervals_list,
                                                  sem_start)

        assert_equal(len(received.reservation_list), len(requests))
        assert_equal(received.type, operator)


    def test_construct_many_compound_reservation(self):
        request           = self.make_constrained_request()
        requests          = [request, request]
        operator          = 'many'
        compound_request  = self.make_compound_request(requests, operator)
        intersection_dict = self.make_intersection_dict()
        sem_start         = self.start

        #TODO: Replace with cleaner mock patching
        compound_request.priority = 1

        received = construct_many_compound_reservation(
                                               compound_request,
                                               child_idx=0,
                                               intersection_dict=intersection_dict,
                                               sem_start=sem_start)

        assert_equal(len(received.reservation_list), 1)
        assert_equal(received.type, 'single')


    def test_filter_on_scheduling_horizon_applies_horizon_to_singles(self):
        start             = datetime(2011, 11, 1, 6, 0, 0)
        end               = datetime(2011, 12, 1, 6, 0, 0)
        request           = self.make_constrained_request(start=start, end=end)
        operator          = 'single'
        requests          = [request]
        user_request      = self.make_user_request(requests, operator)
        user_requests     = [user_request]
        scheduling_horizon = datetime(2011, 11, 15, 6, 0, 0)
        filtered_urs = filter_on_scheduling_horizon(user_requests,
                                                    scheduling_horizon)
        
        expected_window_start = start
        expected_window_end = scheduling_horizon
        
        assert_equal(1, len(filtered_urs))
        output_ur = filtered_urs[0]
        assert_equal(1, len(output_ur.requests))
        bpl_1m0a_doma_windows = output_ur.requests[0].windows.at('1m0a.doma.bpl')
        assert_equal(1, len(bpl_1m0a_doma_windows))
        assert_equal(bpl_1m0a_doma_windows[0].start, expected_window_start)
        assert_equal(bpl_1m0a_doma_windows[0].end, expected_window_end)


    def test_filter_on_scheduling_horizon_applies_horizon_to_manys(self):
            start             = datetime(2011, 11, 1, 6, 0, 0)
            end               = datetime(2011, 12, 1, 6, 0, 0)
            request1          = self.make_constrained_request(start=start, end=end)
            request2          = self.make_constrained_request(start=start, end=end)
            operator          = 'many'
            requests          = [request1, request2]
            user_request      = self.make_user_request(requests, operator)
            user_requests     = [user_request]
            scheduling_horizon = datetime(2011, 11, 15, 6, 0, 0)
            filtered_urs = filter_on_scheduling_horizon(user_requests,
                                                        scheduling_horizon)
            
            expected_window_start = start
            expected_window_end = scheduling_horizon
            
            assert_equal(1, len(filtered_urs))
            output_ur = filtered_urs[0]
            assert_equal(2, len(output_ur.requests))
            bpl_1m0a_doma_windows1 = output_ur.requests[0].windows.at('1m0a.doma.bpl')
            bpl_1m0a_doma_windows2 = output_ur.requests[0].windows.at('1m0a.doma.bpl')
            assert_equal(1, len(bpl_1m0a_doma_windows1))
            assert_equal(1, len(bpl_1m0a_doma_windows2))
            assert_equal(bpl_1m0a_doma_windows1[0].start, expected_window_start)
            assert_equal(bpl_1m0a_doma_windows1[0].end, expected_window_end)
            assert_equal(bpl_1m0a_doma_windows2[0].start, expected_window_start)
            assert_equal(bpl_1m0a_doma_windows2[0].end, expected_window_end)


    def test_filter_on_scheduling_horizon_no_horizon_applied_to_oneof(self):
            start             = datetime(2011, 11, 1, 6, 0, 0)
            end               = datetime(2011, 12, 1, 6, 0, 0)
            request1          = self.make_constrained_request(start=start, end=end)
            request2          = self.make_constrained_request(start=start, end=end)
            operator          = 'oneof'
            requests          = [request1, request2]
            user_request      = self.make_user_request(requests, operator)
            user_requests     = [user_request]
            scheduling_horizon = datetime(2011, 11, 15, 6, 0, 0)
            filtered_urs = filter_on_scheduling_horizon(user_requests,
                                                        scheduling_horizon)
            
            expected_window_start = start
            expected_window_end = end
            
            assert_equal(1, len(filtered_urs))
            output_ur = filtered_urs[0]
            assert_equal(2, len(output_ur.requests))
            bpl_1m0a_doma_windows1 = output_ur.requests[0].windows.at('1m0a.doma.bpl')
            bpl_1m0a_doma_windows2 = output_ur.requests[0].windows.at('1m0a.doma.bpl')
            assert_equal(1, len(bpl_1m0a_doma_windows1))
            assert_equal(1, len(bpl_1m0a_doma_windows2))
            assert_equal(bpl_1m0a_doma_windows1[0].start, expected_window_start)
            assert_equal(bpl_1m0a_doma_windows1[0].end, expected_window_end)
            assert_equal(bpl_1m0a_doma_windows2[0].start, expected_window_start)
            assert_equal(bpl_1m0a_doma_windows2[0].end, expected_window_end)


    def test_filter_on_scheduling_horizon_no_horizon_applied_to_and(self):
            start             = datetime(2011, 11, 1, 6, 0, 0)
            end               = datetime(2011, 12, 1, 6, 0, 0)
            request1          = self.make_constrained_request(start=start, end=end)
            request2          = self.make_constrained_request(start=start, end=end)
            operator          = 'and'
            requests          = [request1, request2]
            user_request      = self.make_user_request(requests, operator)
            user_requests     = [user_request]
            scheduling_horizon = datetime(2011, 11, 15, 6, 0, 0)
            filtered_urs = filter_on_scheduling_horizon(user_requests,
                                                        scheduling_horizon)
            
            expected_window_start = start
            expected_window_end = end
            
            assert_equal(1, len(filtered_urs))
            output_ur = filtered_urs[0]
            assert_equal(2, len(output_ur.requests))
            bpl_1m0a_doma_windows1 = output_ur.requests[0].windows.at('1m0a.doma.bpl')
            bpl_1m0a_doma_windows2 = output_ur.requests[0].windows.at('1m0a.doma.bpl')
            assert_equal(1, len(bpl_1m0a_doma_windows1))
            assert_equal(1, len(bpl_1m0a_doma_windows2))
            assert_equal(bpl_1m0a_doma_windows1[0].start, expected_window_start)
            assert_equal(bpl_1m0a_doma_windows1[0].end, expected_window_end)
            assert_equal(bpl_1m0a_doma_windows2[0].start, expected_window_start)
            assert_equal(bpl_1m0a_doma_windows2[0].end, expected_window_end)


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
                       target         = self.target,
                       molecules      = [self.mol],
                       windows        = dt_windows,
                       constraints    = constraints,
                       request_number = '1'
                      )

        visibility_from = construct_visibilities(self.tels, self.start, self.end)
        received = make_dark_up_kernel_intervals(req, visibility_from)

        format = '%Y-%m-%d %H:%M:%S.%f'
        rise_set_dark_intervals = (
                            datetime.strptime('2011-11-01 02:02:43.257196', format),
                            datetime.strptime('2011-11-01 07:52:00.564199', format),
                            datetime.strptime('2011-11-02 02:01:50.423880', format),
                            datetime.strptime('2011-11-02 07:48:04.692316', format)
                           )

        # Verify we get the intervals we expect
        for resource_name, received_intervals in received.iteritems():
            for i, received_tp in enumerate(received_intervals.timepoints):
                assert_equal(received_tp.time, rise_set_dark_intervals[i])


    def test_proper_motion_in_rise_set(self):
        target_dict = self.prop_mot_target.in_rise_set_format()
        proper_motion_ra, proper_motion_dec = convert_proper_motion(getattr(self.prop_mot_target, 'proper_motion_ra'),
                                                                    getattr(self.prop_mot_target, 'proper_motion_dec'))
        assert_equal(target_dict['ra_proper_motion'].in_degrees_per_year(), proper_motion_ra / 3600.0)
        assert_equal(target_dict['dec_proper_motion'].in_degrees_per_year(), proper_motion_dec / 3600.0)


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

        received = make_dark_up_kernel_intervals(req, visibility_from, True)

        # The user windows constrain the available observing windows (compare to
        # previous test)
        format = '%Y-%m-%d %H:%M:%S.%f'
        rise_set_dark_intervals = (
                             datetime.strptime('2011-11-01 06:00:00.0', format),
                             datetime.strptime('2011-11-01 07:52:00.564199', format),
                             datetime.strptime('2011-11-02 02:01:50.423880', format),
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
                            datetime.strptime('2011-11-01 07:52:00.564199', format),
                            datetime.strptime('2011-11-02 02:01:50.423880', format),
                            datetime.strptime('2011-11-02 04:00:00.0', format),
                           )

        # Verify we get the intervals we expect
        for resource_name, received_intervals in received.iteritems():
            for i, received_tp in enumerate(received_intervals.timepoints):
                assert_equal(received_tp.time, rise_set_dark_intervals[i])


    def test_visibility_intervals_are_limited_by_hour_angle(self):

        window_dict = {
                        'start' : datetime(2013, 03, 22, 0, 0, 0),
                        'end'   : datetime(2013, 03, 23, 0, 0, 0),
                      }

        tel_name = '1m0a.doma.coj'
        tel = Telescope(
                         name         = tel_name,
                         tel_class    = '1m0',
                         latitude     = -31.273,
                         longitude    = 149.070593,
                         horizon      = 15,
                         ha_limit_neg = -4.6,
                         ha_limit_pos = 4.6,
                       )

        tels = {
                 tel_name : tel,
               }

        target = SiderealTarget(
                                  ra  = 310.35795833333333,
                                  dec = -60.0,
                               )

        window = Window(window_dict, tel)
        dt_windows = Windows()
        dt_windows.append(window)

        constraints = Constraints()
        req = Request(
                       target          = target,
                       molecules       = [self.mol],
                       windows         = dt_windows,
                       constraints     = constraints,
                       request_number  = '1',
                       instrument_type = '1M0-SCICAM-SBIG',
                     )
        sem_start = datetime(2013, 03, 1, 0, 0, 0)
        sem_end   = datetime(2013, 03, 31, 0, 0, 0)

        visibility_from = construct_visibilities(tels, sem_start, sem_end)
        received = make_dark_up_kernel_intervals(req, visibility_from)


        # Hour angle not violated independently confirmed by hand-cranking through SLALIB
        expected_tps = [
                         {
                           'type': 'start',
                           'time': datetime(2013, 3, 22, 18, 11, 28, 78641)
                         },
                         {
                           'type': 'end',
                           'time': datetime(2013, 3, 22, 19, 16, 27, 292072)
                         },
                       ]

        for received_tp, expected_tp in zip(received[tel_name].timepoints, expected_tps):
            assert_equal(received_tp.type, expected_tp['type'])
            assert_equal(received_tp.time, expected_tp['time'])


    def test_visibility_intervals_at_low_horizon_are_allowed_by_hour_angle(self):

        window_dict = {
                        'start' : datetime(2013, 03, 22, 0, 0, 0),
                        'end'   : datetime(2013, 03, 23, 0, 0, 0),
                      }

        tel_name = '1m0a.doma.coj'
        tel = Telescope(
                         name         = tel_name,
                         tel_class    = '1m0',
                         latitude     = -31.273,
                         longitude    = 149.070593,
                         horizon      = 15,
                         ha_limit_neg = -4.6,
                         ha_limit_pos = 4.6,
                       )

        tels = {
                 tel_name : tel,
               }

        target = SiderealTarget(
                                  # RA 15:41:25.91
                                  ra  = 235.357958333,
                                  dec = -60.0,
                               )

        window = Window(window_dict, tel)
        dt_windows = Windows()
        dt_windows.append(window)

        constraints = Constraints()
        req = Request(
                       target          = target,
                       molecules       = [self.mol],
                       windows         = dt_windows,
                       constraints     = constraints,
                       request_number  = '1',
                       instrument_type = '1M0-SCICAM-SBIG',
                     )
        sem_start = datetime(2013, 03, 1, 0, 0, 0)
        sem_end   = datetime(2013, 03, 31, 0, 0, 0)

        visibility_from = construct_visibilities(tels, sem_start, sem_end)
        received = make_dark_up_kernel_intervals(req, visibility_from)


        # Hour angle not violated independently confirmed by hand-cranking through SLALIB
        expected_tps = [
                         {
                           'type': 'start',
                           'time': datetime(2013, 3, 22, 13, 12, 17, 226447)
                         },
                         {
                           'type': 'end',
                           'time': datetime(2013, 3, 22, 19, 16, 27, 292072)
                         },
                       ]


        for received_tp, expected_tp in zip(received[tel_name].timepoints, expected_tps):
            assert_equal(received_tp.type, expected_tp['type'])
            assert_equal(received_tp.time, expected_tp['time'])


    def test_construct_global_availability(self):
        tel_name = '1m0a.doma.bpl'
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
        masked_inervals = {
                           '1m0a.doma.bpl' : Intervals([Timepoint(dt2, 'start'),
                                                        Timepoint(dt3, 'end')])
                           }

        # Expected available intervals after masking are
        # 3-4, 5-7
        received = construct_global_availability(masked_inervals, sem_start,
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



    def test_airmass_is_honoured_high_airmass(self):
        airmass = 3.0
        req_airmass3   = self.make_constrained_request(airmass)
        req_no_airmass = self.make_constrained_request()

        visibility_from = construct_visibilities(self.tels, self.start, self.end)

        received_no_airmass = make_dark_up_kernel_intervals(req_no_airmass,
                                                            visibility_from,
                                                            True)
        timepoints_no_airmass = received_no_airmass['1m0a.doma.bpl'].timepoints

        received_airmass3 = make_dark_up_kernel_intervals(req_airmass3,
                                                          visibility_from,
                                                          True)
        timepoints_airmass3 = received_airmass3['1m0a.doma.bpl'].timepoints

        assert_equal(timepoints_no_airmass, timepoints_airmass3)


    def test_airmass_is_honoured_low_airmass(self):
        airmass = 1.0
        req_airmass1   = self.make_constrained_request(airmass)
        req_no_airmass = self.make_constrained_request()

        visibility_from = construct_visibilities(self.tels, self.start, self.end)

        received_no_airmass = make_dark_up_kernel_intervals(req_no_airmass,
                                                            visibility_from, True)
        timepoints_no_airmass = received_no_airmass['1m0a.doma.bpl'].timepoints

        received_airmass1 = make_dark_up_kernel_intervals(req_airmass1,
                                                          visibility_from,
                                                          True)
        timepoints_airmass1 = received_airmass1['1m0a.doma.bpl'].timepoints

        assert_not_equal(timepoints_no_airmass, timepoints_airmass1)
        assert_equal(len(timepoints_airmass1), 0)


class TestVisibility(object):
    '''Integration tests for rise_set - memoization interaction'''

    def setup(self):
        self.capella = {
                     'ra'                : RightAscension('05 16 41.36'),
                     'dec'               : Declination('+45 59 52.8'),
                     'epoch'             : 2000,
                   }
        self.bpl        = {
                          'latitude'  : Angle(degrees = 34.4332222222),
                          'longitude' : Angle(degrees = -119.863045833)
                          }
        self.start_date = datetime(year=2011, month=2, day=9)
        self.end_date   = datetime(year=2011, month=2, day=11)
        self.horizon    = 0
        self.twilight   = 'sunrise'

        self.visibility = Visibility(self.bpl, self.start_date, self.end_date,
                                     self.horizon, self.twilight)


    def test_memoize_preserves_correct_output_no_airmass(self):
        received = self.visibility.get_target_intervals(self.capella, up=True)
        memoized_func = Memoize(self.visibility.get_target_intervals)
        mem_received = memoized_func(self.capella, up=True)

        assert_equal(received, mem_received)


    def test_memoize_preserves_correct_output_with_airmass(self):
        received = self.visibility.get_target_intervals(self.capella, up=True,
                                                        airmass=2.0)
        memoized_func = Memoize(self.visibility.get_target_intervals)
        mem_received = memoized_func(self.capella, up=True, airmass=2.0)

        assert_equal(received, mem_received)


    def test_memoize_preserves_correct_output_with_differing_airmass(self):
        received = self.visibility.get_target_intervals(self.capella, up=True,
                                                        airmass=2.0)
        memoized_func = Memoize(self.visibility.get_target_intervals)
        mem_received = memoized_func(self.capella, up=True, airmass=1.0)

        assert_not_equal(received, mem_received)
