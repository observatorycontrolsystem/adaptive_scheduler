#!/usr/bin/python
from __future__ import division

import copy

from adaptive_scheduler.models import (ICRSTarget, Request, Proposal,
                                       RequestGroup, Window, Windows, Configuration)
from adaptive_scheduler.utils import (datetime_to_epoch, normalise_datetime_intervals,
                                      normalised_epoch_to_datetime)
from time_intervals.intervals import Intervals
from adaptive_scheduler.kernel_mappings import (construct_visibilities,
                                                construct_compound_reservation,
                                                construct_many_compound_reservation,
                                                construct_global_availability,
                                                filter_on_scheduling_horizon,
                                                compute_request_availability,
                                                get_rise_set_timepoint_intervals,
                                                make_cache_key)
from datetime import datetime

import pytest


class TestKernelMappings(object):

    def setup(self):
        self.start = datetime(2011, 11, 1, 0, 0, 0)
        self.end = datetime(2011, 11, 3, 0, 0, 0)

        self.tels = {
            '1m0a.doma.bpl':
                dict(
                    name='1m0a.doma.bpl',
                    tel_class='1m0',
                    latitude=34.433157,
                    longitude=-119.86308,
                    horizon=25,
                    ha_limit_neg=-12.0,
                    ha_limit_pos=12.0,
                    zenith_blind_spot=0.0
                )
        }

        self.target = ICRSTarget(
            # ra  = '20 41 25.91',
            # dec = '+45 16 49.22',
            ra=310.35795833333333,
            dec=45.280338888888885
        )

        self.prop_mot_target = ICRSTarget(
            # ra  = '20 41 25.91',
            # dec = '+45 16 49.22',
            ra=316.73026646,
            dec=38.74205644,
            proper_motion_ra=4106.90,
            proper_motion_dec=3144.68
        )

        self.instrument_config = dict(
            exposure_count=1,
            bin_x=2,
            bin_y=2,
            exposure_time=30,
            optical_elements={'filter': 'B'}
        )

        self.guiding_config = dict(
            mode='ON',
            optional=True,
            optical_elements={},
            exposure_time=10
        )

        self.acquisition_config = dict(
            mode='OFF'
        )

        self.constraints = {'max_airmass': None,
                            'min_lunar_distance': 0,
                            'max_lunar_phase': 1.0}

        self.configuration = Configuration(
            dict(
                id=5,
                target=self.target,
                instrument_type='1M0-SCICAM-SINISTRO',
                type='expose',
                instrument_configs=[self.instrument_config],
                guiding_config=self.guiding_config,
                acquisition_config=self.acquisition_config,
                constraints=self.constraints
            )
        )

    def make_constrained_request(self, airmass=None, max_lunar_phase=1.0,
                                 start=datetime(2011, 11, 1, 6, 0, 0),
                                 end=datetime(2011, 11, 2, 6, 0, 0)):
        # A one day user supplied window
        window_dict = {
            'start': start,
            'end': end
        }
        resource_name = '1m0a.doma.bpl'
        resource = self.tels[resource_name]

        window = Window(window_dict, resource['name'])
        dt_windows = Windows()
        dt_windows.append(window)

        configuration = copy.deepcopy(self.configuration)
        configuration.constraints['max_airmass'] = airmass
        configuration.constraints['max_lunar_phase'] = max_lunar_phase

        req = Request(
            configurations=[configuration],
            windows=dt_windows,
            request_id=1,
            duration=10
        )

        return req

    def make_request_group(self, requests, operator='single'):
        proposal = Proposal({'id': 'TestProposal', 'tag': 'Test Proposal', 'pi': '', 'tac_priority': 10})
        rg = RequestGroup(operator=operator, requests=requests, proposal=proposal, submitter='',
                          expires=datetime(2999, 1, 1), rg_id=1, is_staff=False, name='test group id', ipp_value=1.0,
                          observation_type='NORMAL')

        return rg

    def make_intersection_dict(self):
        timepoints = [
            {
                'time': datetime(2011, 11, 1, 6, 0, 0),
                'type': 'start'
            },
            {
                'time': datetime(2011, 11, 1, 7, 0, 0),
                'type': 'end'
            },
        ]
        intervals = Intervals(timepoints)

        intersection_dict = {
            '1m0a.doma.coj': intervals
        }

        return intersection_dict

    def make_dt_intervals_list(self):
        dt_intervals_list = [
            self.make_intersection_dict(),
            self.make_intersection_dict(),
        ]

        return dt_intervals_list

    def make_rise_set_intervals(self, req, visibilities):
        intervals_for_resource = {}
        for configuration in req.configurations:
            rs_target = configuration.target.in_rise_set_format()
            max_airmass = configuration.constraints['max_airmass']
            min_lunar_distance = configuration.constraints['min_lunar_distance']
            max_lunar_phase = configuration.constraints['max_lunar_phase']
            for resource, visibility in visibilities.items():
                intervals = get_rise_set_timepoint_intervals(rs_target, visibility, max_airmass,
                                                             min_lunar_distance, max_lunar_phase)
                if resource in intervals_for_resource:
                    intervals_for_resource[resource] = intervals_for_resource[resource].intersect(intervals)
                else:
                    intervals_for_resource[resource] = intervals

        return intervals_for_resource

    def test_make_cache_key(self):
        max_airmass = 2.5
        min_lunar_distance = 30.0
        max_lunar_phase = 0.75
        resource = '1m0a.doma.lsc'
        rs_target = self.make_constrained_request().configurations[0].target.in_rise_set_format()

        assert (make_cache_key(resource, rs_target, max_airmass, min_lunar_distance, max_lunar_phase) ==
                     '{}_{}_{}_{}_{}'.format(resource, max_airmass, min_lunar_distance, max_lunar_phase, repr(sorted(rs_target.items()))))

    def test_compute_request_availability_lunar_phase_removes_window(self):
        request = self.make_constrained_request(max_lunar_phase=1.0)
        resource = '1m0a.doma.bpl'
        visibilities = construct_visibilities(self.tels, self.start, self.end)

        intervals_for_resource = self.make_rise_set_intervals(request, visibilities)
        compute_request_availability(request, intervals_for_resource, {})
        base_windows = request.windows.windows_for_resource.copy()

        # Lunar phase goes above 40 at ~16:30 on 11/1, so it should eliminate the second window
        constrainted_request = self.make_constrained_request(max_lunar_phase=0.4)
        intervals_for_resource = self.make_rise_set_intervals(constrainted_request, visibilities)
        compute_request_availability(constrainted_request, intervals_for_resource, {})

        assert len(base_windows[resource]) == 2
        assert constrainted_request.windows.size() == 1
        # The window below lunar phase of 40% is the first of its two windows
        assert constrainted_request.windows.at(resource)[0] == base_windows[resource][0]

    def test_compute_request_availability_half_downtime(self):
        request = self.make_constrained_request()
        resource = '1m0a.doma.bpl'
        visibilities = construct_visibilities(self.tels, self.start, self.end)
        downtime_intervals = {resource: {'all': [(datetime(2011, 11, 1, 5), datetime(2011, 11, 1, 8)), ]}}

        intervals_for_resource = self.make_rise_set_intervals(request, visibilities)
        compute_request_availability(request, intervals_for_resource, {})
        base_windows = request.windows.windows_for_resource.copy()

        compute_request_availability(request, intervals_for_resource, downtime_intervals)
        assert len(base_windows[resource]) == 2
        assert request.windows.size() == 1
        assert request.windows.at(resource)[0] == base_windows[resource][1]

    def test_compute_request_availability_full_downtime(self):
        request = self.make_constrained_request()
        resource = '1m0a.doma.bpl'
        visibilities = construct_visibilities(self.tels, self.start, self.end)
        downtime_intervals = {resource: {'all': [(datetime(2011, 11, 1), datetime(2011, 11, 3)), ]}}

        intervals_for_resource = self.make_rise_set_intervals(request, visibilities)
        compute_request_availability(request, intervals_for_resource, {})
        base_windows = request.windows.windows_for_resource.copy()

        compute_request_availability(request, intervals_for_resource, downtime_intervals)
        assert len(base_windows[resource]) == 2
        assert request.windows.size() == 0

    def test_compute_request_availability_half_downtime_instrument_type(self):
        request = self.make_constrained_request()
        resource = '1m0a.doma.bpl'
        instrument_type = request.configurations[0].instrument_type
        visibilities = construct_visibilities(self.tels, self.start, self.end)
        downtime_intervals = {resource: {instrument_type: [(datetime(2011, 11, 1, 5), datetime(2011, 11, 1, 8)), ]}}

        intervals_for_resource = self.make_rise_set_intervals(request, visibilities)
        compute_request_availability(request, intervals_for_resource, {})
        base_windows = request.windows.windows_for_resource.copy()

        compute_request_availability(request, intervals_for_resource, downtime_intervals)
        assert len(base_windows[resource]) == 2
        assert request.windows.size() == 1
        assert request.windows.at(resource)[0] == base_windows[resource][1]

    def test_compute_request_availability_combined_full_downtime(self):
        request = self.make_constrained_request()
        resource = '1m0a.doma.bpl'
        instrument_type = request.configurations[0].instrument_type
        visibilities = construct_visibilities(self.tels, self.start, self.end)
        downtime_intervals = {resource: {
            'all': [(datetime(2011, 11, 1, 5), datetime(2011, 11, 1, 8)), ],
            instrument_type: [(datetime(2011, 11, 1, 8), datetime(2011, 11, 3)), ]
        }}

        intervals_for_resource = self.make_rise_set_intervals(request, visibilities)
        compute_request_availability(request, intervals_for_resource, {})
        base_windows = request.windows.windows_for_resource.copy()

        compute_request_availability(request, intervals_for_resource, downtime_intervals)
        assert len(base_windows[resource]) == 2
        assert request.windows.size() == 0

    def test_compute_request_availability_different_instrument_downtime(self):
        request = self.make_constrained_request()
        resource = '1m0a.doma.bpl'
        visibilities = construct_visibilities(self.tels, self.start, self.end)
        downtime_intervals = {resource: {'not_my_inst': [(datetime(2011, 11, 1), datetime(2011, 11, 3)), ]}}

        intervals_for_resource = self.make_rise_set_intervals(request, visibilities)
        compute_request_availability(request, intervals_for_resource, {})
        base_windows = request.windows.windows_for_resource.copy()

        compute_request_availability(request, intervals_for_resource, downtime_intervals)
        assert len(base_windows[resource]) == 2
        assert request.windows.size() == 2

    def test_construct_compound_reservation(self):
        request = self.make_constrained_request()
        requests = [request, request]
        operator = 'and'
        request_group = self.make_request_group(requests, operator)
        sem_start = self.start

        # TODO: Replace with cleaner mock patching
        request_group.proposal.tac_priority = 1

        received = construct_compound_reservation(request_group,
                                                  sem_start, {})

        assert len(received.reservation_list) == len(requests)
        assert received.type == operator

    def test_construct_many_compound_reservation(self):
        request = self.make_constrained_request()
        requests = [request, request]
        operator = 'many'
        request_group = self.make_request_group(requests, operator)
        sem_start = self.start

        # TODO: Replace with cleaner mock patching
        request_group.proposal.tac_priority = 1

        received = construct_many_compound_reservation(
            request_group,
            0,
            sem_start,
            {})

        assert len(received.reservation_list) == 1
        assert received.type == 'single'

    def test_filter_on_scheduling_horizon_applies_horizon_to_singles(self):
        start = datetime(2011, 11, 1, 6, 0, 0)
        end = datetime(2011, 12, 1, 6, 0, 0)
        request = self.make_constrained_request(start=start, end=end)
        operator = 'single'
        requests = [request]
        request_group = self.make_request_group(requests, operator)
        request_groups = [request_group]
        scheduling_horizon = datetime(2011, 11, 15, 6, 0, 0)
        filtered_rgs = filter_on_scheduling_horizon(request_groups,
                                                    scheduling_horizon)

        expected_window_start = start
        expected_window_end = scheduling_horizon

        assert 1 == len(filtered_rgs)
        output_ur = filtered_rgs[0]
        assert 1 == len(output_ur.requests)
        bpl_1m0a_doma_windows = output_ur.requests[0].windows.at('1m0a.doma.bpl')
        assert 1 == len(bpl_1m0a_doma_windows)
        assert bpl_1m0a_doma_windows[0].start == expected_window_start
        assert bpl_1m0a_doma_windows[0].end == expected_window_end

    def test_filter_on_scheduling_horizon_applies_horizon_to_manys(self):
        start = datetime(2011, 11, 1, 6, 0, 0)
        end = datetime(2011, 12, 1, 6, 0, 0)
        request1 = self.make_constrained_request(start=start, end=end)
        request2 = self.make_constrained_request(start=start, end=end)
        operator = 'many'
        requests = [request1, request2]
        request_group = self.make_request_group(requests, operator)
        request_groups = [request_group]
        scheduling_horizon = datetime(2011, 11, 15, 6, 0, 0)
        filtered_rgs = filter_on_scheduling_horizon(request_groups,
                                                    scheduling_horizon)

        expected_window_start = start
        expected_window_end = scheduling_horizon

        assert 1 == len(filtered_rgs)
        output_ur = filtered_rgs[0]
        assert 2 == len(output_ur.requests)
        bpl_1m0a_doma_windows1 = output_ur.requests[0].windows.at('1m0a.doma.bpl')
        bpl_1m0a_doma_windows2 = output_ur.requests[0].windows.at('1m0a.doma.bpl')
        assert 1 == len(bpl_1m0a_doma_windows1)
        assert 1 == len(bpl_1m0a_doma_windows2)
        assert bpl_1m0a_doma_windows1[0].start == expected_window_start
        assert bpl_1m0a_doma_windows1[0].end == expected_window_end
        assert bpl_1m0a_doma_windows2[0].start == expected_window_start
        assert bpl_1m0a_doma_windows2[0].end == expected_window_end

    def test_filter_on_scheduling_horizon_no_horizon_applied_to_oneof(self):
        start = datetime(2011, 11, 1, 6, 0, 0)
        end = datetime(2011, 12, 1, 6, 0, 0)
        request1 = self.make_constrained_request(start=start, end=end)
        request2 = self.make_constrained_request(start=start, end=end)
        operator = 'oneof'
        requests = [request1, request2]
        request_group = self.make_request_group(requests, operator)
        request_groups = [request_group]
        scheduling_horizon = datetime(2011, 11, 15, 6, 0, 0)
        filtered_rgs = filter_on_scheduling_horizon(request_groups,
                                                    scheduling_horizon)

        expected_window_start = start
        expected_window_end = end

        assert 1 == len(filtered_rgs)
        output_ur = filtered_rgs[0]
        assert 2 == len(output_ur.requests)
        bpl_1m0a_doma_windows1 = output_ur.requests[0].windows.at('1m0a.doma.bpl')
        bpl_1m0a_doma_windows2 = output_ur.requests[0].windows.at('1m0a.doma.bpl')
        assert 1 == len(bpl_1m0a_doma_windows1)
        assert 1 == len(bpl_1m0a_doma_windows2)
        assert bpl_1m0a_doma_windows1[0].start == expected_window_start
        assert bpl_1m0a_doma_windows1[0].end == expected_window_end
        assert bpl_1m0a_doma_windows2[0].start == expected_window_start
        assert bpl_1m0a_doma_windows2[0].end == expected_window_end

    def test_filter_on_scheduling_horizon_no_horizon_applied_to_and(self):
        start = datetime(2011, 11, 1, 6, 0, 0)
        end = datetime(2011, 12, 1, 6, 0, 0)
        request1 = self.make_constrained_request(start=start, end=end)
        request2 = self.make_constrained_request(start=start, end=end)
        operator = 'and'
        requests = [request1, request2]
        request_group = self.make_request_group(requests, operator)
        request_groups = [request_group]
        scheduling_horizon = datetime(2011, 11, 15, 6, 0, 0)
        filtered_rgs = filter_on_scheduling_horizon(request_groups,
                                                    scheduling_horizon)

        expected_window_start = start
        expected_window_end = end

        assert 1 == len(filtered_rgs)
        output_ur = filtered_rgs[0]
        assert 2 == len(output_ur.requests)
        bpl_1m0a_doma_windows1 = output_ur.requests[0].windows.at('1m0a.doma.bpl')
        bpl_1m0a_doma_windows2 = output_ur.requests[0].windows.at('1m0a.doma.bpl')
        assert 1 == len(bpl_1m0a_doma_windows1)
        assert 1 == len(bpl_1m0a_doma_windows2)
        assert bpl_1m0a_doma_windows1[0].start == expected_window_start
        assert bpl_1m0a_doma_windows1[0].end == expected_window_end
        assert bpl_1m0a_doma_windows2[0].start == expected_window_start
        assert bpl_1m0a_doma_windows2[0].end == expected_window_end

    def test_make_target_intervals(self):
        window_dict = {
            'start': self.start,
            'end': self.end
        }
        resource_name = '1m0a.doma.bpl'
        resource = self.tels[resource_name]

        window = Window(window_dict, resource['name'])
        dt_windows = Windows()
        dt_windows.append(window)

        req = Request(
            configurations=[self.configuration],
            windows=dt_windows,
            request_id='1'
        )

        visibilities = construct_visibilities(self.tels, self.start, self.end)

        intervals_for_resource = self.make_rise_set_intervals(req, visibilities)
        compute_request_availability(req, intervals_for_resource, {})
        received = req.windows.to_window_intervals()

        date_format = '%Y-%m-%d %H:%M:%S.%f'
        rise_set_dark_intervals = (
            datetime.strptime('2011-11-01 02:02:43.257196', date_format),
            datetime.strptime('2011-11-01 07:52:00.564199', date_format),
            datetime.strptime('2011-11-02 02:01:50.423880', date_format),
            datetime.strptime('2011-11-02 07:48:04.692316', date_format)
        )

        # Verify we get the intervals we expect
        for resource_name, received_intervals in received.items():
            for i, received_tp in enumerate(received_intervals.toDictList()):
                assert received_tp['time'] == rise_set_dark_intervals[i]

    def test_proper_motion_in_rise_set(self):
        target_dict = self.prop_mot_target.in_rise_set_format()

        # According to Rob's calculations, proper motion RA and dec should be as follows
        # See https://issues.lcogt.net/issues/8723 for more info
        converted_proper_motion_ra = 5.265450459478893
        converted_proper_motion_dec = 3.14468
        assert target_dict['ra_proper_motion'].in_degrees_per_year() == pytest.approx(converted_proper_motion_ra / 3600.0)
        assert target_dict['dec_proper_motion'].in_degrees_per_year() == pytest.approx(converted_proper_motion_dec / 3600.0)

    def test_user_interval_is_honoured(self):
        # A one day user supplied window
        window_dict = {
            'start': datetime(2011, 11, 1, 6, 0, 0),
            'end': datetime(2011, 11, 2, 6, 0, 0)
        }
        resource_name = '1m0a.doma.bpl'
        resource = self.tels[resource_name]

        window = Window(window_dict, resource['name'])
        dt_windows = Windows()
        dt_windows.append(window)

        req = Request(
            configurations=[self.configuration],
            windows=dt_windows,
            request_id='1'
        )

        visibilities = construct_visibilities(self.tels, self.start, self.end)

        intervals_for_resource = self.make_rise_set_intervals(req, visibilities)
        compute_request_availability(req, intervals_for_resource, {})
        received = req.windows.to_window_intervals()

        # The user windows constrain the available observing windows (compare to
        # previous test)
        date_format = '%Y-%m-%d %H:%M:%S.%f'
        rise_set_dark_intervals = (
            datetime.strptime('2011-11-01 06:00:00.0', date_format),
            datetime.strptime('2011-11-01 07:52:00.564199', date_format),
            datetime.strptime('2011-11-02 02:01:50.423880', date_format),
            datetime.strptime('2011-11-02 06:00:00.0', date_format),
        )

        # Verify we get the intervals we expect
        for resource_name, received_intervals in received.items():
            for i, received_tp in enumerate(received_intervals.toDictList()):
                assert received_tp['time'] == rise_set_dark_intervals[i]

    def test_multiple_user_intervals_are_honoured(self):
        # A one day user supplied window
        windows = [
            {
                'start': datetime(2011, 11, 1, 6, 0, 0),
                'end': datetime(2011, 11, 1, 9, 0, 0)
            },
            {
                'start': datetime(2011, 11, 2, 1, 0, 0),
                'end': datetime(2011, 11, 2, 4, 0, 0)
            }
        ]

        dt_windows = Windows()
        resource_name = '1m0a.doma.bpl'
        for w in windows:
            dt_windows.append(Window(w, self.tels[resource_name]['name']))

        req = Request(
            configurations=[self.configuration],
            windows=dt_windows,
            request_id='1'
        )

        visibilities = construct_visibilities(self.tels, self.start, self.end)

        intervals_for_resource = self.make_rise_set_intervals(req, visibilities)
        compute_request_availability(req, intervals_for_resource, {})
        received = req.windows.to_window_intervals()

        # The user windows constrain the available observing windows (compare to
        # previous tests)
        date_format = '%Y-%m-%d %H:%M:%S.%f'
        rise_set_dark_intervals = (
            datetime.strptime('2011-11-01 06:00:00.0', date_format),
            datetime.strptime('2011-11-01 07:52:00.564199', date_format),
            datetime.strptime('2011-11-02 02:01:50.423880', date_format),
            datetime.strptime('2011-11-02 04:00:00.0', date_format),
        )

        # Verify we get the intervals we expect
        for resource_name, received_intervals in received.items():
            for i, received_tp in enumerate(received_intervals.toDictList()):
                assert received_tp['time'] == rise_set_dark_intervals[i]

    def test_visibility_intervals_are_limited_by_hour_angle(self):

        window_dict = {
            'start': datetime(2013, 3, 22, 0, 0, 0),
            'end': datetime(2013, 3, 23, 0, 0, 0),
        }

        tel_name = '1m0a.doma.coj'
        tel = dict(
            name=tel_name,
            tel_class='1m0',
            latitude=-31.273,
            longitude=149.070593,
            horizon=15,
            ha_limit_neg=-4.6,
            ha_limit_pos=4.6,
            zenith_blind_spot=0.0
        )

        tels = {
            tel_name: tel,
        }

        target = ICRSTarget(
            ra=310.35795833333333,
            dec=-60.0,
        )

        window = Window(window_dict, tel['name'])
        dt_windows = Windows()
        dt_windows.append(window)

        configuration = copy.deepcopy(self.configuration)
        configuration.target = target

        req = Request(
            configurations=[configuration],
            windows=dt_windows,
            request_id='1',
            duration=10,
        )
        sem_start = datetime(2013, 3, 1, 0, 0, 0)
        sem_end = datetime(2013, 3, 31, 0, 0, 0)

        visibilities = construct_visibilities(tels, sem_start, sem_end)

        intervals_for_resource = self.make_rise_set_intervals(req, visibilities)
        compute_request_availability(req, intervals_for_resource, {})
        received = req.windows.to_window_intervals()

        # Hour angle not violated independently confirmed by hand-cranking through SLALIB
        expected_tps = [
            {
                'type': 'start',
                'time': datetime(2013, 3, 22, 18, 8, 34, 287629)
            },
            {
                'type': 'end',
                'time': datetime(2013, 3, 22, 19, 16, 27, 292072)
            },
        ]

        for received_tp, expected_tp in zip(received[tel_name].toDictList(), expected_tps):
            assert received_tp['type'] == expected_tp['type']
            assert received_tp['time'] == expected_tp['time']

    def test_visibility_intervals_at_low_horizon_are_allowed_by_hour_angle(self):

        window_dict = {
            'start': datetime(2013, 3, 22, 0, 0, 0),
            'end': datetime(2013, 3, 23, 0, 0, 0),
        }

        tel_name = '1m0a.doma.coj'
        tel = dict(
            name=tel_name,
            tel_class='1m0',
            latitude=-31.273,
            longitude=149.070593,
            horizon=15,
            ha_limit_neg=-4.6,
            ha_limit_pos=4.6,
            zenith_blind_spot=0.0
        )

        tels = {
            tel_name: tel,
        }

        target = ICRSTarget(
            # RA 15:41:25.91
            ra=235.357958333,
            dec=-60.0,
        )

        window = Window(window_dict, tel['name'])
        dt_windows = Windows()
        dt_windows.append(window)

        configuration = copy.deepcopy(self.configuration)
        configuration.target = target

        req = Request(
            configurations=[configuration],
            windows=dt_windows,
            request_id='1',
            duration=10,
        )
        sem_start = datetime(2013, 3, 1, 0, 0, 0)
        sem_end = datetime(2013, 3, 31, 0, 0, 0)

        visibilities = construct_visibilities(tels, sem_start, sem_end)

        intervals_for_resource = self.make_rise_set_intervals(req, visibilities)
        compute_request_availability(req, intervals_for_resource, {})
        received = req.windows.to_window_intervals()

        # Hour angle not violated independently confirmed by hand-cranking through SLALIB
        expected_tps = [
            {
                'type': 'start',
                'time': datetime(2013, 3, 22, 13, 9, 28, 988253)
            },
            {
                'type': 'end',
                'time': datetime(2013, 3, 22, 19, 16, 27, 292072)
            },
        ]

        for received_tp, expected_tp in zip(received[tel_name].toDictList(), expected_tps):
            assert received_tp['type'] == expected_tp['type']
            assert received_tp['time'] == expected_tp['time']

    def test_construct_global_availability(self):
        tel_name = '1m0a.doma.lsc'
        sem_start = datetime(2012, 10, 1)

        # Resource is available from 3-7
        dt0 = datetime(2013, 3, 22, 3)
        dt1 = datetime(2013, 3, 22, 7)

        dt_resource_int = Intervals(
            [
                {'time': dt0, 'type': 'start'},
                {'time': dt1, 'type': 'end'},
            ]
        )
        epoch_resource_int = normalise_datetime_intervals(dt_resource_int, sem_start)
        resource_windows = {
            tel_name: epoch_resource_int
        }

        # Resource is unavailable from 4-5
        dt2 = datetime(2013, 3, 22, 4)
        dt3 = datetime(2013, 3, 22, 5)
        masked_inervals = {
            '1m0a.doma.lsc': Intervals([{'time': dt2, 'type': 'start'},
                                        {'time': dt3, 'type': 'end'}])
        }

        # Expected available intervals after masking are
        # 3-4, 5-7
        received = construct_global_availability(masked_inervals, sem_start,
                                                 resource_windows)
        received_int = received[tel_name]
        timepoints = received_int.toDictList()
        assert len(timepoints) == 4
        r0 = normalised_epoch_to_datetime(timepoints[0]['time'],
                                          datetime_to_epoch(sem_start))
        r1 = normalised_epoch_to_datetime(timepoints[1]['time'],
                                          datetime_to_epoch(sem_start))
        # r2 = normalised_epoch_to_datetime(timepoints[2]['time'],
        #                                   datetime_to_epoch(sem_start))
        r3 = normalised_epoch_to_datetime(timepoints[3]['time'],
                                          datetime_to_epoch(sem_start))
        assert r0 == dt0
        assert r1 == dt2
        assert r3 == dt1

    def test_airmass_is_honoured_high_airmass(self):
        airmass = 3.0
        req_airmass3 = self.make_constrained_request(airmass)
        req_no_airmass = self.make_constrained_request()

        visibilities = construct_visibilities(self.tels, self.start, self.end)

        intervals_for_resource = self.make_rise_set_intervals(req_no_airmass, visibilities)
        compute_request_availability(req_no_airmass, intervals_for_resource, {})
        received_no_airmass = req_no_airmass.windows.to_window_intervals()
        timepoints_no_airmass = received_no_airmass['1m0a.doma.bpl'].toDictList()

        intervals_for_resource = self.make_rise_set_intervals(req_airmass3, visibilities)
        compute_request_availability(req_airmass3, intervals_for_resource, {})
        received_airmass3 = req_airmass3.windows.to_window_intervals()
        timepoints_airmass3 = received_airmass3['1m0a.doma.bpl'].toDictList()

        assert timepoints_no_airmass == timepoints_airmass3

    def test_airmass_is_honoured_low_airmass(self):
        airmass = 1.0
        req_airmass1 = self.make_constrained_request(airmass)
        req_no_airmass = self.make_constrained_request()

        visibilities = construct_visibilities(self.tels, self.start, self.end)

        intervals_for_resource = self.make_rise_set_intervals(req_no_airmass, visibilities)
        compute_request_availability(req_no_airmass, intervals_for_resource, {})
        received_no_airmass = req_no_airmass.windows.to_window_intervals()

        intervals_for_resource = self.make_rise_set_intervals(req_airmass1, visibilities)
        compute_request_availability(req_airmass1, intervals_for_resource, {})
        received_airmass1 = req_airmass1.windows.to_window_intervals()

        assert received_airmass1 != received_no_airmass
        assert len(received_airmass1) == 0
