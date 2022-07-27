from adaptive_scheduler.simulation.metrics import (MetricCalculator,
                                                   bin_data)

import os
import json
import calendar
from datetime import datetime, timedelta
from mock import Mock
from rise_set import astrometry
from rise_set.sky_coordinates import RightAscension, Declination
from rise_set.angle import Angle
import numpy as np


class TestMetrics():

    def setup(self):
        self.start = datetime.strptime("2022-07-06T00:30", '%Y-%m-%dT%H:%M')
        self.end = self.start + timedelta(minutes=90)
        self.scheduler_run_time = datetime.utcnow()
        scheduler_result_attrs = {'resources_scheduled.return_value': ['bpl', 'coj']}
        res1 = Mock(duration=10, scheduled=True)
        res2 = Mock(duration=20, scheduled=True)
        res3 = Mock(duration=30, scheduled=True)
        res4 = Mock(scheduled=False)
        res5 = Mock(scheduled=False)
        fake_schedule = {'bpl': [res1, res2], 'coj': [res3]}
        fake_comp_res = Mock(reservation_list=[res1, res2, res3, res4, res5])
        self.mock_scheduler_result = Mock(input_reservations=[fake_comp_res], **scheduler_result_attrs)
        self.mock_scheduler = Mock(estimated_scheduler_end=self.scheduler_run_time)
        self.mock_scheduler_runner = Mock(semester_details={'start': self.start})
        self.mock_scheduler_runner.sched_params.horizon_days = 5
        self.mock_scheduler_runner.sched_params.simulate_now = self.scheduler_run_time

        self.mock_scheduler_result.schedule = fake_schedule

        self.mock_scheduler.visibility_cache = {'bpl': Mock(), 'coj': Mock()}
        self.mock_scheduler.visibility_cache['bpl'].dark_intervals = [
            (self.scheduler_run_time-timedelta(days=5), self.scheduler_run_time-timedelta(days=4)),
            (self.scheduler_run_time, self.scheduler_run_time+timedelta(days=1)),
            (self.scheduler_run_time+timedelta(days=2), self.scheduler_run_time+timedelta(days=3)),
        ]
        self.mock_scheduler.visibility_cache['coj'].dark_intervals = [
            (self.scheduler_run_time, self.scheduler_run_time+timedelta(days=2))]

        self.metrics = MetricCalculator(self.mock_scheduler_result,
                                        self.mock_scheduler_result,
                                        self.mock_scheduler,
                                        self.mock_scheduler_runner)

    def test_combining_schedules(self):
        scheduler_result_attrs = {'resources_scheduled.return_value': ['bpl', 'coj', 'ogg']}
        fake_schedule1 = {'bpl': ['hi', 'there'], 'coj': ['person']}
        fake_schedule2 = {'ogg': ['lco', 'rocks'], 'coj': ['woohoo!']}
        fake_input = [Mock(reservation_list=['foo', 'bar'])]
        mock_normal_scheduler_result = Mock(schedule=fake_schedule1, **scheduler_result_attrs)
        mock_normal_scheduler_result.input_reservations = fake_input
        mock_rr_scheduler_result = Mock(schedule=fake_schedule2, **scheduler_result_attrs)
        mock_rr_scheduler_result.input_reservations = fake_input

        only_normal = MetricCalculator(mock_normal_scheduler_result, None,
                                       self.mock_scheduler, self.mock_scheduler_runner)
        both_schedules = MetricCalculator(mock_normal_scheduler_result, mock_rr_scheduler_result,
                                          self.mock_scheduler, self.mock_scheduler_runner)
        same_schedule = MetricCalculator(mock_normal_scheduler_result, mock_normal_scheduler_result,
                                         self.mock_scheduler, self.mock_scheduler_runner)

        assert only_normal.combined_schedule == fake_schedule1
        assert both_schedules.combined_schedule == {'bpl': ['hi', 'there'],
                                                    'coj': ['woohoo!', 'person'],
                                                    'ogg': ['lco', 'rocks']}
        assert same_schedule.combined_schedule == fake_schedule1

    def test_percent_scheduled(self):
        assert self.metrics.percent_reservations_scheduled() == 60.

        scheduled_reservation = Mock(scheduled=True)
        unscheduled_reservation = Mock(scheduled=False)
        mock_schedule = {'bpl': [scheduled_reservation], 'coj': [scheduled_reservation, scheduled_reservation]}
        mock_scheduler_input = [unscheduled_reservation, scheduled_reservation, scheduled_reservation, scheduled_reservation]
        metrics2 = MetricCalculator(self.mock_scheduler_result,
                                    None,
                                    self.mock_scheduler,
                                    self.mock_scheduler_runner)
        metrics2.combined_schedule = mock_schedule
        metrics2.combined_input_reservations = mock_scheduler_input

        assert metrics2.percent_reservations_scheduled() == 75.

    def test_total_time_aggregators(self):
        seconds_in_day = 86400

        assert sum(self.metrics.get_duration_data()[0]) == 60
        assert self.metrics.total_available_seconds() == 4*seconds_in_day

    def test_bin_data(self):
        bin_by    = [1, 3, 4, 2, 6, 5, 3, 2, 3, 4, 7, 9, 3, 8, 6, 4]
        bin_data_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 8, 7, 6, 5, 4, 3, 2]
        bin_by_float = [0.5, 2.1, 2.8, 6.9, 1.8]
        bin_range = (1, 9)

        allparams = {'1-3': 7, '4-6': 6, '7-9': 3}
        defaults = {'1': 1, '2': 2, '3': 4, '4': 3, '5': 1, '6': 2, '7': 1, '8': 1, '9': 1}
        unevenbins = {'1-2': 3, '3-4': 7, '5-6': 3, '7-8': 2, '9': 1}
        floatbinsize = {'0.0-2.5': 3, '2.5-5.0': 7, '5.0-7.5': 4, '7.5-9.0': 2}
        floats = {'0.5-1.5': 1, '1.5-2.5': 2, '2.5-3.5': 1, '6.5-6.9': 1}
        capped_floats = {'0': 1, '1': 1, '2': 2}
        sumdata = {'1-3': 36, '4-6': 27, '7-9': 17}
        mindata = {'1-3': 1, '4-6': 2, '7-9': 4}

        assert bin_data(bin_by, bin_size=3, bin_range=bin_range, fill=None) == allparams
        assert bin_data(bin_by, fill=None) == defaults
        assert bin_data(bin_by, bin_size=2, fill=None) == unevenbins
        assert bin_data(bin_by, bin_size=2.5, bin_range=(0, 9), fill=None) == floatbinsize
        assert bin_data(bin_by_float, fill=None) == floats
        assert bin_data(bin_by_float, bin_range=(0, 4), fill=None) == capped_floats
        assert bin_data(bin_by, bin_data_, bin_size=3, fill=None, aggregator=sum) == sumdata
        assert bin_data(bin_by, bin_data_, bin_size=3, aggregator=min) == mindata

    def test_airmass_functions(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        data_path_1 = os.path.join(dir_path, 'airmass_data.json')
        data_path_2 = os.path.join(dir_path, 'airmass_data_2.json')
        with open(data_path_1) as f:
            airmass_data_1 = json.load(f)
        with open(data_path_2) as f:
            airmass_data_2 = json.load(f)
        self.metrics._get_airmass_data_for_request = Mock(side_effect=[airmass_data_1, airmass_data_2])
        request_1 = Mock(id=1)
        mock_reservation_1 = Mock(scheduled_start=0, scheduled_resource='1m0a.doma.tfn',
                                  request=request_1, duration=2400)
        request_2 = Mock(id=2)
        mock_reservation_2 = Mock(scheduled_start=0, scheduled_resource='1m0a.doma.egg',
                                  request=request_2, duration=2400)
        scheduled_reservations = [mock_reservation_1, mock_reservation_2]
        schedule = {'reservations': scheduled_reservations}
        midpoint_time = self.start + timedelta(seconds=mock_reservation_1.duration/2)
        midpoint_duration = timedelta(seconds=mock_reservation_1.duration/2)

        assert self.metrics._get_midpoint_airmasses_by_site(airmass_data_1, midpoint_time) == {'tfn': 5, 'egg': 4}
        assert self.metrics._get_minmax_airmass(airmass_data_1, midpoint_duration) == (1, 20)

        airmass_metrics = self.metrics.airmass_metrics(schedule)
        midpoint_airmasses = [5, 3]
        assert type(airmass_metrics) is dict
        assert airmass_metrics['avg_midpoint_airmass'] == 4
        assert airmass_metrics['avg_min_poss_airmass'] == 1
        assert airmass_metrics['raw_airmass_data'][0]['midpoint_airmasses'] == midpoint_airmasses

    def test_avg_slew_distance(self):
        conf1 = {'target': Mock(name='star1',
                                ra=RightAscension(degrees=35),
                                dec=Declination(degrees=0))}
        conf2 = {'target': Mock(name='star2',
                                ra=RightAscension(degrees=35),
                                dec=Declination(degrees=15))}
        conf3 = {'target': Mock(name='star3',
                                ra=RightAscension(degrees=10),
                                dec=Declination(degrees=15))}
        conf4 = {'target': Mock(name='star4',
                                ra=RightAscension(degrees=60),
                                dec=Declination(degrees=10))}
        conf5 = {'target': Mock(name='star5',
                                ra=RightAscension(degrees=80),
                                dec=Declination(degrees=10))}
        conf6 = {'target': Mock(name='star6',
                                ra=RightAscension(degrees=80),
                                dec=Declination(degrees=-10))}
        res1 = Mock(scheduled_start=10)
        res2 = Mock(scheduled_start=20)
        res3 = Mock(scheduled_start=10)
        res4 = Mock(scheduled_start=20)
        res5 = Mock(scheduled_start=30)
        res1.request.configurations = [conf1, conf2]
        res2.request.configurations = [conf3]
        res3.request.configurations = [conf4]
        res4.request.configurations = [conf5, conf5, conf5]
        res5.request.configurations = [conf6]
        fake_schedule1 = {'bpl': [res1, res2], 'coj': [res5, res4, res3]}
        d = timedelta(seconds=10)
        radec1 = astrometry.mean_to_apparent({'ra': Angle(degrees=35), 'dec': Angle(degrees=0)},
                                             astrometry.date_to_tdb(self.scheduler_run_time+d))
        radec2 = astrometry.mean_to_apparent({'ra': Angle(degrees=35), 'dec': Angle(degrees=15)},
                                             astrometry.date_to_tdb(self.scheduler_run_time+d))
        radec3 = astrometry.mean_to_apparent({'ra': Angle(degrees=10), 'dec': Angle(degrees=15)},
                                             astrometry.date_to_tdb(self.scheduler_run_time+2*d))
        radec4 = astrometry.mean_to_apparent({'ra': Angle(degrees=60), 'dec': Angle(degrees=10)},
                                             astrometry.date_to_tdb(self.scheduler_run_time+3*d))
        radec5 = astrometry.mean_to_apparent({'ra': Angle(degrees=80), 'dec': Angle(degrees=10)},
                                             astrometry.date_to_tdb(self.scheduler_run_time+4*d))
        radec6 = astrometry.mean_to_apparent({'ra': Angle(degrees=80), 'dec': Angle(degrees=-10)},
                                             astrometry.date_to_tdb(self.scheduler_run_time+5*d))
        slewdists = [astrometry.angular_distance_between(*radec1, *radec2),
                     astrometry.angular_distance_between(*radec2, *radec3),
                     astrometry.angular_distance_between(*radec4, *radec5),
                     astrometry.angular_distance_between(*radec5, *radec5),
                     astrometry.angular_distance_between(*radec5, *radec5),
                     astrometry.angular_distance_between(*radec5, *radec6)]
        slewdists = [a.in_degrees() for a in slewdists]
        metrics = MetricCalculator(self.mock_scheduler_result, None, self.mock_scheduler, self.mock_scheduler_runner)
        metrics.combined_schedule = fake_schedule1
        metrics.scheduler_runner.semester_details['start'] = self.scheduler_run_time

        assert np.isclose(metrics.avg_slew_distance(), np.mean(slewdists))
