from adaptive_scheduler.simulation.metrics import (MetricCalculator,
                                                   fill_bin_with_reservation_data)
from adaptive_scheduler.models import DataContainer

import os
import json
from datetime import datetime, timedelta
from mock import Mock, patch


class TestMetrics():

    def setup(self):
        self.start = datetime.strptime("2022-07-06T00:30", '%Y-%m-%dT%H:%M')
        self.end = self.start + timedelta(minutes=90)
        self.scheduler_run_time = datetime.utcnow()
        scheduler_result_attrs = {'resources_scheduled.return_value': ['bpl', 'coj']}
        self.mock_scheduler_result = Mock(**scheduler_result_attrs)
        self.mock_scheduler = Mock(estimated_scheduler_end=self.scheduler_run_time)
        self.mock_scheduler_runner = Mock(semester_details={'start': self.start})
        self.mock_scheduler_runner.sched_params.metric_effective_horizon = 5
        
        # self.mock_scheduler_runner = start
        res1 = Mock(duration=10)
        res2 = Mock(duration=20)
        res3 = Mock(duration=30)
        fake_schedule = {'bpl': [res1, res2], 'coj': [res3]}
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
        mock_normal_scheduler_result = Mock(schedule=fake_schedule1, **scheduler_result_attrs)
        mock_rr_scheduler_result = Mock(schedule=fake_schedule2, **scheduler_result_attrs)

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
        scheduled_reservation = Mock(scheduled=True)
        unscheduled_reservation = Mock(scheduled=False)

        all_scheduled = {'bpl': [scheduled_reservation]}
        half_scheduled = {'bpl': [scheduled_reservation, unscheduled_reservation]}
        none_scheduled = {'bpl': [unscheduled_reservation]}
        multiple_sites = {'bpl': [scheduled_reservation, unscheduled_reservation],
                          'coj': [scheduled_reservation, scheduled_reservation]}

        assert self.metrics.percent_reservations_scheduled(all_scheduled) == 100.
        assert self.metrics.percent_reservations_scheduled(half_scheduled) == 50.
        assert self.metrics.percent_reservations_scheduled(none_scheduled) == 0.
        assert self.metrics.percent_reservations_scheduled(multiple_sites) == 75.

    def test_total_time_aggregators(self):
        seconds_in_day = 86400

        assert self.metrics.total_scheduled_seconds(self.mock_scheduler_result.schedule) == 60
        assert self.metrics.total_available_seconds(['bpl', 'coj'], 0) == 0
        assert self.metrics.total_available_seconds(['bpl', 'coj'], 1) == 2*seconds_in_day
        assert self.metrics.total_available_seconds(['bpl', 'coj'], 5) == 4*seconds_in_day
        assert self.metrics.total_available_seconds(['bpl'], 1) == seconds_in_day
        assert self.metrics.total_available_seconds([], 1) == 0
        assert self.metrics.total_scheduled_seconds() == 60
        assert self.metrics.total_available_seconds() == 4*seconds_in_day

    def test_percent_time_utilization(self):
        test_schedule = {'bpl': [Mock(duration=86400)]}
        assert self.metrics.percent_time_utilization(test_schedule, ['bpl'], 1) == 100.
        assert self.metrics.percent_time_utilization() == 60/(86400*4)*100

    # def test_fill_bin_with_reservation_data(self):
    #     data_dict = {}
    #     start_time = datetime.utcnow()

    #     mock_reservation = Mock(
    #         duration=10,
    #         scheduled_resource='bpl',
    #         scheduled_start=start_time,
    #         scheduled=True,
    #     )
    #     mock_reservation.request_group.ipp_value = 20
    #     mock_reservation.request_group.proposal.tac_priority = 50
    #     mock_reservation.request_group.id = 1
    #     mock_reservation.request.id = 2

    #     expected_datacontainer = DataContainer(
    #         request_group_id=1,
    #         request_id=2,
    #         duration=10,
    #         scheduled_resource='bpl',
    #         scheduled=True,
    #         scheduled_start=start_time,
    #         ipp_value=20,
    #         tac_priority=50,
    #     )

    #     bin_data = {
    #         'bin1': mock_reservation,
    #         'bin2': mock_reservation,
    #     }
    #     for bin_name, reservation in bin_data.items():
    #         fill_bin_with_reservation_data(data_dict, bin_name, reservation)

    #     expected = {
    #         'bin1': [expected_datacontainer],
    #         'bin2': [expected_datacontainer],
    #     }
    #     for bin_name, data in data_dict.items():
    #         for i, item in enumerate(data):
    #             assert expected[bin_name][i].__dict__ == item.__dict__

    def test_airmass_functions(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        data_path_1 = os.path.join(dir_path, 'airmass_data.json')
        data_path_2 = os.path.join(dir_path, 'airmass_data_2.json')
        with open(data_path_1) as f:
            airmass_data_1 = json.load(f)
        with open(data_path_2) as f:
            airmass_data_2 = json.load(f)
        self.metrics._get_airmass_data_from_observation_portal = Mock(side_effect=[airmass_data_1, airmass_data_1,
                                                                                   airmass_data_1, airmass_data_2,
                                                                                   airmass_data_1, airmass_data_2,
                                                                                   airmass_data_1, airmass_data_2,
                                                                                   airmass_data_1, airmass_data_2])
        request_id_1 = Mock()
        request_1 = Mock(id=request_id_1)
        mock_reservation_1 = Mock(scheduled_start=0, scheduled_resource='1m0a.doma.tfn',
                                    request=request_1, duration=5400)
        request_id_2 = Mock()
        request_2 = Mock(id=request_id_2)
        mock_reservation_2 = Mock(scheduled_start=0, scheduled_resource='1m0a.doma.egg',
                                    request=request_2, duration=5400)
        scheduled_reservations = [mock_reservation_1, mock_reservation_2]
        schedule = {'reservations': scheduled_reservations}

        assert self.metrics._get_midpoint_airmasses_from_request(request_id_1, self.start, self.end) == {'tfn': 7, 'egg': 3}
        assert self.metrics._get_ideal_airmass_for_request(request_id_2) == 1
        assert self.metrics.avg_ideal_airmass(schedule) == 2
        assert self.metrics.avg_midpoint_airmass(schedule) == 5
        assert self.metrics.avg_ideal_airmass() == float(5/3)
        