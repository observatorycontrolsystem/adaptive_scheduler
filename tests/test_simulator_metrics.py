from adaptive_scheduler.simulation.metrics import (MetricCalculator, fill_bin_with_reservation_data,
                                                   percent_reservations_scheduled,
                                                   total_scheduled_seconds,
                                                   total_available_seconds,)
from adaptive_scheduler.scheduler import Scheduler, SchedulerRunner, SchedulerResult
from adaptive_scheduler.models import DataContainer

from mock import Mock

from datetime import datetime, timedelta

import pytest


class TestMetrics():

    def test_percent_scheduled(self):
        scheduled_reservation = Mock(scheduled=True)
        unscheduled_reservation = Mock(scheduled=False)

        all_scheduled = {'bpl': [scheduled_reservation]}
        half_scheduled = {'bpl': [scheduled_reservation, unscheduled_reservation]}
        none_scheduled = {'bpl': [unscheduled_reservation]}
        multiple_sites = {'bpl': [scheduled_reservation, unscheduled_reservation],
                          'coj': [unscheduled_reservation, scheduled_reservation]}

        assert percent_reservations_scheduled(all_scheduled) == 100.
        assert percent_reservations_scheduled(half_scheduled) == 50.
        assert percent_reservations_scheduled(none_scheduled) == 0.
        assert percent_reservations_scheduled(multiple_sites) == 50.

    def test_total_scheduled_seconds(self):
        res1 = Mock(duration=10)
        res2 = Mock(duration=20)
        res3 = Mock(duration=30)
        fake_schedule = {'bpl': [res1, res2], 'coj': [res3]}

        assert total_scheduled_seconds(fake_schedule) == 60

    def test_total_available_seconds(self):
        seconds_in_day = 86400
        test_time = datetime.utcnow()
        
        scheduler_result_attrs = {'resources_scheduled.return_value': ['bpl', 'coj']}
        mock_scheduler_result = Mock(**scheduler_result_attrs)
        
        mock_scheduler = Mock(estimated_scheduler_end=test_time)
        mock_scheduler.visibility_cache = {'bpl': Mock(), 'coj': Mock()}
        mock_scheduler.visibility_cache['bpl'].dark_intervals = [(test_time-timedelta(days=5), test_time-timedelta(days=4)),
                                                                 (test_time, test_time+timedelta(days=1)),
                                                                 (test_time+timedelta(days=2), test_time+timedelta(days=3))]
        mock_scheduler.visibility_cache['coj'].dark_intervals = [(test_time, test_time+timedelta(days=2))]

        assert total_available_seconds(mock_scheduler_result, mock_scheduler_result, mock_scheduler, 0) == 0
        assert total_available_seconds(mock_scheduler_result, mock_scheduler_result, mock_scheduler, 1) == 2*seconds_in_day
        assert total_available_seconds(mock_scheduler_result, mock_scheduler_result, mock_scheduler, 5) == 4*seconds_in_day

    def test_fill_bin_with_reservation_data(self):
        data_dict = {}
        start_time = datetime.utcnow()

        mock_reservation = Mock(
            duration=10,
            scheduled_resource='bpl',
            scheduled_start=start_time,
            scheduled=True,
        )
        mock_reservation.request_group.requests = []
        mock_reservation.request_group.ipp_value = 20
        mock_reservation.request_group.proposal.tac_priority = 50
        mock_reservation.request_group.id = 1

        expected_datacontainer = DataContainer(
            request_group_id=1,
            duration=10,
            scheduled_resource='bpl',
            scheduled=True,
            scheduled_start=start_time,
            ipp_value=20,
            tac_priority=50,
            requests=[],
        )
        
        bin_data = {
            'bin1': mock_reservation,
            'bin2': mock_reservation,
        }
        for bin_name, reservation in bin_data.items():
            fill_bin_with_reservation_data(data_dict, bin_name, reservation)

        expected = {
            'bin1': [expected_datacontainer],
            'bin2': [expected_datacontainer],
        }
        for bin_name, data in data_dict.items():
            for i, item in enumerate(data):
                assert expected[bin_name][i].__dict__ == item.__dict__

    def test_airmass_functions(self):
        # test with fake airmass data in the same format as returned by Observation Portal
        # PLACEHOLDER: some test ideal airmass
        # PLACEHOLDER: some test midpoint airmass
        # PLACEHOLDER: some tests with airmass averaging functions
        pass
