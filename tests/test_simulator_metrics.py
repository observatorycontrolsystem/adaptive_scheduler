from adaptive_scheduler.simulation.metrics import SimulatorMetrics, fill_bin_with_reservation_data
from adaptive_scheduler.scheduler import Scheduler, SchedulerRunner, SchedulerResult
from adaptive_scheduler.models import DataContainer

from mock import Mock, patch

from datetime import datetime

import pytest


class TestMetrics():

    def setup(self):
        self.mock_reservation = Mock()
        self.mock_requests = Mock(return_value=[])
        

    def test_percent_scheduled_counters(self):
        # testing input is SchedulerResult with fake Reservation() data
        # PLACEHOLDER: some test all scheduled
        # PLACEHOLDER: some test none scheduled
        # PLACEHOLDER: some test empty schedule
        # PLACEHOLDER: some test some known percentage
        pass

    def test_scheduled_time_aggregator(self):
        # testing input is SchedulerResult with fake Reservation() data with varying duration
        # PLACEHOLDER: some test some known duration
        # PLACEHOLDER: some test no duration
        pass

    def test_available_time_aggregator(self):
        # testing input is SchedulerResult with fake Reservation() data with emphasis on resources scheduled
        # also mock the visibility cache with our own dark intervals
        # test with varying 'effective horizons', e.g.
        test_horizon_days = [1, 0.5, 2, 5]
        # PLACEHOLDER: some test no resources
        # PLACEHOLDER: some test no dark intervals
        # PLACEHOLDER: some test dark intervals less than capped
        # PLACEHOLDER: some test dark intervals that need to be capped
        # PLACEHOLDER: some test different horizon days
        pass

    def test_fill_bin_with_reservation_data(self):
        data_dict = {}
        start_time = datetime.utcnow()
        
        self.mock_reservation.request_group.requests = self.mock_requests
        self.mock_reservation.request_group.ipp_value = 20
        self.mock_reservation.request_group.proposal.tac_priority = 50
        self.mock_reservation.request_group.id = 1
        self.mock_reservation.duration = 10
        self.mock_reservation.scheduled_resource = 'bpl'
        self.mock_reservation.scheduled_start = start_time
        self.mock_reservation.scheduled = True
        
        expected_datacontainer = DataContainer(
            request_group_id=1,
            duration=10,
            scheduled_resource='bpl',
            scheduled=True,
            scheduled_start=start_time,
            ipp_value=20,
            tac_priority=50,
            requests=self.mock_requests,
        )
        
        bin_data = {
            'bin1': self.mock_reservation,
            'bin2': self.mock_reservation,
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
