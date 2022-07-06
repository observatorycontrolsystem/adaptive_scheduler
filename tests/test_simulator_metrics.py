from adaptive_scheduler.simulator.metrics import (SimulatorMetrics, percent_of,
                                                  percent_diff, merge_dicts)
from adaptive_scheduler.scheduler import Scheduler, SchedulerRunner, SchedulerResult

from mock import Mock, patch

import pytest


class TestMetrics():

    def setup(self):
        # PLACEHOLDER: replace the following with fake instances
        # self.normal_scheduler_result = SchedulerResult()
        # self.rr_scheduler_result = SchedulerResult()
        # self.scheduler = Scheduler()
        # self.scheduler_runner = SchedulerRunner()
        pass

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

    def test_binning_functions(self):
        # TODO: refactor metrics.py so binning functions are more modular
        # PLACEHOLDER: some tests with binned data
        pass

    def test_airmass_functions(self):
        # test with fake airmass data in the same format as returned by Observation Portal
        # PLACEHOLDER: some test ideal airmass
        # PLACEHOLDER: some test midpoint airmass
        # PLACEHOLDER: some tests with airmass averaging functions
        pass
