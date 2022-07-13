"""
Metric calculation functions for the scheduler simulator.
"""
import pickle
from datetime import datetime, timedelta
from collections import defaultdict

import numpy as np
import requests
from requests.exceptions import RequestException, Timeout

from adaptive_scheduler.observation_portal_connections import ObservationPortalConnectionError
from adaptive_scheduler.utils import time_in_capped_intervals, normalised_epoch_to_datetime, datetime_to_epoch
from adaptive_scheduler.kernel_mappings import redis


def percent_of(x, y):
    """Returns x/y as a percentage."""
    return x / y * 100.


def percent_diff(x, y):
    """Returns the percent difference between x and y as a float."""
    if x == y == 0:
        return 0
    mean = (abs(x) + abs(y)) / 2
    return abs(x - y) / mean * 100.


def generate_bin_names(bin_size, bin_range):
    """Creates labels for the bins."""
    start = bin_range[0]
    end = bin_range[1]
    bin_names = []
    bin_start = np.arange(start, end+1, bin_size)
    for start_num in bin_start:
        if np.issubdtype(bin_start.dtype, np.integer):
            end_num = start_num + bin_size - 1
            end_num = end_num if end_num < end else end
        else:
            end_num = start_num + bin_size
            end_num = end_num if end_num < end else float(end)
        if end_num == start_num:
            bin_name = str(start_num)
        else:
            bin_name = f'{start_num}-{end_num}'
        bin_names.append(bin_name)
    return bin_names


def bin_data(bin_by, data=[], bin_size=1, bin_range=None, aggregator=sum):
    """Bins data to create a histogram. Each bin is half-open, i.e. defined on the interval [a, b) for every bin
    except for the last bin, which is defined on the interval [a, b]. The naming convention is different for
    integers and floats. For example, for the label '1-2', this means the discrete values 1 and 2, whereas
    for the label '1.0-2.0' this means the values on the interval [1.0, 2.0).

    Args:
        bin_by (list): A list of data to bin by. Can be float or int.
        data (list): Additional data points associated with the data to bin by. If the lengths are
            mismatched, you will get an IndexError if the data list is too short. If it is too long,
            extra values are thrown out. The aggregation function is applied to the data at the end.
        bin_size (int): The width of the bins.
        bin_range (int, int): Override the bin ranges. Otherwise, use the min/max of the data.
        aggregator (func): The aggregation function to apply over the list of data. Must be callable on an array.
            Additional items can be passed to the aggregation function.

    Returns:
        data_dict (str: int): The frequency count of the data.
    """
    bin_range = (min(bin_by), max(bin_by)) if bin_range is None else bin_range
    bin_dict = {bin_name: [] for bin_name in generate_bin_names(bin_size, bin_range)}

    for i, value in enumerate(bin_by):
        if value < bin_range[0] or value > bin_range[1]+1:
            continue
        index = int((value - bin_range[0]) / bin_size)
        keyname = list(bin_dict)[index]
        if data:
            bin_dict[keyname].append(data[i])
        else:
            bin_dict[keyname].append(1)
    bin_dict = {key: aggregator(val) for key, val in bin_dict.items() if val}
    return bin_dict


class MetricCalculator():
    """A class encapsulating the metric calculating functions for the scheduler simulator.

    Args:
        normal_scheduler_result (SchedulerResult): The normal schedule output of the scheduler.
            The attribute of interest is SchedulerResult.schedule, which is a dictionary formatted
            as follows:
                {scheduled_resource, [reservations]}
        rr_scheduler_result (SchedulerResult): The rapid-response schedule output of the scheduler.
        scheduler (LCOGTNetworkScheduler): The instance of the scheduler used by the simulator.
        scheduler_runner (SchedulerRunner): The instance of the scheduler runner used by the simulator.
    """
    def __init__(self, normal_scheduler_result, rr_scheduler_result, scheduler, scheduler_runner):
        self.scheduler = scheduler
        self.scheduler_runner = scheduler_runner
        self.observation_portal_interface = self.scheduler_runner.network_interface.observation_portal_interface
        self.horizon_days = self.scheduler_runner.sched_params.horizon_days

        self.normal_scheduler_result = normal_scheduler_result
        self.normal_schedule = self.normal_scheduler_result.schedule
        self.normal_input_reservations = self.normal_scheduler_result.input_reservations
        self.combined_schedule = defaultdict(dict)
        self.combined_input_reservations = []
        if rr_scheduler_result:
            self.rr_scheduler_result = rr_scheduler_result
            self.rr_schedule = self.rr_scheduler_result.schedule
            self.rr_input_reservations = self.rr_scheduler_result.input_reservations
            self._combine_normal_rr_schedules()
            self._combine_resources_scheduled()
            self._combine_normal_rr_input_reservations()
        else:
            self.combined_schedule = self.normal_schedule
            self.combined_resources_scheduled = self.normal_scheduler_result.resources_scheduled()
            for comp_res in self.normal_input_reservations:
                self.combined_input_reservations.extend(comp_res.reservation_list)

        self.airmass_data_by_request_id = defaultdict(dict)

    def _combine_resources_scheduled(self):
        normal_resources = self.normal_scheduler_result.resources_scheduled()
        rr_resources = self.rr_scheduler_result.resources_scheduled()
        self.combined_resources_scheduled = list(set(normal_resources + rr_resources))

    def _combine_normal_rr_schedules(self):
        self.combined_schedule = defaultdict(list)
        for resource, reservations in self.rr_schedule.items():
            self.combined_schedule[resource].extend(reservations)
        for resource, reservations in self.normal_schedule.items():
            reservations = [res for res in reservations if res not in self.combined_schedule[resource]]
            self.combined_schedule[resource].extend(reservations)

    def _combine_normal_rr_input_reservations(self):
        for comp_res in self.normal_input_reservations:
            self.combined_input_reservations.extend(comp_res.reservation_list)
        for comp_res in self.rr_input_reservations:
            reservations = [res for res in comp_res.reservation_list if res not in self.combined_input_reservations]
            self.combined_input_reservations.extend(reservations)

    def count_scheduled(self, input_reservations=None, schedule=None):
        input_reservations = self.combined_input_reservations if input_reservations is None else input_reservations
        schedule = self.combined_schedule if schedule is None else schedule
        scheduled_reservations = []
        for reservations in schedule.values():
            scheduled_reservations.extend(reservations)
        return len(scheduled_reservations), len(input_reservations)

    def percent_reservations_scheduled(self, input_reservations=None, schedule=None):
        input_reservations = self.combined_input_reservations if input_reservations is None else input_reservations
        schedule = self.combined_schedule if schedule is None else schedule
        scheduled, total = self.count_scheduled(input_reservations, schedule)
        return percent_of(scheduled, total)

    def total_scheduled_eff_priority(self, schedule=None):
        schedule = self.combined_schedule if schedule is None else schedule
        effective_priorities = []
        for reservations in schedule.values():
            effective_priorities.extend([res.priority for res in reservations])
        return sum(effective_priorities), effective_priorities

    def get_scheduled_durations(self, schedule=None):
        schedule = self.combined_schedule if schedule is None else schedule
        durations = []
        for reservations in schedule.values():
            durations.extend([res.duration for res in reservations])
        return durations

    def total_available_seconds(self, resources_scheduled=None, horizon_days=None):
        """Aggregates the total available time, calculated from dark intervals.

        Args:
            resources_scheduled (list): The list of sites scheduled, if nothing is passed then use the
                list generated when MetricCalculators is initialized.
            horizon_days (float): The number of days to cap, basically an effective horizon. If nothing
                is passed then use the value in sched_params.

        Returns:
            total_available_time (float): The dark intervals capped by the horizon.
        """
        resources_scheduled = self.combined_resources_scheduled if resources_scheduled is None else resources_scheduled
        horizon_days = self.horizon_days if horizon_days is None else horizon_days
        total_available_time = 0
        start_time = self.scheduler.estimated_scheduler_end
        end_time = start_time + timedelta(days=horizon_days)
        for resource in resources_scheduled:
            if resource in self.scheduler.visibility_cache:
                dark_intervals = self.scheduler.visibility_cache[resource].dark_intervals
                available_time = time_in_capped_intervals(dark_intervals, start_time, end_time)
                total_available_time += available_time
        return total_available_time

    def percent_time_utilization(self, schedule=None, resources_scheduled=None, horizon_days=None):
        schedule = self.combined_schedule if schedule is None else schedule
        resources_scheduled = self.combined_resources_scheduled if resources_scheduled is None else resources_scheduled
        horizon_days = self.horizon_days if horizon_days is None else horizon_days
        return percent_of(sum(self.get_scheduled_durations(schedule)),
                          self.total_available_seconds(resources_scheduled, horizon_days))

    def _get_airmass_data_for_request(self, request_id):
        """Pulls airmass data from the Observation Portal.

        Args:
            observation_portal_interface (ObservationPortalInterface): Instance of the Observation Portal
                used by the scheduler.
            request_id (str): The request id.

        Returns:
            airmass_data (dict): The airmass data returned from the API.
        """
        airmass_url = f'{self.observation_portal_interface.obs_portal_url}/api/requests/{request_id}/airmass/'
        try:
            cached_airmass_data = pickle.loads(redis.get('airmass_data_by_request_id'))
            self.airmass_data_by_request_id[request_id] = cached_airmass_data[request_id]
            return cached_airmass_data[request_id]
        except Exception:
            # the request has not been cached yet, get the data from the portal
            pass
        try:
            response = requests.get(airmass_url, headers=self.observation_portal_interface.headers, timeout=180)
            response.raise_for_status()
            airmass_data_for_request = response.json()['airmass_data']
            self.airmass_data_by_request_id[request_id] = airmass_data_for_request
            redis.set('airmass_data_by_request_id', pickle.dumps(dict(self.airmass_data_by_request_id)))
            return airmass_data_for_request
        except (RequestException, ValueError, Timeout) as e:
            raise ObservationPortalConnectionError("get_airmass_data failed: {}".format(repr(e)))

    def _get_ideal_airmass(self, airmass_data):
        """Finds the minimum airmass across all sites."""
        ideal_airmass = 1000
        for site in airmass_data.values():
            ideal_for_site = min(site['airmasses'])
            ideal_airmass = min(ideal_airmass, ideal_for_site)
        return ideal_airmass

    def _get_midpoint_airmasses_by_site(self, airmass_data, start_time, end_time):
        """"Gets the midpoint airmasses by site for a request. This is done by finding the time
        closest matching the calculated midpoint of the observation in the observe portal airmass data.

        Args:
            request_id (int): The id of the request we want to get airmass data of.
            start_time (datetime.datetime): The start time of the scheduled observation.
            end_time (datetime.datetime): The end time of the scheduled observation.

        Returns:
            midpoint_airmasses (str: float): A dictionary with observation sites as keys and corresponding
                midpoint airmasses as values.
        """
        midpoint_airmasses = {}
        midpoint_time = start_time + (end_time - start_time) / 2
        for site, details in airmass_data.items():
            details = list(details.values())
            times, airmasses = details[0], details[1]
            index = 0
            time_diff = abs((midpoint_time - datetime.strptime(times[0], '%Y-%m-%dT%H:%M')).total_seconds())
            for i, _ in enumerate(times):
                temp_time_diff = abs((midpoint_time - datetime.strptime(times[i], '%Y-%m-%dT%H:%M')).total_seconds())
                if temp_time_diff < time_diff:
                    time_diff = temp_time_diff
                    index = i
            midpoint_airmass = airmasses[index]
            midpoint_airmasses[site] = midpoint_airmass
        return midpoint_airmasses

    def airmass_metrics(self, schedule=None):
        """Calculate the average midpoint airmass of all scheduled reservations for a single schedule.

        Args:
            schedule (scheduler, optional): the schedule we calculate our metricses on. Defaults to None.

        Returns:
            average(float): the average midpoint airmass of all scheduled reservation for one schedule.
        """
        schedule = self.combined_schedule if schedule is None else schedule
        semester_start = self.scheduler_runner.semester_details['start']

        midpoint_airmasses = []
        ideal_airmasses = []
        durations = self.get_scheduled_durations(schedule)
        for reservations in schedule.values():
            for reservation in reservations:
                airmass_data = self._get_airmass_data_for_request(reservation.request.id)
                start_time = normalised_epoch_to_datetime(reservation.scheduled_start,
                                                          datetime_to_epoch(semester_start))
                end_time = start_time + timedelta(seconds=reservation.duration)
                midpoint_airmasses_by_site = self._get_midpoint_airmasses_by_site(airmass_data, start_time, end_time)
                site = reservation.scheduled_resource[-3:]
                midpoint_airmasses.append(midpoint_airmasses_by_site[site])
                ideal_airmass = self._get_ideal_airmass(airmass_data)
                ideal_airmasses.append(ideal_airmass)
        airmass_metrics = {'raw_airmass_data': [{'midpoint_airmasses': midpoint_airmasses},
                                                {'ideal_airmasses': ideal_airmasses},
                                                {'durations': durations}],
                           'avg_midpoint_airmass': sum(midpoint_airmasses)/len(midpoint_airmasses),
                           'avg_ideal_airmass': sum(ideal_airmasses)/len(ideal_airmasses),
                           'ci_midpoint_airmass': [[np.percentile(midpoint_airmasses, 2.5),
                                                    np.percentile(midpoint_airmasses, 97.5)]],
                           }
        return airmass_metrics

    def binned_tac_priority_metrics(self, input_reservations=None, schedule=None):
        """Bins TAC Priority into the following bins: '10-19', '20-29', '30-39', '1000'."""
        input_reservations = self.combined_input_reservations if input_reservations is None else input_reservations
        schedule = self.combined_schedule if schedule is None else schedule
        bin_size = 10
        sched_priority_values = []
        sched_durations = self.get_scheduled_durations(schedule)
        all_priority_values = []
        all_durations = []
        for reservations in schedule.values():
            sched_priority_values.extend([res.request_group.proposal.tac_priority for res in reservations])
        for reservation in input_reservations:
            all_priority_values.append(reservation.request_group.proposal.tac_priority)
            all_durations.append(reservation.duration)

        sched_histogram = bin_data(sched_priority_values, bin_size=bin_size)
        bin_sched_durations = bin_data(sched_priority_values, sched_durations, bin_size)
        full_histogram = bin_data(all_priority_values, bin_size=bin_size)
        bin_all_durations = bin_data(all_priority_values, all_durations, bin_size)
        bin_percent_count = {bin_: percent_of(np.array(sched_histogram[bin_]), np.array(full_histogram[bin_]))
                             for bin_ in sched_histogram}
        bin_percent_duration = {bin_: percent_of(np.array(bin_sched_durations[bin_]), np.array(bin_all_durations[bin_]))
                                for bin_ in bin_sched_durations}

        output_dict = {
            'sched_histogram': sched_histogram,
            'sched_durations': bin_sched_durations,
            'full_histogram': full_histogram,
            'all_durations': bin_all_durations,
            'percent_count': bin_percent_count,
            'percent_duration': bin_percent_duration,
        }
        return output_dict
