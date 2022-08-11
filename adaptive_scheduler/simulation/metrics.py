"""
Metric calculation functions for the scheduler simulator.
"""
import copy
import logging
import pickle
from datetime import datetime, timedelta
from collections import defaultdict

import numpy as np
import requests
import rise_set
from requests.exceptions import RequestException, Timeout
from rise_set import astrometry

from adaptive_scheduler.observation_portal_connections import ObservationPortalConnectionError
from adaptive_scheduler.utils import time_in_capped_intervals, normalised_epoch_to_datetime, datetime_to_epoch
from adaptive_scheduler.models import redis_instance

log = logging.getLogger('adaptive_scheduler')

DTFORMAT = '%Y-%m-%dT%H:%M'


def percent_of(x, y):
    """Returns x/y as a percentage."""
    try:
        return x / y * 100.
    except ZeroDivisionError as e:
        if y == 0:
            return 0
        else:
            raise e


def percent_diff(x, y):
    """Returns the percent difference between x and y as a float."""
    if x == y == 0:
        return 0
    mean = (abs(x) + abs(y)) / 2
    return abs(x - y) / mean * 100.


def scalefunc(p, newmax, newmin, oldmax, oldmin):
    """Remaps a range of values to another range of values."""
    return (p-oldmin)*(newmax-newmin)/(oldmax-oldmin) + newmin


def generate_bin_names(bin_size, bin_range):
    """Creates labels for the bins."""
    start, end = bin_range
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


def bin_data(bin_by, data=[], bin_size=1, bin_range=None, fill=[], aggregator=len):
    """Bins data to create a histogram. For float values, each bin is half-open, i.e. defined on
    the interval [a, b) for every bin except for the last bin, which is defined on the interval [a, b].
    The naming convention is different for integers, which use open intervals [a, b] since they are discrete.
    For example, for the label '1-3', this means the values 1, 2, and 3, whereas for the label '1.0-3.0'
    this means the values on the interval [1.0, 3.0). Bins are uniformly spaced.

    Args:
        bin_by (list): A list of data to bin by. Can be float or int.
        data (list): Additional data points associated with the data to bin by. It is best for the length
            of this array to match the length of bin_by. The aggregation function is applied to the data
            after binning, on a per-bin basis.
        bin_size: The width of the bins.
        bin_range: A tuple of numbers to override the bin ranges. Otherwise, use the min/max of the data.
        fill: The data value(s) to fill with if the bin is empty. An iterable may be passed, in which case it is
            casted to a list. If None is passed, then empty bins are removed. The aggregator will be applied
            to fill values as well. Default is empty list to work with the default aggregator len().
        aggregator (func): The aggregation function to apply over the list of data. Must be callable on an array.
            If None is passed, then the raw values are stored in a list.

    Returns:
        data_dict: The binned data. Each key is a label corresponding to either a list of values or a single number,
            depending on the type of aggregation function used.

    Examples:
        Simple frequency count:
        >>> bin_data([1, 2, 3, 2, 6])
        {'1': 1, '2': 2, '3': 1, '4': 0, '5': 0, '6': 1}

        Frequency count without empty bins ('3' is removed):
        >>> bin_data([4, 4, 5, 6, 7, 2], fill=None)
        {'2': 1, '4': 2, '5': 1, '6': 1, '7': 1}

        Bin two lists of data, e.g. highest test score by age group:
        >>> ages = [12, 13, 11, 14, 15, 12, 13, 10, 10, 13]
        >>> scores = [76, 84, 92, 56, 91, 87, 72, 95, 89, 77]
        >>> bin_data(ages, scores, bin_size=2, bin_range=(10, 15), aggregator=max)
        {'10-11': 95, '12-13': 87, '14-15': 91}

        Get the raw list of items in each bin:
        >>> bin_data([4, 4, 5, 6, 9], [4, 4, 5, 6, 9], aggregator=None)
        {'4': [4, 4], '5': [5], '6': [6], '9': [9]}
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
            bin_dict[keyname].append(value)
    if fill is not None:
        try:
            fill = list(fill)
        except TypeError:
            fill = [fill]
        bin_dict = {key: vals if vals else fill for key, vals in bin_dict.items()}
    else:
        bin_dict = {key: vals for key, vals in bin_dict.items() if vals}
    bin_dict = {key: aggregator(vals) if aggregator else vals for key, vals in bin_dict.items()}

    return bin_dict


class MetricCalculator():
    """A class encapsulating the metric calculating functions for the scheduler simulator.

    Args:
        normal_scheduler_result (SchedulerResult): The normal schedule output of the scheduler.
            The attribute of interest is SchedulerResult.schedule, which is a dictionary formatted
            as follows:
                {scheduled_resource, [reservations]}
        rr_scheduler_result (SchedulerResult): The rapid-response schedule output of the scheduler.
        scheduler (Scheduler): The instance of the scheduler used by the simulator.
        scheduler_runner (SchedulerRunner): The instance of the scheduler runner used by the simulator.
    """
    def __init__(self, normal_scheduler_result, rr_scheduler_result, scheduler, scheduler_runner):
        self.scheduler = scheduler
        self.scheduler_runner = scheduler_runner
        self.observation_portal_interface = self.scheduler_runner.network_interface.observation_portal_interface
        self.simulation_start = self.scheduler_runner.sched_params.simulate_now
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
            self.combined_resources_scheduled = [site for site in self.normal_schedule.keys() if self.normal_schedule[site]]
            for comp_res in self.normal_input_reservations:
                self.combined_input_reservations.extend(comp_res.reservation_list)

        self.request_groups = self.scheduler_runner.normal_scheduler_input.request_groups
        if self.scheduler_runner.rr_scheduler_input:
            self.request_groups.extend(self.scheduler_runner.rr_scheduler_input.request_groups)

        self.airmass_data_by_request_id = defaultdict(dict)

    def _combine_resources_scheduled(self):
        normal_resources = self.normal_scheduler_result.resources_scheduled()
        rr_resources = self.rr_scheduler_result.resources_scheduled()
        normal_resources = [site for site in self.normal_schedule.keys() if self.normal_schedule[site]]
        rr_resources = [site for site in self.rr_schedule.keys() if self.rr_schedule[site]]
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

    def count_scheduled(self):
        scheduled_reservations = []
        for reservations in self.combined_schedule.values():
            scheduled_reservations.extend(reservations)
        return len(scheduled_reservations), len(self.combined_input_reservations)

    def percent_reservations_scheduled(self):
        scheduled, total = self.count_scheduled()
        return percent_of(scheduled, total)

    def total_scheduled_eff_priority(self):
        effective_priorities = []
        for reservations in self.combined_schedule.values():
            effective_priorities.extend([res.priority for res in reservations])
        return sum(effective_priorities), effective_priorities

    def get_duration_data(self):
        """Returns scheduled and unscheduled durations."""
        sched_durations = []
        unsched_durations = []
        for res in self.combined_input_reservations:
            sched_durations.append(res.duration) if res.scheduled else unsched_durations.append(res.duration)
        return sched_durations, unsched_durations

    def get_priority_data(self):
        """Returns scheduled and unscheduled priority values. Accesses them in the same order as durations so
        they can be cross-matched. Scaling changes the priorities to a different range of numbers."""
        sched_rg_ids = []
        unsched_rg_ids = []
        for res in self.combined_input_reservations:
            sched_rg_ids.append(res.request_group_id) if res.scheduled else unsched_rg_ids.append(res.request_group_id)
        priorities_by_rg_id = {rg.id: rg.proposal.tac_priority for rg in self.request_groups}
        sched_priorities = [priorities_by_rg_id[rg_id] for rg_id in sched_rg_ids]
        unsched_priorities = [priorities_by_rg_id[rg_id] for rg_id in unsched_rg_ids]
        return sched_priorities, unsched_priorities

    def get_window_duration_data(self):
        sched_window_durations = []
        for res in self.combined_input_reservations:
            if res.scheduled:
                windows = res.request.windows
                # format the data to a list, each element is a list corresponding to a resource
                windows_list = list(windows.windows_for_resource.values())
                window_durations = []
                for loc in windows_list:
                    window_durations.extend([(w.end-w.start).total_seconds() for w in loc])
                sched_window_durations.append(max(window_durations))
        return sched_window_durations

    def total_available_seconds(self):
        """Aggregates the total available time, calculated from dark intervals.

        Returns:
            total_available_time (float): The dark intervals capped by the horizon.
        """
        total_available_time = 0
        start_time = self.scheduler.estimated_scheduler_end
        end_time = start_time + timedelta(days=self.horizon_days)
        for resource in self.combined_resources_scheduled:
            if resource in self.scheduler.visibility_cache:
                dark_intervals = self.scheduler.visibility_cache[resource].dark_intervals
                available_time = time_in_capped_intervals(dark_intervals, start_time, end_time)
                total_available_time += available_time
        return total_available_time

    def percent_time_utilization(self):
        scheduled_durations, _ = self.get_duration_data()
        return percent_of(sum(scheduled_durations), self.total_available_seconds())

    def _get_airmass_data_for_request(self, request_id):
        """Pulls airmass data from the Observation Portal, cache it in redis.

        Args:
            request_id (str): The request id.

        Returns:
            airmass_data (dict): The airmass data returned from the API or the cache.
        """
        airmass_url = f'{self.observation_portal_interface.obs_portal_url}/api/requests/{request_id}/airmass/'
        cached_airmass_data = redis_instance.get(f'airmass_data_{request_id}')
        if cached_airmass_data:
            self.airmass_data_by_request_id[request_id] = pickle.loads(cached_airmass_data)
            return pickle.loads(cached_airmass_data)
        try:
            response = requests.get(airmass_url, headers=self.observation_portal_interface.headers, timeout=180)
            response.raise_for_status()
            airmass_data_for_request = response.json()['airmass_data']
            self.airmass_data_by_request_id[request_id] = airmass_data_for_request
            redis_instance.set(f'airmass_data_{request_id}', pickle.dumps(airmass_data_for_request))
            return airmass_data_for_request
        except (RequestException, ValueError, Timeout) as e:
            raise ObservationPortalConnectionError("get_airmass_data failed: {}".format(repr(e)))

    def _get_minmax_airmass(self, airmass_data, midpoint_duration):
        """Finds the minimum and maximum midpoint airmass across all sites."""
        max_airmass = 0
        min_airmass = 1000
        for site in airmass_data.values():
            _, airmasses = site.values()
            airmasses = np.array(airmasses)
            min_airmass = min(min(airmasses), min_airmass)
            max_airmass = max(max(airmasses), max_airmass)
        return min_airmass, max_airmass

    def _get_midpoint_airmasses_by_site(self, airmass_data, midpoint_time):
        """"Gets the midpoint airmasses by site for a request. This is done by finding the time
        closest matching the calculated midpoint of the observation in the observe portal airmass data.

        Args:
            airmass_data (dict): The airmass data we want to use to calculate midpoint of.
            start_time (datetime.datetime): The start time of the scheduled observation.
            end_time (datetime.datetime): The end time of the scheduled observation.

        Returns:
            midpoint_airmasses (str: float): A dictionary with observation sites as keys and corresponding
                midpoint airmasses as values.
        """
        midpoint_airmasses = {}
        for site, details in airmass_data.items():
            details = list(details.values())
            times, airmasses = details
            airmasses = np.array(airmasses)
            times = np.array([datetime.strptime(time, DTFORMAT) for time in times])
            midpoint_airmasses[site] = airmasses[np.argmin(np.abs(times-midpoint_time))]
        return midpoint_airmasses

    def airmass_metrics(self, schedule=None):
        """Generate the airmass metrics of all scheduled reservations for a single schedule.

        Args:
            schedule (scheduler, optional): the schedule we calculate our metrics on. Uses the schedule stored in
                the MetricCalculator instance if nothing is passed.

        Returns:
            airmass_metrics (dict): Variety of airmass metrics including raw data, average midpoint airmass, average
            ideal airmass and 95% confidence interval for midpoint airmass.
        """
        schedule = self.combined_schedule if schedule is None else schedule
        semester_start = self.scheduler_runner.semester_details['start']

        midpoint_airmasses = []
        min_airmasses = []
        max_airmasses = []
        for reservations in schedule.values():
            for reservation in reservations:
                airmass_data = self._get_airmass_data_for_request(reservation.request.id)
                start_time = normalised_epoch_to_datetime(reservation.scheduled_start,
                                                          datetime_to_epoch(semester_start))
                midpoint_duration = timedelta(seconds=reservation.duration/2)
                midpoint_time = start_time + midpoint_duration
                midpoint_airmasses_by_site = self._get_midpoint_airmasses_by_site(airmass_data, midpoint_time)
                site = reservation.scheduled_resource[-3:]
                midpoint_airmasses.append(midpoint_airmasses_by_site[site])
                min_airmass, max_airmass = self._get_minmax_airmass(airmass_data, midpoint_duration)
                min_airmasses.append(min_airmass)
                max_airmasses.append(max_airmass)
        airmass_metrics = {'raw_airmass_data': [{'midpoint_airmasses': midpoint_airmasses},
                                                {'min_poss_airmasses': min_airmasses},
                                                {'max_poss_airmasses': max_airmasses}],
                           'avg_midpoint_airmass': sum(midpoint_airmasses)/len(midpoint_airmasses),
                           'avg_min_poss_airmass': sum(min_airmasses)/len(min_airmasses),
                           }
        return airmass_metrics

    def binned_tac_priority_metrics(self):
        """Bins metrics based on TAC priority. Priority bins should be changed to match the data."""
        bin_size = 5
        bin_range = (10, 30)
        collect_upper_range = True

        sched_durations, unsched_durations = self.get_duration_data()
        all_durations = sched_durations + unsched_durations

        sched_priorities, unsched_priorities = self.get_priority_data()
        all_priorities = sched_priorities + unsched_priorities
        sched_histogram = bin_data(sched_priorities, bin_size=bin_size, bin_range=bin_range, fill=None)
        bin_sched_durations = bin_data(sched_priorities, sched_durations,
                                       bin_size, bin_range, fill=None, aggregator=sum)
        combined_histogram = bin_data(all_priorities, bin_size=bin_size, bin_range=bin_range, fill=None)
        bin_all_durations = bin_data(all_priorities, all_durations,
                                     bin_size, bin_range, fill=None, aggregator=sum)

        # collects things above maximum bin range into one large bin
        # e.g. bin 10-19, 20-29, 30, 31&up
        if collect_upper_range:
            max_prio = max(all_priorities)
            lower = bin_range[-1] + 1  # assumes discrete priority values
            upper_sched_histogram = bin_data(sched_priorities, bin_size=max_prio, bin_range=(lower, max_prio), fill=None)
            upper_sched_durations = bin_data(sched_priorities, sched_durations, bin_size=max_prio,
                                             bin_range=(lower, max_prio), fill=None, aggregator=sum)
            upper_combined_histogram = bin_data(all_priorities, bin_size=max_prio, bin_range=(lower, max_prio), fill=None)
            upper_all_durations = bin_data(all_priorities, all_durations, bin_size=max_prio,
                                           bin_range=(lower, max_prio), fill=None, aggregator=sum)
            sched_histogram = sched_histogram | upper_sched_histogram
            bin_sched_durations = bin_sched_durations | upper_sched_durations
            combined_histogram = combined_histogram | upper_combined_histogram
            bin_all_durations = bin_all_durations | upper_all_durations

        bin_percent_count = {bin_: percent_of(sched_histogram[bin_], combined_histogram[bin_])
                             for bin_ in sched_histogram}
        bin_percent_time = {bin_: percent_of(bin_sched_durations[bin_], bin_all_durations[bin_])
                            for bin_ in bin_sched_durations}

        output_dict = {
            'sched_histogram': sched_histogram,
            'sched_durations': bin_sched_durations,
            'full_histogram': combined_histogram,
            'all_durations': bin_all_durations,
            'percent_count': bin_percent_count,
            'percent_time': bin_percent_time
        }
        return output_dict

    def avg_slew_distance(self):
        semester_start = self.scheduler_runner.semester_details['start']
        slew_distances = []
        schedule_copy = copy.deepcopy(self.combined_schedule)
        for reservations in schedule_copy.values():
            apparent_radecs = []
            reservations.sort(key=lambda r: r.scheduled_start)
            for res in reservations:
                res_startdt = normalised_epoch_to_datetime(res.scheduled_start, datetime_to_epoch(semester_start))
                tdb = astrometry.date_to_tdb(res_startdt)
                for c in res.request.configurations:
                    try:
                        apparent_radecs.append(astrometry.mean_to_apparent(c.target.in_rise_set_format(), tdb))
                    except rise_set.exceptions.IncompleteTargetError:
                        # set a conservative estimate
                        ra = dec = rise_set.angle.Angle(degrees=0)
                        apparent_radecs.append((ra, dec))
            for i, radec in enumerate(apparent_radecs):
                try:
                    next_radec = apparent_radecs[i+1]
                    ang_dist = astrometry.angular_distance_between(*radec, *next_radec)
                    slew_distances.append(ang_dist.in_degrees())
                except IndexError:
                    break

        return np.mean(slew_distances)
