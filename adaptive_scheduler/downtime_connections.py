from adaptive_scheduler.utils import metric_timer, SendMetricMixin, timeit

import logging
from datetime import datetime
import requests

log = logging.getLogger(__name__)
DOWNTIME_DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


class DowntimeError(Exception):
    pass


class DowntimeInterface(SendMetricMixin):
    ''' Class for providing access to information in downtime.lco.gtn. It provides a set of datetimes on resources
        and potentially by instrument_type that should be used to exclude scheduling on those resources during those times.
    '''

    def __init__(self, downtime_url):
        self.downtime_url = downtime_url
        if not self.downtime_url.endswith('/'):
            self.downtime_url += '/'

    def _get_downtime_json(self, start, end):
        ''' Function calls the downtime endpoint and returns the json list of downtime intervals back.
        '''
        try:
            r = requests.get(
                self.downtime_url + 'api/?ends_after={}&starts_before={}'.format(
                    start.strftime('%Y-%m-%d %H:%M:%S'), end.strftime('%Y-%m-%d %H:%M:%S')
                ),
                timeout=120
            )
        except requests.exceptions.RequestException as e:
            msg = "{}: {}".format(e.__class__.__name__, "_get_downtime_json failed: {} connection down: {}".format(
                self.downtime_url, repr(e)))
            raise DowntimeError(msg)
        except requests.exceptions.Timeout as te:
            msg = "{}: {}".format(te.__class__.__name__, "_get_downtime_json failed: {} connection timeout: {}".format(
                self.downtime_url, repr(te)))
            raise DowntimeError(msg)
        r.encoding = 'UTF-8'
        if not r.status_code == 200:
            raise DowntimeError("_get_downtime_json failed: {} status code {}".format(self.downtime_url, r.status_code))
        json_results = r.json()
        return json_results['results']

    @timeit
    @metric_timer('downtime.get_downtime_intervals')
    def get_downtime_intervals_by_resource_and_instrument_type(self, start, end):
        ''' Function returns the downtime intervals by resource (telescope) and instrument_type as datetime tuples
        '''
        downtime_intervals = {}
        downtime_json = self._get_downtime_json(start, end)

        for interval in downtime_json:
            resource = '.'.join(
                [interval['telescope'].lower(), interval['enclosure'].lower(), interval['site'].lower()])
            instrument_type = interval['instrument_type'] if interval['instrument_type'] else 'all'
            if resource not in downtime_intervals:
                downtime_intervals[resource] = {}
            if instrument_type not in downtime_intervals[resource]:
                downtime_intervals[resource][instrument_type] = []
            downtime_intervals[resource][instrument_type].append(
                (datetime.strptime(interval['start'], DOWNTIME_DATE_FORMAT),
                datetime.strptime(interval['end'], DOWNTIME_DATE_FORMAT))
            )

        return downtime_intervals
