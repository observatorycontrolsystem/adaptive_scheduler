from adaptive_scheduler.utils import timeit
from reqdb.client import SearchQuery, SchedulerClient

import json
import logging

log = logging.getLogger(__name__)

@timeit
def get_requests_from_db(url, telescope_class, sem_start, sem_end):
    format = '%Y-%m-%d %H:%M:%S'

    search = SearchQuery()
    search.set_date(start=sem_start.strftime(format), end=sem_end.strftime(format))

    log.info("Asking DB (%s) for User Requests between %s and %s", url, sem_start, sem_end)
    sc = SchedulerClient(url)

    ur_list = sc.retrieve(search, debug=True)

    return ur_list


def get_requests(url, telescope_class):

    rc = RetrievalClient(url)
    rc.set_location(telescope_class)

    json_req_str = rc.retrieve()
    requests     = json.loads(json_req_str)

    return requests