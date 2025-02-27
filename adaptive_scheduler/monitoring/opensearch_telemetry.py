from datetime import datetime
import collections
from retry import retry
from dateutil.parser import parse

from opensearchpy import OpenSearch
import logging

log = logging.getLogger('adaptive_scheduler')


class OSConnectionError(Exception):
    pass


def get_datum(datum_name, opensearch_url, os_index, os_excluded_observatories, instance=None, originator=None):
    ''' Get data from live telemetry index in OS, ordered by timestamp ascending (i.e.
        newest value is last). '''
    opensearch = OpenSearch(opensearch_url, http_compress=True)

    datum_query = _get_datum_query(datum_name, instance, originator)

    try:
        # retry one time in case this was a momentary outage which happens occasionally
        results = _get_datums(opensearch, index=os_index, query=datum_query)
    except Exception as ex:
        # retry one time in case this was a momentary outage which happens occasionally
        raise OSConnectionError(
            "Failed to get datum {} from OpenSearch after 2 attempts: {}".format(datum_name, repr(ex)))

    return [_convert_datum(dat['_source']) for dat in results['hits']['hits'] if
            dat['_source']['observatory'] not in os_excluded_observatories]


@retry(tries=2)
def _get_datums(opensearch, index, query, size=1000, timeout=60):
    results = opensearch.search(index=index, request_timeout=timeout, body=query, size=size)
    return results


def _get_datum_query(datumname, datuminstance=None, originator=None):
    datum_query = {
        "query": {
            "bool": {
                "filter": [
                    {
                        "match": {
                            "datumname": datumname
                        }
                    },
                    {
                        "range": {
                            "timestampmeasured": {
                                "gte": "now-60d/d"
                            }
                        }
                    }
                ]
            }
        }
    }
    if datuminstance:
        datum_query['query']['bool']['filter'].append({
            "match": {
                "datuminstance": datuminstance
            }
        })
    if originator:
        datum_query['query']['bool']['filter'].append({
            "match": {
                "originator": originator
            }
        })

    return datum_query


def _convert_datum(datum):
    ''' Convert datum keys and values using the MAPPING dict '''
    new_datum = {}
    for key in MAPPING:
        name, conversion_function = MAPPING[key]
        new_datum[name] = conversion_function(datum[key])

    return Datum(**new_datum)


def _timestamp(value):
    ''' Convert time (s) to datetime instance. '''
    return parse(value).replace(tzinfo=None)


NULL_CONVERSION = str
MAPPING = {
    'datuminstance': ('instance', NULL_CONVERSION),
    'site': ('site', NULL_CONVERSION),
    'observatory': ('observatory', NULL_CONVERSION),
    'telescope': ('telescope', NULL_CONVERSION),
    'timestamp': ('timestamp_changed', _timestamp),
    'timestampmeasured': ('timestamp_measured', _timestamp),
    '@timestamp': ('timestamp_recorded', _timestamp),
    'value_string': ('value', NULL_CONVERSION)
}

Datum = collections.namedtuple('Datum', [key for key, _ in MAPPING.values()])
