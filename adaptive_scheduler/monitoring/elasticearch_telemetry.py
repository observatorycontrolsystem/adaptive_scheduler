from datetime import datetime
import collections

from elasticsearch import Elasticsearch
import logging

log = logging.getLogger('adaptive_scheduler')


class ConnectionError(Exception):
    pass


def get_datum(datum_name, elasticsearch_url, es_index, es_excluded_observatories, instance=None, originator=None):
    ''' Get data from live telemetry index in ES, ordered by timestamp ascending (i.e.
        newest value is last). '''
    es = Elasticsearch([elasticsearch_url])

    datum_query = _get_datum_query(datum_name, instance, originator)

    try:
        results = es.search(index=es_index, request_timeout=60, body=datum_query, size=1000)
    except Exception as e:
        # retry one time in case this was a momentary outage which happens occasionally
        try:
            results = es.search(index=es_index, request_timeout=60, body=datum_query, size=1000)
        except Exception as ex:
            raise ConnectionError(
                "Failed to get datum {} from Elasticsearch after 2 attempts: {}".format(datum_name, repr(ex)))

    return [_convert_datum(dat['_source']) for dat in results['hits']['hits'] if
            dat['_source']['observatory'] not in es_excluded_observatories]


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
    return datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')


NULL_CONVERSION = lambda x: str(x)
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
