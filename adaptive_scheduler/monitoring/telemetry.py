from datetime import datetime
import collections

from elasticsearch import Elasticsearch
import logging

log = logging.getLogger('adaptive_scheduler')

ES_HOSTS = [
    'http://es1.lco.gtn:9200',
    'http://es2.lco.gtn:9200',
    'http://es3.lco.gtn:9200',
    'http://es4.lco.gtn:9200'
]

ES_TELEMETRY_INDEX = 'live-telemetry'


class ConnectionError(Exception):
    pass


def get_datum(datum, instance=None, originator=None):
    ''' Get data from live telemetry index in ES, ordered by timestamp ascending (i.e.
        newest value is last). '''
    es = Elasticsearch(ES_HOSTS)

    datum_query = _get_datum_query(datum, instance, originator)

    try:
        results = es.search(index=ES_TELEMETRY_INDEX, request_timeout=60, body=datum_query, size=1000)
    except Exception as e:
        # retry one time in case this was a momentary outage
        try:
            results = es.search(index=ES_TELEMETRY_INDEX, request_timeout=60, body=datum_query, size=1000)
        except Exception as e2:
            raise ConnectionError("Failed to get datum {} from ES after 2 attempts: {}".format(datum, repr(e2)))

    return [_convert_datum(dat['_source']) for dat in results['hits']['hits']]


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
                                "gte": "now-7d/d"
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
             'datuminstance':('instance'          ,NULL_CONVERSION),
             'site'         :('site'              ,NULL_CONVERSION),
             'observatory'  :('observatory'       ,NULL_CONVERSION),
             'telescope'    :('telescope'         ,NULL_CONVERSION),
             'timestamp'           :('timestamp_changed' ,_timestamp),
             'timestampmeasured'    :('timestamp_measured',_timestamp),
             'value_string'               :('value'             ,NULL_CONVERSION)
          }

Datum = collections.namedtuple('Datum', [ key for key,_ in MAPPING.values() ] )
