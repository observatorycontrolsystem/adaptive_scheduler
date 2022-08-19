from opensearchpy import OpenSearch
from dateutil.parser import parse
from datetime import datetime, timedelta


class ConnectionError(Exception):
    pass


def get_seeing(resources, opensearch_url, os_index, os_excluded_observatories, lookback_minutes):
    ''' Get seeing value from an opensearch index structured like the banzai qc index. Any opensearch index which has keys on telescope
        , site, observatory, and seeing value should work though.
    '''
    os = OpenSearch(opensearch_url, http_compress=True)

    seeing_by_resource = {}
    for resource in resources:
        # Resource format is from configdb telescope data, so telescope.enclosure.site
        telescope, enclosure, site = resource.split('.')
        if enclosure not in os_excluded_observatories:
            seeing_query = _get_seeing_query(site, enclosure, telescope, lookback_minutes)
            try:
                results = os.search(index=os_index, request_timeout=60, body=seeing_query, source=['seeing', '@timestamp'])
            except Exception:
                # retry one time in case this was a momentary outage which happens occasionally
                try:
                    results = os.search(index=os_index, request_timeout=60, body=seeing_query, source=['seeing', '@timestamp'])
                except Exception as ex:
                    raise ConnectionError(
                        f"Failed to get seeing for {resource} from OpenSearch after 2 attempts: {repr(ex)}")
            if results['hits']['hits']:
                seeing_by_resource[resource] = {}
                for key in MAPPING:
                    name, conversion_function = MAPPING[key]
                    seeing_by_resource[resource][name] = conversion_function(results['hits']['hits']['_source'][key])

    return seeing_by_resource


def _get_seeing_query(site, enclosure, telescope, lookback_minutes):
    formatter = "%Y-%m-%d %H:%M:%S"
    seeing_query = {
        "query": {
            "bool": {
                "filter": [
                    {
                        "match": {
                            "site": site
                        }
                    },
                    {
                        "match": {
                            "enclosure": enclosure
                        }
                    },
                    {
                        "match": {
                            "telescope": telescope
                        }
                    },
                     {
                        "range": {
                            "@timestamp": {
                                "gte": (datetime.utcnow() - timedelta(minutes=lookback_minutes)).strftime(formatter),
                                "format": "yyyy-MM-dd HH:mm:ss"
                            }
                        }
                    }
                ]
            }
        },
        "size": 1,
        "sort": [
            {
                "@timestamp": {
                    "order": "desc"
                }

            }
        ]
    }

    return seeing_query


NULL_CONVERSION = str
MAPPING = {
    '@timestamp': ('time', parse),
    'seeing': ('seeing', NULL_CONVERSION)
}