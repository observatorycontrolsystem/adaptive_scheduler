'''
telemetry.py - Module for retrieving data from telemetry database.

description

Author: Martin Norbury
May 2013
'''
from __future__ import division

import collections

from sqlalchemy.engine import create_engine
from datetime import datetime

DEFAULT_URL    = 'mysql://hibernate:hibernate@harvester3.lco.gtn/harvest'
DEFAULT_ENGINE = create_engine(DEFAULT_URL)

class ConnectionError(Exception):
    pass

def get_datum(datum, instance=None, engine=None):
    ''' Get data from telemetry database. '''

    try:
        engine  = engine or DEFAULT_ENGINE
        results = _query_db(datum, instance, engine)
        return [_convert_datum(datum) for datum in results]
    except Exception as e:
        raise ConnectionError(e)

def _query_db(datum, instance, engine):
    ''' Retrieve datum from database.

        This query uses the SCRAPEVALUE table from harvester3 - a table
        populated via a cron script from the LIVEVALUE table at each site.
    '''
    query = """SELECT * from PROPERTY as P
               join SCRAPEVALUE as LV on P.IDENTIFIER=LV.IDENTIFIER
               where P.ADDRESS_DATUM='{datum}'
            """
    if instance:
        query += "and P.ADDRESS_DATUMINSTANCE='{instance}'"

    connection   = engine.connect()
    query_string = query.format(datum=datum, instance=instance)
    results      = connection.execute(query_string).fetchall()
    connection.close()
    return results

def _convert_datum(datum):
    ''' Convert datum keys and values using the MAPPING file. '''
    new_datum = {}
    for key in MAPPING:
        name, conversion_function = MAPPING[key]
        new_datum[name] = conversion_function(datum[key])
    #return new_datum
    return Datum(**new_datum)

def _timestamp(value):
    ''' Convert time (s) to datetime instance. '''
    return datetime.utcfromtimestamp(value/1000.0)

NULL_CONVERSION = lambda x: x
MAPPING = {
             'ADDRESS_DATUMINSTANCE':('instance'          ,NULL_CONVERSION),
             'ADDRESS_SITE'         :('site'              ,NULL_CONVERSION),
             'ADDRESS_OBSERVATORY'  :('observatory'       ,NULL_CONVERSION),
             'ADDRESS_TELESCOPE'    :('telescope'         ,NULL_CONVERSION),
             'TIMESTAMP_'           :('timestamp_changed' ,_timestamp),
             'TIMESTAMPMEASURED'    :('timestamp_measured',_timestamp),
             'VALUE_'               :('value'             ,NULL_CONVERSION)
          }

Datum = collections.namedtuple('Datum', [ key for key,_ in MAPPING.values() ] )
