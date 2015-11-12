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

DEFAULT_URL    = 'mysql://hibernate:hibernate@harvester2.lco.gtn/harvest'
DEFAULT_ENGINE = create_engine(DEFAULT_URL)

class ConnectionError(Exception):
    pass

def get_datum(datum, instance=None, engine=None, persistence_model=None, originator=None):
    ''' Get data from telemetry database, ordered by timestamp ascending (i.e.
        newest value is last). '''

    try:
        engine  = engine or DEFAULT_ENGINE
        results = _query_db(datum, originator, instance, engine, persistence_model)
        datums = [_convert_datum(datum) for datum in results]
        return datums

    except Exception as e:
        raise ConnectionError(e)

def _query_db(datum, originator, instance, engine, persistence_model=None):
    ''' Retrieve datum from database.

        This query uses a table populated from the SCRAPEVALUE table at each site.
    '''
    query = """SELECT * from PROPERTY as P
               join SCRAPEVALUE as SV on P.IDENTIFIER=SV.IDENTIFIER
               where P.ADDRESS_DATUM='{datum}' and P.ADDRESS_SITE!='tst'
            """
    if originator:
        query += "and P.ADDRESS_ORIGINATOR='{originator}'"
    if instance:
        query += "and P.ADDRESS_DATUMINSTANCE='{instance}'"
    if persistence_model:
        query += "and P.ADDRESS_PERSISTENCEMODEL='{persistence_model}'"

    query += "order by SV.TIMESTAMP_"
    connection   = engine.connect()
    query_string = query.format(datum=datum, originator=originator, instance=instance, persistence_model=persistence_model)
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
             'ADDRESS_PERSISTENCEMODEL':('persistence_model',NULL_CONVERSION),
             'ADDRESS_DATUMINSTANCE':('instance'          ,NULL_CONVERSION),
             'ADDRESS_SITE'         :('site'              ,NULL_CONVERSION),
             'ADDRESS_OBSERVATORY'  :('observatory'       ,NULL_CONVERSION),
             'ADDRESS_TELESCOPE'    :('telescope'         ,NULL_CONVERSION),
             'TIMESTAMP_'           :('timestamp_changed' ,_timestamp),
             'TIMESTAMPMEASURED'    :('timestamp_measured',_timestamp),
             'VALUE_'               :('value'             ,NULL_CONVERSION)
          }

Datum = collections.namedtuple('Datum', [ key for key,_ in MAPPING.values() ] )
