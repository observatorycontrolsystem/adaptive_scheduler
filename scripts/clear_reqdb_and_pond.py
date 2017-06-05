#!/usr/bin/env python

'''
clear_scheduler_dbs.py - Clear RequestDB and POND

Running this script drops all Requests and associated objects from the RequestDB,
and all Blocks and associated objects from the POND.
Proposal and user information is left untouched.

Author: Eric Saunders
February 2014
'''

from sqlalchemy.engine import create_engine

REQUESTDB_URL    = 'mysql://root:tootyrooty@localhost/scheduler_requests'
REQUESTDB_ENGINE = create_engine(REQUESTDB_URL)

POND_URL    = 'mysql://root:tootyrooty@localhost/pond'
POND_ENGINE = create_engine(POND_URL)



def delete_rows(table_name, engine):

    query      = "DELETE FROM {table_name}"
    query_str  = query.format(table_name=table_name)

    connection = engine.connect()
    results    = connection.execute(query_str)
    connection.close()

    return results


def delete_all(tables, engine):
    connection = engine.connect()
    for table in tables:
        print "Deleting rows from table '%s'" % table
        delete_rows(table, engine)

def truncate_all(tables, engine):
    connection = engine.connect()

    disable_foreign_key_checking(connection)
    for table in tables:
        print "Truncating table '%s'" % table
        truncate_table(table, connection)

    connection.close()


def disable_foreign_key_checking(connection):
    query_str = "SET FOREIGN_KEY_CHECKS=0"
    results    = connection.execute(query_str)

    return results


def truncate_table(table_name, connection):

    query      = "TRUNCATE {table_name}"
    query_str  = query.format(table_name=table_name)

    results    = connection.execute(query_str)

    return results



if __name__ == '__main__':
    reqdb_tables = (
                     'service_constraints',
                     'service_molecules',
                     'service_target',
                     'service_windows',
                     'service_location',
                     'service_requests',
                     'service_userrequests',
                   )

    truncate_all(reqdb_tables, REQUESTDB_ENGINE)


    pond_tables = (
                    'pond_event',
                    'pond_molecule',
                    'pond_pointing',
                  )

    truncate_all(pond_tables, POND_ENGINE)


    # Can't truncate - stuck waiting for a metadata lock
    special_pond_tables_that_cant_be_truncated = (
                                                   'pond_block',
                                                 )
    delete_all(special_pond_tables_that_cant_be_truncated, POND_ENGINE)
