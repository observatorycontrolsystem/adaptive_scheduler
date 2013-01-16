#!/usr/bin/env python
'''
as2.py - Run the adaptive scheduler in single use, non-persistent mode.

This is most useful for testing purposes.

Author: Eric Saunders
July 2012
'''
from __future__ import division


from adaptive_scheduler.orchestrator import ( main, get_requests_from_file,
                                              get_requests_from_db )

from datetime import datetime


def filter_out_of_date_requests(requests):
    '''Temporary filter to prevent rescheduling of requests that can't happen.'''
    good_requests = []
    for r in requests:
        dt = datetime.strptime(r['requests'][0]['requests'][0]['windows'][0]['end'],
                               '%Y-%m-%d %H:%M:%S')
        if dt > datetime.utcnow():
            good_requests.append(r)

    return good_requests


if __name__ == '__main__':

    # Acquire and collapse the requests
    #requests = get_requests(url, telescope_class)
    #requests = get_requests_from_file('new_requests.dat', 'dummy arg')
    #requests = get_requests_from_file('human_readable_new_requests_new_format.dat', 'dummy arg')

    url = 'http://localhost:8001/'
    #telescope_class = '0m4'
    telescope_class = '1m0'
    requests = get_requests_from_db(url, telescope_class)

    print "Request DB gave us the following requests for telescope_class %s:" % telescope_class
    for r in requests:
        print r['tracking_number']

    print r

    possible_requests = filter_out_of_date_requests(requests)

    #good_requests = [good_requests[0]]
    print "%d requests are not yet expired." % len(possible_requests)

    if possible_requests:
        main(possible_requests)
    else:
        print "No requests to schedule. Aborting."


