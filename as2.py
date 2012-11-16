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


if __name__ == '__main__':

    # Acquire and collapse the requests
    #requests = get_requests(url, telescope_class)
    #requests = get_requests_from_file('new_requests.dat', 'dummy arg')
    #requests = get_requests_from_file('human_readable_new_requests_new_format.dat', 'dummy arg')

    url = 'http://localhost:8001/'
    telescope_class = '0m4'
    requests = get_requests_from_db(url, telescope_class)

    print "Request DB gave us the following requests for telescope_class %s:" % telescope_class
    for r in requests:
        print r

    main(requests)
