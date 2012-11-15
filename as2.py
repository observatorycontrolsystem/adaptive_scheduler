#!/usr/bin/env python
'''
as2.py - Run the adaptive scheduler in single use, non-persistent mode.

This is most useful for testing purposes.

Author: Eric Saunders
July 2012
'''
from __future__ import division


from adaptive_scheduler.orchestrator import main, get_requests_from_file


if __name__ == '__main__':

    # Acquire and collapse the requests
    #requests = get_requests(url, telescope_class)
    requests = get_requests_from_file('new_requests.dat', 'dummy arg')
    main(requests)
