#!/usr/bin/env python
'''
as2.py - summary line

description

Author: Eric Saunders
July 2012
'''
from __future__ import division


from adaptive_scheduler.orchestrator import main, get_requests_from_file


if __name__ == '__main__':

    # Acquire and collapse the requests
    #requests = get_requests(url, telescope_class)
    requests = get_requests_from_file('requests.dat', 'dummy arg')
    main(requests)
