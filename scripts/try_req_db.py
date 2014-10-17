#!/usr/bin/env python

'''
try_req_db.py - Testing request filtering from Req DB

description

Author: Eric Saunders
January 2013
'''

import json
from reqdb.client import SearchQuery, SchedulerClient


url = 'http://localhost:8001/'
telescope_class = '1m0'

# Content of get_requests_from_db()
search = SearchQuery()
#search.set_location(telescope_class=telescope_class)

# Retrieves entire tree - not expected
#search.set_date(date='2012-03-26 13:37:43')

# Retrieves one result, as expected
search.set_date(date='2013-01-29 20:16:58')

sc = SchedulerClient(url)
json_ur_list = sc.retrieve(search, debug=True)
ur_list = json.loads(json_ur_list)



print "Request DB gave us the following requests for telescope_class %s:" % telescope_class
for ur in ur_list:
    print ur['tracking_number']
    for r in ur['requests']:
        for r_data in r['requests']:
            print "    location: %s" % r_data['location']
            print "    windows: %s"  % r_data['windows']
            #print r_data.keys()

