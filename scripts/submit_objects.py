#!/usr/bin/env python

'''
submit_objects.py - Simple Request submission example

This example demonstrates how to create and submit a User Request to the
Request DB, using the object API of the Request client.

This is a work in progress, and feedback is desired! Please come and see me, or send me
email (esaunders@lcogt.net) if something is confusing or unclear, or if you have suggestions
for improvement.

Author: Eric Saunders
March 2013
'''

from reqdb.requests import Request, UserRequest
from reqdb.client   import SchedulerClient

'''
Proposal information is resolved from a DB lookup. Your user name should be an email
address registered with the system, and your proposal ID must exist (and you must have
authorisation to use it).
'''
proposal = {
             'proposal_id'   : 'LCOSchedulerTest',
             'user_id'       : 'zwalker@lcogt.net',
           }


'''
You may specify the target resource in as much detail as required. For example, providing a site
will ensure your Request will either be scheduled at that site, or not scheduled at all. However,
you are strongly encouraged to only provide telescope_class, if possible. This maximises the
flexibility of your Request for rescheduling, and therefore, your chance of getting your data.

Special note: If you provide an explicit instrument name, or set any other telescope-specific
attributes, you have no choice but to fully qualify the telescope here. If you don't, you may
submit invalid blocks to the POND.
'''
location = {
              # Required
              'telescope_class' : '2m0',
              # Optional
#              'site'            : 'lsc',
#              'observatory'     : 'doma',
#              'telescope'       : '1m0a',
            }

'''
Target parameters.
    * Targets always have equinox J2000.
    * Only ICRS coordinates are accepted.
    * At present only SIDEREAL is a supported type.
    * If you need automatic target name resolution, use the web interface at
      http://scheduler-dev.lco.gtn/odin/
'''
target = {
           # Required fields
           'name'              : 'HD 40307',    # Your name for your target
           'ra'                : 88.629,      # In decimal degrees
           'dec'               : +59.96,      # In decimal degrees
           # Optional fields. Defaults are as below.
           'proper_motion_ra'  : 0,             # In arcsecs/yr
           'proper_motion_dec' : 0,             # In arcsecs/yr
           'parallax'          : 0,             # In arcsecs
           'epoch'             : 2000,          # Not needed unless proper motion is also specified
         }

'''
Window parameters. The window tells the scheduler the range of times during which
it's ok to observe your Request. Think of it as the 'wiggle room' within which your
observation may be placed. The actual schedulable windows are typically smaller,
since they depend on darkness and when the target is up at each site.

You may have as many windows as you like, of any size. Your successfully scheduled Request
will ultimately only fall within ONE of the provided windows.

You should try to provide the largest window or set of windows that make sense for your
Request, since this maximises the chance of scheduling and rescheduling your observation.

The corollary is that if you specify a window within which your target cannot be observed
from any site, it will not be scheduled!
'''
# You may specify windows by start/end...
window1 = {
            'start'    : '2014-02-16 19:50:00',  # Time is UTC
            'end'      : '2014-02-20 22:00:00',
          }

# ...or with a duration


molecule = {
             # Required fields
             'exposure_time'   : 60,            # Exposure time, in secs
             'exposure_count'  : 10,              # The number of consecutive exposures
             'filter'          : 'R',            # The generic filter name
             # Optional fields. Defaults are as below.
             'type'            : 'EXPOSE',       # The type of the molecule
             'ag_name'         : 'kb03',             # Normally, this is resolved for you
             'ag_mode'         : 'Optional',
             'instrument_name' : 'em01',       # This resolves to the main science camera on the scheduled resource
#             'bin_x'           : 2,              # Your binning choice. Right now these need to be the same.
#             'bin_y'           : 2,
             'defocus'         : 0.0             # Mechanism movement of M2, or how much focal plane has moved (mm)
           }

# Future supported constraints will include lunar distance and phase, and seeing
constraints = {
#               'max_airmass' : 1.7,   # The maximum airmass you are willing to accept
            }

# Build the Request
req = Request()
req.set_location(location)
req.set_target(target)
req.add_window(window1)
req.add_molecule(molecule) # Molecules will be executed in the order in which they are added

# Set your constraints
req.set_constraints(constraints)

# Add the Request to the outer User Request
ur = UserRequest(group_id='2m em01 + specified AG')
ur.add_request(req)
ur.operator = 'single'
ur.set_proposal(proposal)

# You're done! Send the complete User Request to the DB for scheduling
#client        = SchedulerClient('http://scheduler-dev.lco.gtn/requestdb/')
client        = SchedulerClient('http://localhost:8001/')
response_data = client.submit(ur, keep_record=True, debug=True)
client.print_submit_response()

# Log in to the web interface at http://scheduler-dev.lco.gtn/odin/ to track the progress
# of your observations.

