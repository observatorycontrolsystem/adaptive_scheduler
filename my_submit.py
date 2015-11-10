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
Proposal information will eventually be resolved from a DB lookup. For now, you must provide
the correct credentials and priority for your project, just as happens with direct POND
submissions.
'''
proposal = {
             'proposal_id'   : 'LCOSchedulerTest',
             'user_id'       : 'esaunders@lcogt.net',
           }


'''
You may specify the target resource in as much detail as required. For example, providing a site
will ensure your Request will either be scheduled at that site, or not scheduled at all. However,
you are strongly encouraged to only provide telescope_class, if possible. This maximises the
flexibility of your Request for rescheduling.

Special note: If you provide an explicit instrument name, or set other telescope-specific
attributes, you have no choice but to fully qualify the telescope here. If you don't, you may
submit invalid blocks to the POND.
'''
location = {
              # Required
              'telescope_class' : '2m0',
              # Optional
            }

'''
Target parameters.
    * Targets always have equinox J2000.
    * Only ICRS coordinates are accepted.
    * At present only SIDEREAL is a supported type.
    * If you need automatic target name resolution, use the web interface.
'''
target = {
           # Required fields
           'name'              : 'NGC 2997',    # Your name for your target
           'ra'                : 146.4117,      # In decimal degrees
           'dec'               : -31.1911,      # In decimal degrees
           # Optional fields. Defaults are as below.
           'proper_motion_ra'  : 0,             # In milli arcsecs/yr
           'proper_motion_dec' : 0,             # In milli arcsecs/yr
           'parallax'          : 0,             # In milli arcsecs
           'epoch'             : 2000,          # Not needed unless proper motion is also specified
           'acquire_mode'      : 'ON',
           'rot_angle'         : 45.0,
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
            'start'    : '2014-11-01 20:30:00',  # Time is UTC
            'end'      : '2014-12-03 20:30:00',
          }

# ...or with a duration
#window2 = {
#            'start'    : '2013-03-26 13:30:00',
#            'duration' : '25:00:00',             # HRS:MINS:SECS
#          }


molecule = {
             # Required fields
             'exposure_time'   : 600,            # Exposure time, in secs
             'exposure_count'  : 2,              # The number of consecutive exposures
             'spectra_slit'    : 'FLOYDS_SLIT_DEFAULT',            # The generic filter name
             # Optional fields. Defaults are as below.
             'type'            : 'SPECTRUM',       # The type of the molecule
             'ag_mode'         : 'Optional',
             'instrument_name' : '2m0-FLOYDS-SCICAM',       # This resolves to the main science camera on the scheduled resource
           }

# Build the Request
req = Request()
req.set_location(location)
req.set_target(target)
req.add_window(window1)
#req.add_window(window2)
req.add_molecule(molecule) # Molecules will be executed in the order in which they are added

# Add the Request to the outer User Request
ur = UserRequest(group_id='Your phrase to describe this group')
ur.add_request(req)
ur.operator = 'single'
ur.set_proposal(proposal)

# You're done! Send the complete User Request to the DB for scheduling
client        = SchedulerClient('http://scheduler-dev.lco.gtn/requestdb/')
#client        = SchedulerClient('http://scheduler1.lco.gtn/requestdb/')
#client        = SchedulerClient('http://localhost:8001/')
response_data = client.submit(ur, keep_record=True, debug=True)
client.print_submit_response()
