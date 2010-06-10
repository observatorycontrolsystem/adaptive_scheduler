#!/usr/bin/env python

from __future__ import division

from adaptive_scheduler.availability.domain import Availability
from adaptive_scheduler.availability.client import HardCodedClient
from adaptive_scheduler.kernel import Kernel


# Start the 'new observation' listener thread

# When we get a new ping...

# Acquire the current availability matrix (i.e. the plan)
plan_source = HardCodedClient()
plan        = plan_source.get_current_plan()

#print repr(plan)

print 'here'


while plan.has_targets():
    target = plan.pop()
    print str(target)
plan = plan_source.get_current_plan()

# Construct the new schedule for each site
kernel   = Kernel()
schedule = kernel.construct_schedule(plan)

# Send the schedule to each site
