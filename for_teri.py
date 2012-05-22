#!/usr/bin/env python

'''
for_teri.py - Bug with oneof?

I think there might be a bug with oneof handling. Try the code below and see what
you think.

I'm asking for a oneof CR, but I end up with both resources scheduled.

Incidently, both resources are at the same site, which is why they have the same
windows. That's not a bug!

Author: Eric Saunders
May 2012
'''

#from adaptive_scheduler.input import load_scheduler_input
from adaptive_scheduler.kernel.fullscheduler_v2 import FullScheduler_v2 as FullScheduler
from adaptive_scheduler.printing import print_compound_reservations, print_schedule

import cPickle
def load_scheduler_input(pickle_file):
    in_fh = open(pickle_file, 'r')

    to_schedule                 = cPickle.load(in_fh)
    resource_windows            = cPickle.load(in_fh)
    contractual_obligation_list = cPickle.load(in_fh)

    return to_schedule, resource_windows, contractual_obligation_list

scheduler_dump_file = 'to_schedule.pickle'

(to_schedule, resource_windows, 
             contractual_obs) = load_scheduler_input(scheduler_dump_file)


print_compound_reservations(to_schedule)

print "Before scheduling"
scheduler = FullScheduler(to_schedule, resource_windows, contractual_obs)

schedule = scheduler.schedule_all()
print "After scheduling"

print_schedule(schedule)
