#!/usr/bin/env python

'''
for_teri.py - Bug report


Author: Eric Saunders
May 2012
'''

#from adaptive_scheduler.input import load_scheduler_input
from adaptive_scheduler.kernel.fullscheduler_v5 import FullScheduler_v5 as FullScheduler
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

time_slicing_dict = {
                        '0m4a.aqwa.bpl' : [0, 600],
                        '0m4b.aqwa.bpl' : [0, 600],
                        '1m0a.doma.elp' : [0, 600],
                        '1m0a.domb.lsc' : [0, 600],
                    }
print "Before scheduling"
scheduler = FullScheduler(to_schedule, resource_windows, contractual_obs,
                          time_slicing_dict)

schedule = scheduler.schedule_all()
print "After scheduling"

print_schedule(schedule)
