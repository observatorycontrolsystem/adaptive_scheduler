#!/usr/bin/env python

'''
read_dump.py - summary line

description

Author: Eric Saunders
June 2013
'''


import json
import sys

first_fh = open(sys.argv[1], 'r')
first_str = first_fh.read()
first_fh.close()

second_fh = open(sys.argv[2], 'r')
second_str = second_fh.read()
second_fh.close()

first_unpickled = json.loads(first_str)
second_unpickled = json.loads(second_str)



for key in first_unpickled:
    print key, first_unpickled[key] == second_unpickled[key]

print "@@@@@@@"


first_strs  = [str(x) for x in first_unpickled['to_schedule']]
second_strs = [str(x) for x in second_unpickled['to_schedule']]

first_set  = set(first_strs)
second_set = set(second_strs)

print first_set
print len(first_set)
print second_set
print first_set - second_set
print second_set - first_set

#for key, value in first_unpickled['to_schedule']:
#    print key, repr(first_unpickled['global_windows'][key]) == repr(second_unpickled['global_windows'][key])

#for key in first_unpickled['global_windows']:
#    print key, repr(first_unpickled['global_windows'][key]) == repr(second_unpickled['global_windows'][key])


print "@@@@@@@"

x = repr(first_unpickled['global_windows']['1m0a.doma.cpt'])
y = repr(second_unpickled['global_windows']['1m0a.doma.cpt'])

#for key in x:
    #print key, x == y


#print first_unpickled['global_windows']
#print second_unpickled['global_windows']
