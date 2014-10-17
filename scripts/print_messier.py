#!/usr/bin/env python

'''
print_messier.py - Pretty print the contents of the messier dict

description

Author: Eric Saunders
July 2012
'''

import ast


def load(path_to_messier):

    fh = open(path_to_messier, 'r')

    messier_objects = ast.literal_eval(fh.read())

    return messier_objects




path_to_messier = 'messier_catalog.dict'

obj = load(path_to_messier)

for name, data in obj.iteritems():
    print name
    print data['ra']
    print data['dec']
    print
