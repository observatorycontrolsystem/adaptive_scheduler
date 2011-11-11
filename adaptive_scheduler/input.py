#!/usr/bin/env python

'''
input.py - Read and marshal scheduling input (targets, telescopes, etc)

description

Author: Eric Saunders
November 2011
'''

from adaptive_scheduler.model import Telescope

import ast

def file_to_dicts(filename):
    fh = open(filename, 'r')
    data = fh.read()

    return ast.literal_eval(data)


def build_telescopes(filename):
    telescopes = []
    tel_dicts  = file_to_dicts(filename)

    for d in tel_dicts:
        telescopes.append(Telescope(d))

    return telescopes


def build_targets(filename):
    targets = []
    target_dicts  = file_to_dicts(filename)

    for d in target_dicts:
        targets.append(Target(d))

    return targets
