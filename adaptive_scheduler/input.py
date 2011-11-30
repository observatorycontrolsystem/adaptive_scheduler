#!/usr/bin/env python

'''
input.py - Read and marshal scheduling input (targets, telescopes, etc)

description

Author: Eric Saunders
November 2011
'''

from adaptive_scheduler.model import Telescope, Target
from rise_set.sky_coordinates import RightAscension, Declination
from rise_set.angle           import Angle

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



def target_to_rise_set_target(target):
    '''
        Convert scheduler Target to rise_set target dict.
        TODO: Move scheduler Target code to rise_set.
        TODO: Change to default_dict, expand to allow proper motion etc.
    '''

    target_dict = {
                    'ra'    : RightAscension(target.ra),
                    'dec'   : Declination(target.dec),
                    'epoch' : target.epoch,
                   }

    return target_dict


def telescope_to_rise_set_telescope(telescope):
    '''
        Convert scheduler Telescope to rise_set telescope dict.
        TODO: Move scheduler Telescope code to rise_set.
    '''

    telescope_dict = {
                        'latitude'  : Angle(degrees=telescope.latitude),
                        'longitude' : Angle(degrees=telescope.longitude),
                      }

    return telescope_dict
