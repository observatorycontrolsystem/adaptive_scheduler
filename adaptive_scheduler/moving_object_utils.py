#!/usr/bin/env python
'''
moving_objects_utils.py - Utility methods for moving object targets

Mapping of orbital elements to their corresponding POND object types is tedious, but
follows a well-defined naming convention. This module automates that mapping.

To add a new orbital elements scheme, append to scheme_mappings. The corresponding
POND pointing method should exist (see below for naming convention).

Author: Eric Saunders
December 2013
'''

scheme_mappings = {
    'ASA_MAJOR_PLANET': ('name', 'scheme', 'epochofel', 'orbinc',
                         'longascnode', 'longofperih', 'meandist',
                         'eccentricity', 'meanlong', 'dailymot'),
    'ASA_MINOR_PLANET': ('name', 'scheme', 'epochofel', 'orbinc',
                         'longascnode', 'argofperih', 'meandist',
                         'eccentricity', 'meananom'),
    'ASA_COMET': ('name', 'scheme', 'epochofel', 'orbinc',
                  'longascnode', 'argofperih', 'perihdist',
                  'eccentricity', 'epochofperih'),
    'JPL_MAJOR_PLANET': ('name', 'scheme', 'epochofel', 'orbinc',
                         'longascnode', 'argofperih', 'meandist',
                         'eccentricity', 'meananom', 'dailymot'),
    'JPL_MINOR_PLANET': ('name', 'scheme', 'epochofel', 'orbinc',
                         'longascnode', 'argofperih', 'perihdist',
                         'eccentricity', 'epochofperih'),
    'MPC_MINOR_PLANET': ('name', 'scheme', 'epochofel', 'orbinc',
                         'longascnode', 'argofperih', 'meandist',
                         'eccentricity', 'meananom'),
    'MPC_COMET': ('name', 'scheme', 'epochofel', 'orbinc',
                  'longascnode', 'argofperih', 'perihdist',
                  'eccentricity', 'epochofperih')
}


def required_fields_from_scheme(scheme):
    if scheme.upper() not in scheme_mappings:
        msg = "Unknown orbital element scheme '%s'" % scheme
        raise InvalidElements(msg)

    return scheme_mappings[scheme.upper()]


class InvalidElements(Exception):
    pass
