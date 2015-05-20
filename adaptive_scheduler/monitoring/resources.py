'''
resource.py - Module for retrieving the network resource.

description

Author: Martin Norbury
May 2013
'''
import logging
LOGGER = logging.getLogger(__name__)

import ast


def get_site_resources(site):
    ''' Return a list of site resources by name. '''
    return [resource['name']
            for resource in _resources()
            if site in resource['name']]


def get_observatory_resources(site, observatory):
    ''' Return a list of observatory resources by name. '''
    return [resource['name']
            for resource in _resources()
            if '.'.join([observatory, site]) in resource['name']]


def _resources(filename='telescopes.dat'):
    ''' Read resources from file. '''
    LOGGER.debug("Loading resources from {0}".format(filename))
    with open(filename) as filep:
        return ast.literal_eval(filep.read())
