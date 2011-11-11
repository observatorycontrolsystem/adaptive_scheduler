'''
model.py - Data model of the adaptive scheduler.

This module provides the model objects which form the adaptive scheduler's domain.
It includes representations of targets, telescopes and observable time slots.

Author: Eric Saunders
November 2011
'''

# Required for true (non-integer) division
from __future__ import division


class DataContainer(object):
    def __init__(self, *initial_data, **kwargs):
        for dictionary in initial_data:
            for key in dictionary:
                setattr(self, key, dictionary[key])
        for key in kwargs:
            setattr(self, key, kwargs[key])


class Target(DataContainer):
    pass


class Telescope(DataContainer):
    pass
