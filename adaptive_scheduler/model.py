'''
model.py - Data model of the adaptive scheduler.

This module provides the model objects which form the adaptive scheduler's domain.
It includes representations of targets, telescopes and observable time slots.

Author: Eric Saunders
November 2011
'''

# Required for true (non-integer) division
from __future__ import division


class Target(object):
