'''
model.py - Data model of the adaptive scheduler.

This module provides the model objects which form the adaptive scheduler's domain.
It includes representations of targets, telescopes and observable time slots.

Author: Eric Saunders
November 2011
'''

# Required for true (non-integer) division
from __future__ import division

from adaptive_scheduler.exceptions import InvalidRequestError
from adaptive_scheduler.kernel.reservation_v2 import CompoundReservation_v2 as CompoundReservation


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


class Request(object):
    '''
        Represents a single valid configuration where an observation could take
        place. These are combined within a CompoundRequest to allow AND and OR
        semantics ("do this and this and this", "do this or this").

        target    - a Target object (pointing information)
        telescope - a Telescope object (lat/long information)
        priority  - the value the scheduler should place on this request
        duration - exposure time of each observation. TODO: Clarify what this means.
    '''

    def __init__(self, target, telescope, priority, duration):
        self.target    = target
        self.telescope = telescope
        self.priority  = priority
        self.duration  = duration



class CompoundRequest(object):
    '''
        A user-level request for an observation. This will be translated into the
        Reservation/CompoundReservation of the scheduling kernel.

        requests - a list of Request objects. There must be at least one.
        res_type - the type of request (single, and, oneof)
        windows  - a list of start/end datetimes, representing when this observation
                   is eligible to be performed. For user observations with no
                   time constraints, this should be the planning window of the
                   scheduler (e.g. the semester bounds).
    '''

    # TODO: Add sanity checking, e.g. requiring windows for AND blocks, etc.
    valid_types = CompoundReservation.valid_types

    def __init__(self, requests, res_type, windows):
        self.requests  = requests
        self.res_type  = self._validate_type(res_type)

        if len(windows) % 2 > 0:
            error_msg = ("You must provide a start and end for each window "
                         "(you provided an odd number of window edges)")
            raise InvalidRequestError(error_msg)

        self.windows = windows


    def _validate_type(self, provided_type):
        '''Check the type being asked for matches a valid type
           of CompoundObservation.'''

        if provided_type not in CompoundRequest.valid_types:

            error_msg = ("You've asked for a type of request that doesn't exist. "
                         "Valid types are:\n")

            for res_type, help_txt in CompoundRequest.valid_types.iteritems():
                error_msg += "    %9s - %s\n" % (res_type, help_txt)

            raise InvalidRequestError(error_msg)

        return provided_type


