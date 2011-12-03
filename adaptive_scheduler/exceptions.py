'''
exceptions.py - Adaptive scheduler exceptions.

Author: Eric Saunders
December 2011
'''



class InvalidRequestError(Exception):
    '''Raised when a target is missing a key value (RA, Dec).'''

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value

