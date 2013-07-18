#!/usr/bin/env python

'''
memoize.py - Transparent function and method caching.

description

Author: Eric Saunders
July 2013
'''

import functools

def make_hashable(o):
    if isinstance(o, dict):
        return hash(repr(sorted(o.iteritems())))
    else:
        return hash(repr(o))


def make_hashable_old(o):
    if isinstance(o, (set, tuple, list)):
      return hash(frozenset([type(o)] + [make_hashable(e) for e in o]))

    elif isinstance(o, dict):
        hashable = {}
        for k, v in o.iteritems():
            hashable[k] = make_hashable(v)

        return hash(frozenset([type(o)] + hashable.items()))

    else:
      return hash(o)


def memoize(obj):
    ''' Decorate a function to provide transparent caching of pre-computed values.
        This function handles kwargs and will also inspect the contents of mutable
        types if provided.
        Adapted from http://wiki.python.org/moin/PythonDecoratorLibrary#Memoize'''
    cache = obj.cache = {}
    HASH_NO_ARGS   = make_hashable(())
    HASH_NO_KWARGS = make_hashable({})

    @functools.wraps(obj)
    def memoizer(*args, **kwargs):
        if args:
            hashable_args   = make_hashable(args)
        else:
            hashable_args   = HASH_NO_ARGS

        if kwargs:
            hashable_kwargs = make_hashable(kwargs)
        else:
            hashable_kwargs = HASH_NO_KWARGS

        key = (hashable_args, hashable_kwargs)
        if key not in cache:
            cache[key] = obj(*args, **kwargs)
        return cache[key]
    return memoizer



from types import MethodType

class CounterFunction(object):
    """Designed to work as function or method decorator """
    def __init__(self, function):
        self.func = function
        self.counter = 0
    def __call__(self, *args, **kw):
        self.counter += 1
        return self.func(*args, **kw)
    def __get__(self, instance, owner):
        return MethodType(self, instance, owner)


class Memoize(object):
    """Designed to work as function or method decorator """
    HASH_NO_ARGS   = make_hashable(())
    HASH_NO_KWARGS = make_hashable({})

    def __init__(self, function):
        self.func = function
        self.cache = {}
        self.cached_func = self._memoize(function)

        self.hit_cache    = 0
        self.missed_cache = 0

    def __call__(self, *args, **kw):
        return self.cached_func(*args, **kw)

    def __get__(self, instance, owner):
        return MethodType(self, instance, owner)

    def _memoize(self, obj):
        @functools.wraps(obj)
        def memoizer(*args, **kwargs):
            if args:
                hashable_args   = make_hashable(args)
            else:
                hashable_args   = Memoize.HASH_NO_ARGS

            if kwargs:
                hashable_kwargs = make_hashable(kwargs)
            else:
                hashable_kwargs = Memoize.HASH_NO_KWARGS
            key = (hashable_args, hashable_kwargs)
            if key not in self.cache:
                self.cache[key] = obj(*args, **kwargs)
                self.missed_cache += 1
            else:
                self.hit_cache += 1

            return self.cache[key]
        return memoizer


#@memoize
def fibonacci(n):
    '''I am the fibonacci docstring.'''
    if n in (0, 1):
        return n
    return fibonacci(n-1) + fibonacci(n-2)

#@memoize
def fibonacci_iterative(n):
    '''I am the fibonacci docstring.'''
    if n in (0, 1):
        return n

    x_first  = 0
    x_second = 1

    for i in range(2,n+1):
        x_next   = x_first + x_second
        x_first  = x_second
        x_second = x_next

    return x_next
