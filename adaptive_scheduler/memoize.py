#!/usr/bin/env python

'''
memoize.py - Transparent function and method caching.

description

Author: Eric Saunders
July 2013
'''

import functools
from types import MethodType
from dogpile.cache import make_region

region = make_region().configure(
    'dogpile.cache.dbm',
    expiration_time=86400,
    arguments={'filename': '/data/adaptive_scheduler/rise_set_cache.dbm',
               'rw_lockfile': False,
               'dogpile_lockfile': False}
)


def make_hashable(o):
    if isinstance(o, dict):
        return hash(repr(sorted(o.iteritems())))
    else:
        return hash(repr(o))


class Memoize(object):
    """Designed to work as function or method decorator """
    HASH_NO_ARGS   = make_hashable(())
    HASH_NO_KWARGS = make_hashable({})

    def __init__(self, resource, semester_start, semester_end, function):
        self.func = function
        self.resource = resource
        self.local_memory_cache = {}
        self.semester_start = semester_start
        self.semester_end = semester_end
        self.cached_func = self._memoize(function)

    def __call__(self, *args, **kw):
        return self.cached_func(*args, **kw)

    def __get__(self, instance, owner):
        return MethodType(self, instance, owner)

    def generate_key(self, hashable_args, hashable_kwargs):
        return str(self.resource) + '_' + str(self.semester_start) + '_' + str(self.semester_end) + '_' \
               + str(hashable_args) + '_' + str(hashable_kwargs)

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

            local_memory_key = (hashable_args, hashable_kwargs)
            if local_memory_key not in self.local_memory_cache:
                key = self.generate_key(hashable_args, hashable_kwargs)
                value = region.get(key, ignore_expiration=True)
                if not value:
                    value = obj(*args, **kwargs)
                    region.set(key, value)
                self.local_memory_cache[local_memory_key] = value
            else:
                value = self.local_memory_cache[local_memory_key]

            return value
        return memoizer
