import functools

def make_hashable(o):

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

    @functools.wraps(obj)
    def memoizer(*args, **kwargs):
        hashable_args   = make_hashable(args)
        hashable_kwargs = make_hashable(kwargs)
        key = (hashable_args, hashable_kwargs)
        if key not in cache:
            cache[key] = obj(*args, **kwargs)
        return cache[key]
    return memoizer


@memoize
def fibonacci(n):
    '''I am the fibonacci docstring.'''
    if n in (0, 1):
        return n
    return fibonacci(n-1) + fibonacci(n-2)


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
    def __init__(self, function):
        self.func = function
        self.cache = {}
        self.cached_func = self._memoize(function)

    def __call__(self, *args, **kw):
        return self.cached_func(*args, **kw)

    def __get__(self, instance, owner):
        return MethodType(self, instance, owner)

    def _memoize(self, obj):
        @functools.wraps(obj)
        def memoizer(*args, **kwargs):
            hashable_args   = make_hashable(args)
            hashable_kwargs = make_hashable(kwargs)
            key = (hashable_args, hashable_kwargs)
            if key not in self.cache:
                self.cache[key] = obj(*args, **kwargs)
            return self.cache[key]
        return memoizer



from nose.tools import assert_equal, assert_not_equal
from random import random

class TestMemoize(object):

    def setup(self):
        @memoize
        def target_manipulation(target):
            '''I am the docstring.'''
            return target['ra'] + random()

        self.func = target_manipulation
        self.target  = { 'ra' : 4 }
        self.target2 = { 'ra' : 8 }


    def test_new_values_are_cached(self):
        assert_equal(len(self.func.cache), 0)

        self.func(self.target)
        self.func(self.target2)

        assert_equal(len(self.func.cache), 2)


    def test_new_values_are_not_cached_again(self):
        assert_equal(len(self.func.cache), 0)

        self.func(self.target)
        self.func(self.target)

        assert_equal(len(self.func.cache), 1)


    def test_caches_keyword_args(self):
        assert_equal(len(self.func.cache), 0)

        self.func(target=self.target)
        self.func(target=self.target)

        assert_equal(len(self.func.cache), 1)


    def test_docstring_is_preserved_under_decoration(self):
        assert_equal(self.func.__doc__, 'I am the docstring.')



class TestHashable(object):

    def setup(self):
        class o(object):
            pass

        self.o = o()


    def test_can_hash_immutables(self):
        immutables = (1, 1.1, True, 'a string', (), frozenset())
        for x in immutables:
            assert_equal(type(make_hashable(x)), type(1))


    def test_can_hash_dict(self):
        assert_equal(type(make_hashable({})), type(1))


    def test_can_hash_object(self):
        assert_equal(type(make_hashable(self.o)), type(1))


    def test_can_hash_dict_of_dicts(self):
        d = {
              'a' : {
                      'b' : 1
                    }
            }
        assert_equal(type(make_hashable(d)), type(1))


    def test_different_dict_contents_have_different_hashes(self):
        d1 = {
               'a' : {
                       'b' : 1
                     },
               1 : 33
             }
        d2 = {
               'z' : {
                       'b' : 1
                     },
               1 : 33
             }

        assert_not_equal(make_hashable(d1), make_hashable(d2))


    def test_same_dict_contents_have_same_hashes(self):
        d1 = {
               'a' : {
                       'b' : 1
                     },
               1 : 33
             }
        d2 = {
               'a' : {
                       'b' : 1
                     },
               1 : 33
             }

        assert_equal(make_hashable(d1), make_hashable(d2))


    def test_equal_content_different_datatypes_dont_match(self):
        l = []
        d = {}

        assert_not_equal(make_hashable(l), make_hashable(d))
