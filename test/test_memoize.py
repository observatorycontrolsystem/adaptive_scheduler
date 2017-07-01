from nose.tools import assert_equal, assert_not_equal, assert_false
from random import random
from mock import patch

from adaptive_scheduler.memoize import Memoize, make_hashable
import adaptive_scheduler.memoize

from dogpile.cache import make_region
from dogpile.cache.api import NO_VALUE


local_region = make_region().configure(
    'dogpile.cache.memory',
    expiration_time=86400,
)

class TestMemoizeClass(object):

    def setup(self):
        class Target(object):
            def __init__(self, name):
                self.value = 6

            def test_manipulation(self, parameter):
                return str(parameter) + str(self.value)

            def target_manipulation(self, target):
                '''I am the docstring.'''
                return target['ra'] + random() + self.value

        self.target = Target('Eric')

        self.test_parameter1 = 1
        self.test_parameter2 = 'two'

        self.target_dict1  = { 'ra' : 4 }
        self.target_dict2  = { 'ra' : 8 }

    @patch('adaptive_scheduler.memoize.region', local_region)
    def test_new_values_are_cached(self):
        memoized_method = Memoize('resource1', self.target.test_manipulation)
        hashable_args1 = str(make_hashable((self.test_parameter1,)))
        hashable_args2 = str(make_hashable((self.test_parameter2,)))
        key1 = memoized_method.generate_key(hashable_args1, str(make_hashable({})))
        key2 = memoized_method.generate_key(hashable_args2, str(make_hashable({})))

        cache_value = local_region.get(key1)
        assert_equal(cache_value, NO_VALUE)
        cache_value = local_region.get(key2)
        assert_equal(cache_value, NO_VALUE)

        memoized_method(self.test_parameter1)
        memoized_method(self.test_parameter2)

        cache_value = local_region.get(key1)
        assert_false(cache_value == NO_VALUE)
        cache_value = local_region.get(key2)
        assert_false(cache_value == NO_VALUE)


    @patch('adaptive_scheduler.memoize.region', local_region)
    def test_caches_keyword_args(self):
        memoized_method = Memoize('resource2', self.target.target_manipulation)
        hashable_kwargs1 = str(make_hashable(dict(target=self.target_dict1)))
        hashable_kwargs2 = str(make_hashable(dict(target=self.target_dict2)))
        key1 = memoized_method.generate_key(str(make_hashable(())), hashable_kwargs1)
        key2 = memoized_method.generate_key(str(make_hashable(())), hashable_kwargs2)

        cache_value = local_region.get(key1)
        assert_equal(cache_value, NO_VALUE)
        cache_value = local_region.get(key2)
        assert_equal(cache_value, NO_VALUE)

        memoized_method(target=self.target_dict1)
        memoized_method(target=self.target_dict2)

        cache_value = local_region.get(key1)
        assert_false(cache_value == NO_VALUE)
        cache_value = local_region.get(key2)
        assert_false(cache_value == NO_VALUE)


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

        #assert_not_equal(make_hashable(l), make_hashable(d))
