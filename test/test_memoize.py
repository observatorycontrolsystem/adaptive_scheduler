from nose.tools import assert_equal, assert_not_equal
from random import random

from adaptive_scheduler.memoize import Memoize, memoize, make_hashable

class TestMemoizeClass(object):

    def setup(self):
        class Target(object):
            def __init__(self, name):
                self.value = 6

            def target_manipulation(self, target):
                '''I am the docstring.'''
                return target['ra'] + random() + self.value

        self.target = Target('Eric')

        self.target_dict1  = { 'ra' : 4 }
        self.target_dict2  = { 'ra' : 8 }


    def test_new_values_are_cached(self):
        memoized_method = Memoize(self.target.target_manipulation)
        assert_equal(len(memoized_method.cache), 0)

        received_1 = memoized_method(self.target_dict1)
        received_2 = memoized_method(self.target_dict2)

        assert_equal(len(memoized_method.cache), 2)
        assert_not_equal(received_1, received_2)

    def test_new_values_are_not_cached_again(self):
        memoized_method = Memoize(self.target.target_manipulation)
        assert_equal(len(memoized_method.cache), 0)

        received_1 = memoized_method(self.target_dict1)
        received_2 = memoized_method(self.target_dict1)

        assert_equal(len(memoized_method.cache), 1)
        assert_equal(received_1, received_2)

    def test_caches_keyword_args(self):
        memoized_method = Memoize(self.target.target_manipulation)
        assert_equal(len(memoized_method.cache), 0)

        received_1 = memoized_method(target=self.target_dict1)
        received_2 = memoized_method(target=self.target_dict2)

        assert_equal(len(memoized_method.cache), 2)
        assert_not_equal(received_1, received_2)

#    def test_docstring_is_preserved_under_decoration(self):
#        memoized_method = Memoize(self.target.target_manipulation)
#        assert_equal(memoized_method.__doc__, 'I am the docstring.')



class TestMemoizeFunction(object):

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

        #assert_not_equal(make_hashable(l), make_hashable(d))
