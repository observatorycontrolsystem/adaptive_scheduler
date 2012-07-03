#!/usr/bin/env python
from __future__ import division

from nose.tools import assert_equal, raises


from adaptive_scheduler.tree_walker import TreeCollapser, InvalidTreeError


class TestTreeCollapse(object):

    def setup(self):
        pass


    def test_collapses_all_ands(self):
        input_tree = {
                         'type'     : 'and',
                         'requests' : [
                                          {
                                            'type'     : 'and',
                                            'requests' : [
                                                             { 'target' : 'blah' },
                                                             { 'target' : 'blah' },
                                                             { 'target' : 'blah' },
                                                         ]
                                          },
                                          {
                                            'type'     : 'and',
                                            'requests' : [
                                                             { 'target' : 'blah' },
                                                             { 'target' : 'blah' },
                                                             { 'target' : 'blah' },
                                                         ]
                                          },
                                      ]
                     }

        expected_tree = {
                          'type'     : 'and',
                          'requests' : [
                                           { 'target' : 'blah'},
                                           { 'target' : 'blah'},
                                           { 'target' : 'blah'},
                                           { 'target' : 'blah'},
                                           { 'target' : 'blah'},
                                           { 'target' : 'blah'},
                                       ]
                        }

        self.tc = TreeCollapser(input_tree)
        self.tc.collapse_tree()
        assert_equal(expected_tree, self.tc.collapsed_tree)


    def test_collapses_all_oneofs(self):
        input_tree = {
                         'type'     : 'oneof',
                         'requests' : [
                                          {
                                            'type'     : 'oneof',
                                            'requests' : [
                                                             { 'target' : 'blah' },
                                                             { 'target' : 'blah' },
                                                             { 'target' : 'blah' },
                                                         ]
                                          },
                                          {
                                            'type'     : 'oneof',
                                            'requests' : [
                                                             { 'target' : 'blah' },
                                                             { 'target' : 'blah' },
                                                             { 'target' : 'blah' },
                                                         ]
                                          },
                                      ]
                     }

        expected_tree = {
                          'type'     : 'oneof',
                          'requests' : [
                                           { 'target' : 'blah'},
                                           { 'target' : 'blah'},
                                           { 'target' : 'blah'},
                                           { 'target' : 'blah'},
                                           { 'target' : 'blah'},
                                           { 'target' : 'blah'},
                                       ]
                        }

        self.tc = TreeCollapser(input_tree)
        self.tc.collapse_tree()
        assert_equal(expected_tree, self.tc.collapsed_tree)


    def test_collapses_ands_and_singles(self):
        input_tree = {
                         'type'     : 'and',
                         'requests' : [
                                          {
                                            'type'     : 'and',
                                            'requests' : [
                                                             { 'target' : 'blah' },
                                                             { 'target' : 'blah' },
                                                             { 'target' : 'blah' },
                                                         ]
                                          },
                                          {
                                            'type'     : 'and',
                                            'requests' : [
                                                             { 'target' : 'blah' },
                                                             { 'target' : 'blah' },
                                                         ]
                                          },
                                          {
                                            'type'     : 'single',
                                            'requests' : [
                                                             { 'target' : 'blah' },
                                                         ]
                                          },
                                      ]
                     }

        expected_tree = {
                          'type'     : 'and',
                          'requests' : [
                                           { 'target' : 'blah'},
                                           { 'target' : 'blah'},
                                           { 'target' : 'blah'},
                                           { 'target' : 'blah'},
                                           { 'target' : 'blah'},
                                           { 'target' : 'blah'},
                                       ]
                        }

        self.tc = TreeCollapser(input_tree)
        self.tc.collapse_tree()
        assert_equal(expected_tree, self.tc.collapsed_tree)


    def test_cant_collapse_ands_and_oneofs(self):
        input_tree = {
                         'type'     : 'and',
                         'requests' : [
                                          {
                                            'type'     : 'oneof',
                                            'requests' : [
                                                             { 'target' : 'blah' },
                                                             { 'target' : 'blah' },
                                                             { 'target' : 'blah' },
                                                         ]
                                          },
                                          {
                                            'type'     : 'and',
                                            'requests' : [
                                                             { 'target' : 'blah' },
                                                             { 'target' : 'blah' },
                                                         ]
                                          },
                                      ]
                     }


        self.tc = TreeCollapser(input_tree)
        self.tc.collapse_tree()
        assert_equal(self.tc.is_collapsible, False)
        assert_equal(input_tree, self.tc.collapsed_tree)


    @raises(InvalidTreeError)
    def test_rejects_invalid_type(self):
        input_tree = {
                       'type' : 'married',
                       'requests' : []
                     }

        self.tc = TreeCollapser(input_tree)
        self.tc.collapse_tree()


    @raises(InvalidTreeError)
    def test_rejects_invalid_single_at_top_level(self):
        input_tree = {
                         'type'     : 'single',
                         'requests' : [
                                          {
                                            'type'     : 'oneof',
                                            'requests' : [
                                                             { 'target' : 'blah' },
                                                             { 'target' : 'blah' },
                                                         ]
                                          },
                                          {
                                            'type'     : 'oneof',
                                            'requests' : [
                                                             { 'target' : 'blah' },
                                                             { 'target' : 'blah' },
                                                         ]
                                          },
                                      ]
                     }


        self.tc = TreeCollapser(input_tree)
        self.tc.collapse_tree()


    @raises(InvalidTreeError)
    def test_rejects_invalid_single_at_lower_level(self):
        input_tree = {
                         'type'     : 'oneof',
                         'requests' : [
                                          {
                                            'type'     : 'single',
                                            'requests' : [
                                                             { 'target' : 'blah' },
                                                             { 'target' : 'blah' },
                                                             { 'target' : 'blah' },
                                                         ]
                                          },
                                          {
                                            'type'     : 'oneof',
                                            'requests' : [
                                                             { 'target' : 'blah' },
                                                             { 'target' : 'blah' },
                                                         ]
                                          },
                                      ]
                     }


        self.tc = TreeCollapser(input_tree)
        self.tc.collapse_tree()
