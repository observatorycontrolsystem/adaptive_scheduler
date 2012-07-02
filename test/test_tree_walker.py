#!/usr/bin/env python
from __future__ import division

from nose.tools import assert_equal


from adaptive_scheduler.tree_walker import ( collapse_tree, is_a_node, get_children,
                                             collapse_node, collapse_leaf )


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

        received_tree = collapse_tree(input_tree, is_a_node, get_children,
                                      collapse_node, collapse_leaf)
        assert_equal(expected_tree, received_tree)
