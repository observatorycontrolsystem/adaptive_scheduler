#!/usr/bin/env python
from __future__ import division

from nose.tools import assert_equal, raises


from adaptive_scheduler.request_parser import TreeCollapser, InvalidTreeError


class TestTreeCollapse(object):

    def setup(self):
        pass


    def test_collapses_all_ands(self):
        input_tree = {
                         'timestamp': '2012-09-30 12:27:38',
                         'tracking_number': '0000000001',
                         'group_id' : 'g01',
                         'type'     : 'compound_request',
                         'expires'  : '2012-09-30 12:27:38',
                         'type'     : 'compound_request',
                         'operator' : 'and',
                         'expires'  : '2012-09-30 12:27:38',
                         'proposal' : {},
                         'requests' : [
                                          {
                                            'type'     : 'compound_request',
                                            'operator' : 'and',
                                            'requests' : [
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                         ]
                                          },
                                          {
                                            'type'     : 'compound_request',
                                            'operator'     : 'and',
                                            'requests' : [
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                         ]
                                          },
                                      ]
                     }

        expected_tree = {
                          'timestamp': '2012-09-30 12:27:38',
                          'tracking_number': '0000000001',
                          'group_id' : 'g01',
                          'type'     : 'compound_request',
                          'expires'  : '2012-09-30 12:27:38',
                          'type'     : 'compound_request',
                          'operator' : 'and',
                          'expires'  : '2012-09-30 12:27:38',
                          'proposal' : {},
                          'requests' : [
                                         { 'target' : 'blah',
                                           'type'   : 'request'},
                                         { 'target' : 'blah',
                                           'type'   : 'request'},
                                         { 'target' : 'blah',
                                           'type'   : 'request'},
                                         { 'target' : 'blah',
                                           'type'   : 'request'},
                                         { 'target' : 'blah',
                                           'type'   : 'request'},
                                         { 'target' : 'blah',
                                           'type'   : 'request'},
                                       ]
                        }

        self.tc = TreeCollapser(input_tree)
        self.tc.collapse_tree()
        assert_equal(expected_tree, self.tc.collapsed_tree)


    def test_collapses_all_oneofs(self):
        input_tree = {
                         'timestamp': '2012-09-30 12:27:38',
                         'tracking_number': '0000000001',
                         'group_id' : 'g01',
                         'type'     : 'compound_request',
                         'expires'  : '2012-09-30 12:27:38',
                         'type'     : 'compound_request',
                         'operator' : 'oneof',
                         'expires'  : '2012-09-30 12:27:38',
                         'proposal' : {},
                         'requests' : [
                                          {
                                            'operator' : 'oneof',
                                            'requests' : [
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                         ]
                                          },
                                          {
                                            'operator' : 'oneof',
                                            'requests' : [
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                         ]
                                          },
                                      ]
                     }

        expected_tree = {
                          'timestamp': '2012-09-30 12:27:38',
                          'tracking_number': '0000000001',
                          'group_id' : 'g01',
                          'type'     : 'compound_request',
                          'expires'  : '2012-09-30 12:27:38',
                          'type'     : 'compound_request',
                          'operator' : 'oneof',
                          'expires'  : '2012-09-30 12:27:38',
                          'proposal' : {},
                          'requests' : [
                                         { 'target' : 'blah',
                                           'type'   : 'request'},
                                         { 'target' : 'blah',
                                           'type'   : 'request'},
                                         { 'target' : 'blah',
                                           'type'   : 'request'},
                                         { 'target' : 'blah',
                                           'type'   : 'request'},
                                         { 'target' : 'blah',
                                           'type'   : 'request'},
                                         { 'target' : 'blah',
                                           'type'   : 'request'},
                                       ]
                        }

        self.tc = TreeCollapser(input_tree)
        self.tc.collapse_tree()
        assert_equal(expected_tree, self.tc.collapsed_tree)


    def test_collapses_single_and_ands(self):
        input_tree = {
                         'timestamp': '2012-09-30 12:27:38',
                         'tracking_number': '0000000001',
                         'group_id' : 'g01',
                         'type'     : 'compound_request',
                         'expires'  : '2012-09-30 12:27:38',
                         'type'     : 'compound_request',
                         'operator' : 'single',
                         'expires'  : '2012-09-30 12:27:38',
                         'proposal' : {},
                         'requests' : [
                                          {
                                            'operator' : 'and',
                                            'requests' : [
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             {
                                                               'operator' : 'and',
                                                               'requests' : [
                                                                               { 'target' : 'blah',
                                                                                 'type'   : 'request'},
                                                                            ]
                                                             },
                                                         ]
                                          },
                                      ]
                     }

        expected_tree = {
                          'timestamp': '2012-09-30 12:27:38',
                          'tracking_number': '0000000001',
                          'group_id' : 'g01',
                          'type'     : 'compound_request',
                          'expires'  : '2012-09-30 12:27:38',
                          'type'     : 'compound_request',
                          'operator' : 'and',
                          'expires'  : '2012-09-30 12:27:38',
                          'proposal' : {},
                          'requests' : [
                                           { 'target' : 'blah',
                                             'type'   : 'request'},
                                           { 'target' : 'blah',
                                             'type'   : 'request'},
                                           { 'target' : 'blah',
                                             'type'   : 'request'},
                                           { 'target' : 'blah',
                                             'type'   : 'request'},
                                       ]
                        }

        self.tc = TreeCollapser(input_tree)
        self.tc.collapse_tree()
        assert_equal(expected_tree, self.tc.collapsed_tree)


    def test_collapses_single_and_oneofs(self):
        input_tree = {
                         'timestamp': '2012-09-30 12:27:38',
                         'tracking_number': '0000000001',
                         'group_id' : 'g01',
                         'type'     : 'compound_request',
                         'operator' : 'single',
                         'expires'  : '2012-09-30 12:27:38',
                         'proposal' : {},
                         'requests' : [
                                          {
                                            'operator' : 'oneof',
                                            'requests' : [
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                         ]
                                          },
                                      ]
                     }

        expected_tree = {
                          'timestamp': '2012-09-30 12:27:38',
                          'tracking_number': '0000000001',
                          'group_id' : 'g01',
                          'type'     : 'compound_request',
                          'operator' : 'oneof',
                          'expires'  : '2012-09-30 12:27:38',
                          'proposal' : {},
                          'requests' : [
                                         { 'target' : 'blah',
                                           'type'   : 'request'},
                                         { 'target' : 'blah',
                                           'type'   : 'request'},
                                         { 'target' : 'blah',
                                           'type'   : 'request'},
                                       ]
                        }

        self.tc = TreeCollapser(input_tree)
        self.tc.collapse_tree()
        assert_equal(expected_tree, self.tc.collapsed_tree)



    def test_collapses_ands_and_singles(self):
        input_tree = {
                         'timestamp': '2012-09-30 12:27:38',
                         'tracking_number': '0000000001',
                         'group_id' : 'g01',
                         'type'     : 'compound_request',
                         'operator' : 'and',
                         'expires'  : '2012-09-30 12:27:38',
                         'proposal' : {},
                         'requests' : [
                                          {
                                            'operator' : 'and',
                                            'requests' : [
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                         ]
                                          },
                                          {
                                            'operator' : 'and',
                                            'requests' : [
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                         ]
                                          },
                                          {
                                            'operator' : 'single',
                                            'requests' : [
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                         ]
                                          },
                                      ]
                     }

        expected_tree = {
                          'timestamp': '2012-09-30 12:27:38',
                          'tracking_number': '0000000001',
                          'group_id' : 'g01',
                          'type'     : 'compound_request',
                          'operator' : 'and',
                          'expires'  : '2012-09-30 12:27:38',
                          'proposal' : {},
                          'requests' : [
                                           { 'target' : 'blah',
                                             'type'   : 'request'},
                                           { 'target' : 'blah',
                                             'type'   : 'request'},
                                           { 'target' : 'blah',
                                             'type'   : 'request'},
                                           { 'target' : 'blah',
                                             'type'   : 'request'},
                                           { 'target' : 'blah',
                                             'type'   : 'request'},
                                           { 'target' : 'blah',
                                             'type'   : 'request'},
                                       ]
                        }

        self.tc = TreeCollapser(input_tree)
        self.tc.collapse_tree()
        assert_equal(expected_tree, self.tc.collapsed_tree)


    def test_collapses_oneofs_and_singles(self):
        input_tree = {
                         'timestamp': '2012-09-30 12:27:38',
                         'tracking_number': '0000000001',
                         'group_id' : 'g01',
                         'type'     : 'compound_request',
                         'operator' : 'oneof',
                         'expires'  : '2012-09-30 12:27:38',
                         'proposal' : {},
                         'requests' : [
                                          {
                                            'operator' : 'oneof',
                                            'requests' : [
                                                           { 'target' : 'blah',
                                                             'type'   : 'request'},
                                                           { 'target' : 'blah',
                                                             'type'   : 'request'},
                                                           { 'target' : 'blah',
                                                             'type'   : 'request'},
                                                         ]
                                          },
                                          {
                                            'operator' : 'oneof',
                                            'requests' : [
                                                           { 'target' : 'blah',
                                                             'type'   : 'request'},
                                                           { 'target' : 'blah',
                                                             'type'   : 'request'},
                                                         ]
                                          },
                                          {
                                            'operator' : 'single',
                                            'requests' : [
                                                           { 'target' : 'blah',
                                                             'type'   : 'request'},
                                                         ]
                                          },
                                      ]
                     }

        expected_tree = {
                          'timestamp': '2012-09-30 12:27:38',
                          'tracking_number': '0000000001',
                          'group_id' : 'g01',
                          'type'     : 'compound_request',
                          'operator' : 'oneof',
                          'expires'  : '2012-09-30 12:27:38',
                          'proposal' : {},
                          'requests' : [
                                           { 'target' : 'blah',
                                             'type'   : 'request'},
                                           { 'target' : 'blah',
                                             'type'   : 'request'},
                                           { 'target' : 'blah',
                                             'type'   : 'request'},
                                           { 'target' : 'blah',
                                             'type'   : 'request'},
                                           { 'target' : 'blah',
                                             'type'   : 'request'},
                                           { 'target' : 'blah',
                                             'type'   : 'request'},
                                       ]
                        }

        self.tc = TreeCollapser(input_tree)
        self.tc.collapse_tree()
        assert_equal(expected_tree, self.tc.collapsed_tree)


    def test_cant_collapse_ands_and_oneofs(self):
        input_tree = {
                         'timestamp': '2012-09-30 12:27:38',
                         'tracking_number': '0000000001',
                         'group_id' : 'g01',
                         'type'     : 'compound_request',
                         'expires'  : '2012-09-30 12:27:38',
                         'operator' : 'and',
                         'proposal' : {},
                         'requests' : [
                                          {
                                            'operator' : 'oneof',
                                            'requests' : [
                                                           { 'target' : 'blah',
                                                             'type'   : 'request'},
                                                           { 'target' : 'blah',
                                                             'type'   : 'request'},
                                                           { 'target' : 'blah',
                                                             'type'   : 'request'},
                                                         ]
                                          },
                                          {
                                            'operator' : 'and',
                                            'requests' : [
                                                           { 'target' : 'blah',
                                                             'type'   : 'request'},
                                                           { 'target' : 'blah',
                                                             'type'   : 'request'},
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
                       'operator' : 'married',
                       'requests' : []
                     }

        self.tc = TreeCollapser(input_tree)
        self.tc.collapse_tree()


    @raises(InvalidTreeError)
    def test_rejects_invalid_single_at_top_level(self):
        input_tree = {
                         'timestamp': '2012-09-30 12:27:38',
                         'tracking_number': '0000000001',
                         'group_id' : 'g01',
                         'type'     : 'compound_request',
                         'expires'  : '2012-09-30 12:27:38',
                         'operator' : 'single',
                         'requests' : [
                                          {
                                            'operator' : 'oneof',
                                            'requests' : [
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                         ]
                                          },
                                          {
                                            'operator' : 'oneof',
                                            'requests' : [
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                         ]
                                          },
                                      ]
                     }


        self.tc = TreeCollapser(input_tree)
        self.tc.collapse_tree()


    @raises(InvalidTreeError)
    def test_rejects_invalid_single_at_lower_level(self):
        input_tree = {
                         'timestamp': '2012-09-30 12:27:38',
                         'tracking_number': '0000000001',
                         'group_id' : 'g01',
                         'type'     : 'compound_request',
                         'expires'  : '2012-09-30 12:27:38',
                         'operator' : 'oneof',
                         'proposal' : {},
                         'requests' : [
                                          {
                                            'type'     : 'compound_request',
                                            'operator' : 'single',
                                            'requests' : [
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                         ]
                                          },
                                          {
                                            'type'     : 'compound_request',
                                            'operator' : 'oneof',
                                            'requests' : [
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                             { 'target' : 'blah',
                                                               'type'   : 'request'},
                                                         ]
                                          },
                                      ]
                     }


        self.tc = TreeCollapser(input_tree)
        self.tc.collapse_tree()
