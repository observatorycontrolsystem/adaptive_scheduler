#!/usr/bin/env python

from __future__ import division

from nose.tools import assert_equal

from adaptive_scheduler.tree_walker import TupleMaxDepthFinder, RequestMaxDepthFinder


class TestMaxDepthFinder(object):

    def setup(self):
        pass


    def test_max_depth_one_level(self):
        tree = (
                 1, 2,
               )

        mdf = TupleMaxDepthFinder(tree)
        mdf.walk()

        assert_equal(mdf.max_depth, 1)


    def test_max_depth_three_levels(self):
        tree = (
                 1, 2, (
                         3, 4, (
                                 5, 6,
                               )
                       ),
                       (
                         7, 8,
                       ),
               )

        mdf = TupleMaxDepthFinder(tree)
        mdf.walk()

        assert_equal(mdf.max_depth, 3)


    def test_with_a_real_request(self):
        dict_repr = {
                     'expires': u'2012-09-30 12:27:38',
                     'operator': u'single',
                     'proposal': {u'group_id': u'g01',
                                  u'id': 2,
                                  u'proposal_id': u'201205N2',
                                  u'proposal_name': u'Test Two',
                                  u'tag_id': u'3507abc',
                                  u'user_id': u'u01',
                                  u'user_name': u'User One'},
                     'requests': [{u'location': {u'id': 7,
                                                 u'observatory': u'doma',
                                                 u'request_id': 7,
                                                 u'site': u'bpl',
                                                 u'telescope': u'01ma',
                                                 u'telescope_class': u'1m0'},
                                   u'molecules': [{u'bin_x': None,
                                                   u'bin_y': None,
                                                   u'defocus': None,
                                                   u'expose_at': None,
                                                   u'exposure_count': None,
                                                   u'exposure_time': 800.0,
                                                   u'filter': None,
                                                   u'id': 11,
                                                   u'instrument_name': None,
                                                   u'request_id': 7,
                                                   u'type': u's'}],
                                   u'target': {u'dec': u'23 45 67',
                                               u'epoch': None,
                                               u'id': 7,
                                               u'name': u'andromeda',
                                               u'parallax': None,
                                               u'proper_motion_dec': None,
                                               u'proper_motion_ra': None,
                                               u'ra': u'23 34 45',
                                               u'request_id': 7},
                                   u'windows': [{u'end': None,
                                                 u'id': 10,
                                                 u'request_id': 7,
                                                 u'start': u'2012-07-02 17:27:38'}]}]
                    }

        mdf = RequestMaxDepthFinder(dict_repr)
        mdf.walk()

        assert_equal(mdf.max_depth, 1)

