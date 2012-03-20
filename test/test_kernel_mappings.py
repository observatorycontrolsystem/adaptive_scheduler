#!/usr/bin/python
from __future__ import division

from adaptive_scheduler.model import Telescope, Target, Request
from adaptive_scheduler.kernel_mappings import (construct_visibilities,
                                                make_dark_up_kernel_intervals)

from datetime import datetime


class TestKernelMappings(object):

    def setup(self):
        pass


    def test_make_dark_up_kernel_intervals(self):
        semester_start = datetime(2011, 11, 1, 0, 0, 0)
        semester_end   = datetime(2011, 11, 8, 0, 0, 0)

        tels = {
                 '1m0a.doma.bpl' :
                                   Telescope(
                                              name      = '1m0a.doma.bpl',
                                              tel_class = '1m0',
                                              latitude  = 34.433157,
                                              longitude = -119.86308,
                                              horizon   = 25,
                                            )
               }

        target = Target(
                         ra  = '20 41 25.91',
                         dec = '+45 16 49.22',
                       )

        dt_windows = [(semester_start, semester_end)]

        req  = Request(
                       target    = target,
                       telescope = tels['1m0a.doma.bpl'],
                       molecule  = 'insert molecule here',
                       windows   = dt_windows,
                       duration  = 300,
                      )

        visibility_from = construct_visibilities(tels, semester_start, semester_end)

        make_dark_up_kernel_intervals(req, visibility_from)
