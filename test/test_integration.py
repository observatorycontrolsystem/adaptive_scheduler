#!/usr/bin/python
from __future__ import division

from datetime import datetime, timedelta

from nose.tools import assert_true

# Import the modules to test
from adaptive_scheduler.model2      import (
                                             SiderealTarget, Telescope,
                                             Proposal, MoleculeFactory,
                                             Request, UserRequest,
                                             Windows, Window, Constraints,
                                             TelescopeNetwork)

from test_scheduler import create_scheduler_input_factory
from adaptive_scheduler.kernel.fullscheduler_gurobi import FullScheduler_gurobi
from adaptive_scheduler.scheduler_input import  SchedulerParameters
from adaptive_scheduler.scheduler import LCOGTNetworkScheduler

from mock import Mock

class TestIntegration(object):
    '''Unit tests for the adaptive scheduler Request object.'''

    def setup(self):
        self.target = SiderealTarget(
                                      name  = 'deneb',
                                      #ra  = '20 41 25.91',
                                      #dec = '+45 16 49.22',
                                      ra  = 310.35795833333333,
                                      dec = 45.280338888888885,
                                      epoch = 2000,
                                     )

        self.telescope = Telescope(
                                    name      = '1m0a.doma.ogg',
                                    latitude  = 20.7069444444,
                                    longitude = -156.258055556,
                                    tel_class = '1m0',
                                    horizon = 15,
                                    status = 'online',
                                    ha_limit_neg = -4.6,
                                    ha_limit_pos = 4.6,
                                  )
        self.telescopes = {'1m0a.doma.ogg': self.telescope}
        self.telescope_network = TelescopeNetwork(self.telescopes)

        self.proposal = Proposal(
                                  proposal_name  = 'LCOSchedulerTest',
                                  user           = 'Eric Saunders',
                                  tag            = 'admin',
                                  time_remaining = 10,               # In hours
                                  priority       = 1
                                )

        self.mol_factory = MoleculeFactory()

        self.molecule = self.mol_factory.build(
                                                dict(
                                                  type            = 'expose',
                                                  exposure_count  = 1,
                                                  bin_x           = 2,
                                                  bin_y           = 2,
                                                  instrument_name = 'KB12',
                                                  filter          = 'b',
                                                  exposure_time   = 60*25,
                                                  priority        = 1
                                                )
                                              )
        self.constraints = Constraints({})

        self.base_time = datetime(2016, 9, 14, 6, 0)

        self.semester_start = datetime(2011, 11, 1, 0, 0, 0)
        self.semester_end   = datetime(2011, 11, 8, 0, 0, 0)
        self.resource_1 = Mock()
        self.resource_1.name = '1m0a.doma.ogg'
        self.window_1 = Window({'start': self.base_time, 'end': self.base_time + timedelta(hours=0, minutes=30)}, self.resource_1)
        self.windows_1 = Windows()
        self.windows_1.append(self.window_1)
        self.resource_2 = Mock()
        self.resource_2.name = '1m0a.doma.ogg'
        self.window_2 = Window({'start': self.base_time + timedelta(hours=0, minutes=30), 'end': self.base_time + timedelta(hours=1, minutes=0)}, self.resource_2)
        self.windows_2 = Windows()
        self.windows_2.append(self.window_2)
        self.resource_3 = Mock()
        self.resource_3.name = '1m0a.doma.ogg'
        self.window_3 =  Window({'start': self.base_time + timedelta(hours=1, minutes=0), 'end': self.base_time + timedelta(hours=1, minutes=30)}, self.resource_3)
        self.windows_3 = Windows()
        self.windows_3.append(self.window_3)

        self.request_1 = Request(target         = self.target,
                          molecules      = [self.molecule],
                          windows        = self.windows_1,
                          constraints    = self.constraints,
                          request_number = '0000000001',
                          observation_type = 'NORMAL',
                          instrument_type = '1M0-SCICAM-SBIG')

        self.request_2 = Request(target         = self.target,
                          molecules      = [self.molecule],
                          windows        = self.windows_2,
                          constraints    = self.constraints,
                          request_number = '0000000002',
                          observation_type = 'NORMAL',
                          instrument_type='1M0-SCICAM-SBIG')

        self.request_3 = Request(target         = self.target,
                          molecules      = [self.molecule],
                          windows        = self.windows_2,
                          constraints    = self.constraints,
                          request_number = '0000000003',
                          observation_type = 'NORMAL',
                          instrument_type='1M0-SCICAM-SBIG')

        self.request_4 = Request(target         = self.target,
                          molecules      = [self.molecule],
                          windows        = self.windows_3,
                          constraints    = self.constraints,
                          request_number = '0000000004',
                          observation_type = 'NORMAL',
                          instrument_type='1M0-SCICAM-SBIG')

        self.user_and_request_1 = UserRequest('and', [self.request_1, self.request_2], self.proposal,
                                                  datetime(2050, 1, 1), '0000000001', 1.0, 'ur 1')
        self.user_and_request_2 = UserRequest('and', [self.request_3, self.request_4], self.proposal,
                                                  datetime(2050, 1, 1), '0000000002', 1.0, 'ur 2')

        self.user_many_request_1 = UserRequest('many', [self.request_1, self.request_2], self.proposal,
                                                  datetime(2050, 1, 1), '0000000003', 1.5, 'ur 3')
        self.user_many_request_2 = UserRequest('many', [self.request_3, self.request_4], self.proposal,
                                                  datetime(2050, 1, 1), '0000000004', 1.5, 'ur 4')


    def _schedule_requests(self, too_ur_list, normal_ur_list, scheduler_time = self.base_time - timedelta(days=1)):
        sched_params = SchedulerParameters(run_once=True, dry_run=True)
        event_bus_mock = Mock()
        scheduler = LCOGTNetworkScheduler(FullScheduler_gurobi, sched_params, event_bus_mock, self.telescopes)
        network_interface_mock = Mock()
        network_interface_mock.cancel = Mock(return_value=0)
        network_interface_mock.save = Mock(return_value=0)
        network_interface_mock.get_current_events = Mock(return_value={})

        mock_input_factory = create_scheduler_input_factory(too_ur_list, normal_ur_list)

        scheduler_input = mock_input_factory.create_normal_scheduling_input()
        scheduler_input.scheduler_now = scheduler_time
        scheduler_input.estimated_scheduler_end = scheduler_time + timedelta(minutes=15)

        result = scheduler.run_scheduler(scheduler_input, scheduler_time + timedelta(minutes=15))

        return result


    def test_competing_and_requests(self):
        result = self._schedule_requests([], [self.user_and_request_1, self.user_and_request_2],
                                         self.base_time - timedelta(hours=10))

        scheduled_urs = result.get_scheduled_requests_by_tracking_num()

        # assert that either user request 1 or user request 2 were scheduled in full, with the other not being scheduled
        if '0000000001' in scheduled_urs:
            # check that ur 1s requests are scheduled
            assert '0000000001' in scheduled_urs['0000000001']
            assert '0000000002' in scheduled_urs['0000000001']
            # and check that ur 2 is not scheduled
            assert '0000000002' not in scheduled_urs
        else:
            assert '0000000004' in scheduled_urs['0000000002']
            assert '0000000003' in scheduled_urs['0000000002']
            assert '0000000001' not in scheduled_urs

    def test_competing_many_requests(self):
        result = self._schedule_requests([], [self.user_many_request_1, self.user_many_request_2],
                                         self.base_time - timedelta(hours=10))
        scheduled_urs = result.get_scheduled_requests_by_tracking_num()

        # assert that user request 3 request 1 and user request 4 request 4 were scheduled ,
        # along with one of either 3-2 or 4-3.
        assert '0000000003' in scheduled_urs
        assert '0000000004' in scheduled_urs
        assert '0000000001' in scheduled_urs['0000000003']
        assert '0000000004' in scheduled_urs['0000000004']
        if '0000000002' in scheduled_urs['0000000003']:
            assert '0000000003' not in scheduled_urs['0000000004']
        else:
            assert '0000000002' not in scheduled_urs['0000000003']

    def test_competing_many_and_requests(self):
        normal_request_list = [self.user_and_request_1, self.user_many_request_2]
        result = self._schedule_requests([], normal_request_list, self.base_time - timedelta(hours=10))
        scheduled_urs = result.get_scheduled_requests_by_tracking_num()

        # assert the and request was taken in full, and the remaining many ur 4 request 2 was scheduled
        assert '0000000001' in scheduled_urs
        assert '0000000001' in scheduled_urs['0000000001']
        assert '0000000002' in scheduled_urs['0000000001']
        assert '0000000004' in scheduled_urs
        # the second request from the many was scheduled but the first was not
        assert '0000000004' in scheduled_urs['0000000004']
        assert '0000000003' not in scheduled_urs['0000000004']

