#!/usr/bin/python
from __future__ import division

from datetime import datetime, timedelta

# Import the modules to test
from adaptive_scheduler.model2      import (SiderealTarget, Proposal, MoleculeFactory,
                                            Request, UserRequest,
                                            Windows, Window, Constraints)

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

        self.telescope = dict(
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

        self.proposal = Proposal(
                                  id  = 'LCOSchedulerTest',
                                  pi           = 'Eric Saunders',
                                  tag            = 'admin',
                                  tac_priority       = 1
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

        resource_1 = '1m0a.doma.ogg'
        self.window_1 = Window({'start': self.base_time,
                                'end': self.base_time + timedelta(hours=0, minutes=30)}, resource_1)
        self.windows_1 = Windows()
        self.windows_1.append(self.window_1)

        resource_2 = '1m0a.doma.ogg'
        self.window_2 = Window({'start': self.base_time + timedelta(hours=0, minutes=30),
                                'end': self.base_time + timedelta(hours=1, minutes=0)}, resource_2)
        self.windows_2 = Windows()
        self.windows_2.append(self.window_2)
        resource_3 = '1m0a.doma.ogg'
        self.window_3 =  Window({'start': self.base_time + timedelta(hours=1, minutes=0),
                                 'end': self.base_time + timedelta(hours=1, minutes=30)}, resource_3)
        self.windows_3 = Windows()
        self.windows_3.append(self.window_3)

        self.request_1 = Request(target         = self.target,
                          molecules      = [self.molecule],
                          windows        = self.windows_1,
                          constraints    = self.constraints,
                          request_number = '0000000001',
                          duration       = 1750)

        self.request_2 = Request(target         = self.target,
                          molecules      = [self.molecule],
                          windows        = self.windows_2,
                          constraints    = self.constraints,
                          request_number = '0000000002',
                          duration       =1750)

        self.request_3 = Request(target         = self.target,
                          molecules      = [self.molecule],
                          windows        = self.windows_2,
                          constraints    = self.constraints,
                          request_number = '0000000003',
                          duration       =1750)

        self.request_4 = Request(target         = self.target,
                          molecules      = [self.molecule],
                          windows        = self.windows_3,
                          constraints    = self.constraints,
                          request_number = '0000000004',
                          duration       =1750)


        self.user_and_request_1 = UserRequest(operator='and', requests=[self.request_1, self.request_2],
                                              proposal=self.proposal, expires=datetime(2050, 1, 1),
                                              tracking_number='0000000001', observation_type='NORMAL',
                                              ipp_value=1.0, group_id='ur 1', submitter='')
        self.user_and_request_2 = UserRequest(operator='and', requests=[self.request_3, self.request_4],
                                              proposal=self.proposal, expires=datetime(2050, 1, 1),
                                              tracking_number='0000000002', observation_type='NORMAL',
                                              ipp_value=1.0, group_id='ur 2', submitter='')
        self.user_many_request_1 = UserRequest(operator='many', requests=[self.request_1, self.request_2],
                                              proposal=self.proposal, expires=datetime(2050, 1, 1),
                                              tracking_number='0000000003', observation_type='NORMAL',
                                              ipp_value=1.5, group_id='ur 3', submitter='')
        self.user_many_request_2 = UserRequest(operator='many', requests=[self.request_3, self.request_4],
                                              proposal=self.proposal, expires=datetime(2050, 1, 1),
                                              tracking_number='0000000004', observation_type='NORMAL',
                                              ipp_value=1.5, group_id='ur 4', submitter='')

    def _schedule_requests(self, too_ur_list, normal_ur_list, scheduler_time):
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
        fake_semester = {'id': '2015A', 'start': scheduler_time - timedelta(days=150),
                         'end': scheduler_time + timedelta(days=150)}

        result = scheduler.run_scheduler(scheduler_input, scheduler_time + timedelta(minutes=15), fake_semester)

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

    def test_large_and_requests(self):
        days_out = 0
        # build up a request a day for 100 days out
        new_time = datetime(2016, 10, 3, 5, 0)
        request_list = []
        while days_out < 80:
            resource = '1m0a.doma.ogg'
            window = Window({'start': new_time + timedelta(days=days_out),
                                    'end': new_time + timedelta(days=days_out, hours=0, minutes=30)}, resource)
            windows = Windows()
            windows.append(window)
            request = Request(target=self.target,
                                molecules=[self.molecule],
                                windows=windows,
                                constraints=self.constraints,
                                request_number="11{}".format(days_out).rjust(10, '0'),
                                duration=1750)
            request_list.append(request)
            days_out += 1

        user_request = UserRequest(operator='and', requests=request_list, proposal=self.proposal,
                                    expires=datetime(2050, 1, 1), tracking_number='0000000100',
                                   ipp_value=1.0, group_id='large ur', submitter='', observation_type='NORMAL')

        normal_request_list = [user_request,]
        result = self._schedule_requests([], normal_request_list, new_time - timedelta(hours=10))
        scheduled_urs = result.get_scheduled_requests_by_tracking_num()

        # assert that none of the and is scheduled (since it has an unschedulable request in it)
        # assert that both of the manys are scheduled
        assert '0000000100' in scheduled_urs
        for req in request_list:
            # assert each child request is in the schedule (scheduler schedules past horizon for ands)
            assert req.request_number in scheduled_urs['0000000100']
