#!/usr/bin/python
from __future__ import division

from datetime import datetime, timedelta

# Import the modules to test
from adaptive_scheduler.models      import (SiderealTarget, Proposal, Configuration,
                                            Request, RequestGroup,
                                            Windows, Window)

from test_scheduler import create_scheduler_input_factory, create_running_request_group
from adaptive_scheduler.kernel.fullscheduler_gurobi import FullScheduler_gurobi
from adaptive_scheduler.scheduler_input import  SchedulerParameters
from adaptive_scheduler.scheduler import LCOGTNetworkScheduler, SchedulerRunner
from adaptive_scheduler.utils import get_reservation_datetimes

from nose.tools import assert_equal, assert_not_equal, assert_true, assert_false
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

        self.instrument_config = dict(
            exposure_count=1,
            bin_x=2,
            bin_y = 2,
            exposure_time=60 * 25,
            optical_elements={'filter': 'b'}
        )

        self.guiding_config = dict(
            state='OPTIONAL',
            mode='',
            optical_elements={},
            exposure_time=10
        )

        self.acquisition_config = dict(
            mode='OFF'
        )

        self.constraints = {'max_airmass': None,
                            'min_lunar_distance': 0}

        self.configuration = Configuration(**dict(
            id=5,
            target=self.target,
            type='expose',
            instrument_type='1M0-SCICAM-SBIG',
            priority=1,
            instrument_configs=[self.instrument_config],
            acquisition_config=self.acquisition_config,
            guiding_config=self.guiding_config,
            constraints=self.constraints
        ))

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
        self.resource_3 = '1m0a.doma.ogg'
        self.window_3 =  Window({'start': self.base_time + timedelta(hours=1, minutes=0),
                                 'end': self.base_time + timedelta(hours=1, minutes=30)}, self.resource_3)
        self.windows_3 = Windows()
        self.windows_3.append(self.window_3)

        self.request_1 = Request(configurations= [self.configuration],
                                 windows        = self.windows_1,
                                 id= 1,
                                 duration       = 1750)

        self.request_2 = Request(configurations= [self.configuration],
                                 windows        = self.windows_2,
                                 id= 2,
                                 duration       =1750)

        self.request_3 = Request(configurations= [self.configuration],
                                 windows        = self.windows_2,
                                 id= 3,
                                 duration       =1750)

        self.request_4 = Request(configurations= [self.configuration],
                                 windows        = self.windows_3,
                                 id= 4,
                                 duration       =1750)

        self.request_5 = Request(configurations= [self.configuration],
                                 windows        = self.windows_3,
                                 id= 5,
                                 duration       =1750)

        self.and_request_group_1 = RequestGroup(operator='and', requests=[self.request_1, self.request_2],
                                                proposal=self.proposal, expires=datetime(2050, 1, 1),
                                                id=1, observation_type='NORMAL',
                                                ipp_value=1.0, name='ur 1', submitter='')
        self.and_request_group_2 = RequestGroup(operator='and', requests=[self.request_3, self.request_4],
                                                proposal=self.proposal, expires=datetime(2050, 1, 1),
                                                id=2, observation_type='NORMAL',
                                                ipp_value=1.0, name='ur 2', submitter='')
        self.many_request_group_1 = RequestGroup(operator='many', requests=[self.request_1, self.request_2],
                                                 proposal=self.proposal, expires=datetime(2050, 1, 1),
                                                 id=3, observation_type='NORMAL',
                                                 ipp_value=1.5, name='ur 3', submitter='')
        self.many_request_group_2 = RequestGroup(operator='many', requests=[self.request_3, self.request_4],
                                                 proposal=self.proposal, expires=datetime(2050, 1, 1),
                                                 id=4, observation_type='NORMAL',
                                                 ipp_value=1.5, name='ur 4', submitter='')
        self.rr_request_group_1 = RequestGroup(operator='many', requests=[self.request_5],
                                               proposal=self.proposal, expires=datetime(2050, 1, 1),
                                               id=5, observation_type='RAPID_RESPONSE',
                                               ipp_value=1.5, name='ur 5', submitter='')
        self.rr_request_group_2 = RequestGroup(operator='many', requests=[self.request_1, self.request_3],
                                               proposal=self.proposal, expires=datetime(2050, 1, 1),
                                               id=6, observation_type='RAPID_RESPONSE',
                                               ipp_value=1.5, name='ur 6', submitter='')

    def _schedule_requests(self, rr_rg_list, normal_rg_list, scheduler_time, rr_loop=False,
                           block_schedule_by_resource={}, running_request_groups=[], rapid_response_ids=[],
                           semester_details={}):
        sched_params = SchedulerParameters(run_once=True, dry_run=True)
        event_bus_mock = Mock()
        scheduler = LCOGTNetworkScheduler(FullScheduler_gurobi, sched_params, event_bus_mock, self.telescopes)
        network_interface_mock = Mock()
        network_interface_mock.cancel = Mock(return_value=0)
        network_interface_mock.save = Mock(return_value=0)
        network_interface_mock.abort = Mock(return_value=0)
        network_interface_mock.get_current_events = Mock(return_value={})

        mock_input_factory = create_scheduler_input_factory(rr_rg_list, normal_rg_list, block_schedule_by_resource,
                                                            running_request_groups, rapid_response_ids)

        if rr_loop:
            scheduler_input = mock_input_factory.create_rr_scheduling_input()
        else:
            scheduler_input = mock_input_factory.create_normal_scheduling_input()
        scheduler_input.scheduler_now = scheduler_time
        scheduler_input.estimated_scheduler_end = scheduler_time + timedelta(minutes=15)
        if not semester_details:
            semester_details = {'id': '2015A', 'start': scheduler_time - timedelta(days=150),
                             'end': scheduler_time + timedelta(days=150)}

        result = scheduler.run_scheduler(scheduler_input, scheduler_time + timedelta(minutes=15), semester_details,
                                         preemption_enabled=rr_loop)

        return result

    def test_changing_semester_details_clears_visibility_cache(self):
        scheduler_time = self.base_time - timedelta(hours=10)
        sched_params = SchedulerParameters(run_once=True, dry_run=True)
        event_bus_mock = Mock()
        scheduler = LCOGTNetworkScheduler(FullScheduler_gurobi, sched_params, event_bus_mock, self.telescopes)
        network_interface_mock = Mock()
        network_interface_mock.cancel = Mock(return_value=0)
        network_interface_mock.save = Mock(return_value=0)
        network_interface_mock.abort = Mock(return_value=0)
        network_interface_mock.get_current_events = Mock(return_value={})
        normal_ur_list = [self.and_request_group_1, self.and_request_group_2]
        mock_input_factory = create_scheduler_input_factory([], normal_ur_list, {}, [], [])
        scheduler_input = mock_input_factory.create_normal_scheduling_input()
        scheduler_input.scheduler_now = scheduler_time
        scheduler_input.estimated_scheduler_end = scheduler_time + timedelta(minutes=15)
        semester_details = {'id': '2015A', 'start': scheduler_time - timedelta(days=150),
                                'end': scheduler_time + timedelta(days=150)}

        result = scheduler.run_scheduler(scheduler_input, scheduler_time + timedelta(minutes=15), semester_details,
                                         preemption_enabled=False)
        assert scheduler.visibility_cache != {}
        saved_visibility_cache = scheduler.visibility_cache
        # Now run again with a different semester to clear visibility cache
        semester_details['start'] = scheduler_time - timedelta(days=149)
        result = scheduler.run_scheduler(scheduler_input, scheduler_time + timedelta(minutes=15), semester_details,
                                         preemption_enabled=False)
        assert scheduler.visibility_cache != {}
        assert scheduler.visibility_cache != saved_visibility_cache

    def test_competing_and_requests(self):
        result = self._schedule_requests([], [self.and_request_group_1, self.and_request_group_2],
                                         self.base_time - timedelta(hours=10))

        scheduled_rgs = result.get_scheduled_requests_by_request_group_id()

        # assert that either user request 1 or user request 2 were scheduled in full, with the other not being scheduled
        if 1 in scheduled_rgs:
            # check that ur 1s requests are scheduled
            assert 1 in scheduled_rgs[1]
            assert 2 in scheduled_rgs[1]
            # and check that ur 2 is not scheduled
            assert 2 not in scheduled_rgs
        else:
            assert 4 in scheduled_rgs[2]
            assert 3 in scheduled_rgs[2]
            assert 1 not in scheduled_rgs

    def test_competing_many_requests(self):
        result = self._schedule_requests([], [self.many_request_group_1, self.many_request_group_2],
                                         self.base_time - timedelta(hours=10))
        scheduled_rgs = result.get_scheduled_requests_by_request_group_id()

        # assert that user request 3 request 1 and user request 4 request 4 were scheduled ,
        # along with one of either 3-2 or 4-3.
        assert 3 in scheduled_rgs
        assert 4 in scheduled_rgs
        assert 1 in scheduled_rgs[3]
        assert 4 in scheduled_rgs[4]
        if 2 in scheduled_rgs[3]:
            assert 3 not in scheduled_rgs[4]
        else:
            assert 2 not in scheduled_rgs[3]

    def test_competing_many_and_requests(self):
        normal_request_list = [self.and_request_group_1, self.many_request_group_2]
        result = self._schedule_requests([], normal_request_list, self.base_time - timedelta(hours=10))
        scheduled_rgs = result.get_scheduled_requests_by_request_group_id()

        # assert the and request was taken in full, and the remaining many ur 4 request 2 was scheduled
        assert 1 in scheduled_rgs
        assert 1 in scheduled_rgs[1]
        assert 2 in scheduled_rgs[1]
        assert 4 in scheduled_rgs
        # the second request from the many was scheduled but the first was not
        assert 4 in scheduled_rgs[4]
        assert 3 not in scheduled_rgs[4]

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
            request = Request(configurations=[self.configuration],
                              windows=windows,
                              id=int("11{}".format(days_out).rjust(10, '0')),
                              duration=1750)
            request_list.append(request)
            days_out += 1

        request_group = RequestGroup(operator='and', requests=request_list, proposal=self.proposal,
                                     expires=datetime(2050, 1, 1), id=100,
                                     ipp_value=1.0, name='large ur', submitter='', observation_type='NORMAL')

        normal_request_list = [request_group,]
        result = self._schedule_requests([], normal_request_list, new_time - timedelta(hours=10))
        scheduled_rgs = result.get_scheduled_requests_by_request_group_id()

        # assert that none of the and is scheduled (since it has an unschedulable request in it)
        # assert that both of the manys are scheduled
        assert 100 in scheduled_rgs
        for req in request_list:
            # assert each child request is in the schedule (scheduler schedules past horizon for ands)
            assert req.id in scheduled_rgs[100]

    def test_normal_requests_dont_schedule_over_rr(self):
        ''' Verifies that a normal request will not schedule over a just scheduled RR request
        '''
        rr_schedule = {self.resource_3: [(self.base_time + timedelta(hours=1, minutes=0),
                                          self.base_time + timedelta(hours=1, minutes=25)),]}
        result = self._schedule_requests([self.rr_request_group_1, ], [self.many_request_group_2, ],
                                         self.base_time - timedelta(hours=10), rr_loop=False,
                                         block_schedule_by_resource=rr_schedule)
        scheduled_rgs = result.get_scheduled_requests_by_request_group_id()
        # Ensure request 3 could be scheduled, but request 4 could not because it overlapped with the scheduled RR
        assert_true(4 in scheduled_rgs)
        assert_true(4 not in scheduled_rgs[4])
        assert_true(3 in scheduled_rgs[4])

    def test_rr_requests_dont_schedule_over_running_rr(self):
        ''' Verifies that a RR will not preempt a currently running RR if it overlaps with its window completely
        '''
        rapid_response_id = 99
        running_request_group = create_running_request_group(request_group_id=rapid_response_id,
                                                            request_id=99,
                                                            resource=self.resource_3,
                                                            start=self.base_time,
                                                            end=self.base_time + timedelta(hours=2))
        result = self._schedule_requests([self.rr_request_group_1, ], [self.many_request_group_2, ],
                                         self.base_time - timedelta(hours=10), rr_loop=True,
                                         block_schedule_by_resource={},
                                         running_request_groups=[running_request_group, ],
                                         rapid_response_ids=[rapid_response_id, ])
        scheduled_rgs = result.get_scheduled_requests_by_request_group_id()
        # Ensure no RR was scheduled because the running request group was over it's time
        assert_false(5 in scheduled_rgs)
        assert_equal(scheduled_rgs, {})

    def test_rr_requests_do_schedule_over_running_normal(self):
        ''' Verifies that a RR will preempt a currently running normal request and be scheduled over it at its
            earliest time possible
        '''
        rapid_response_id = 777
        running_request_group = create_running_request_group(request_group_id=99,
                                                            request_id=99,
                                                            resource=self.resource_3,
                                                            start=self.base_time,
                                                            end=self.base_time + timedelta(hours=2))
        scheduler_start = self.base_time - timedelta(hours=10)
        result = self._schedule_requests([self.rr_request_group_1, ], [self.many_request_group_2, ],
                                         scheduler_start, rr_loop=True,
                                         block_schedule_by_resource={},
                                         running_request_groups=[running_request_group, ],
                                         rapid_response_ids=[rapid_response_id, ])
        scheduled_rgs = result.get_scheduled_requests_by_request_group_id()
        # Ensure RR was scheduled at its first time even though it overlaps with the currently running normal request
        assert_true(5 in scheduled_rgs)
        assert_true(5 in scheduled_rgs[5])
        semester_start = scheduler_start - timedelta(days=150)
        dt_start, dt_end = get_reservation_datetimes(scheduled_rgs[5][5], semester_start)
        assert_equal(dt_start, self.window_3.start)
        assert_equal(dt_end, self.window_3.start + timedelta(seconds=1750))

    def test_rr_requests_schedule_after_running_rr(self):
        ''' Verifies that a RR will be scheduled after a currently running RR if it is able
        '''
        rapid_response_id = 99
        running_request_group = create_running_request_group(request_group_id=rapid_response_id,
                                                             request_id=99,
                                                             resource=self.resource_3,
                                                             start=self.base_time,
                                                             end=self.base_time + timedelta(hours=1, minutes=0, seconds=30))
        scheduler_start = self.base_time - timedelta(hours=10)
        result = self._schedule_requests([self.rr_request_group_1, ], [self.many_request_group_2, ],
                                         scheduler_start, rr_loop=True,
                                         block_schedule_by_resource={},
                                         running_request_groups=[running_request_group, ],
                                         rapid_response_ids=[rapid_response_id, ])
        scheduled_rgs = result.get_scheduled_requests_by_request_group_id()
        # Ensure RR was scheduled after the running RR since there was still time
        assert_true(5 in scheduled_rgs)
        assert_true(5 in scheduled_rgs[5])
        semester_start = scheduler_start - timedelta(days=150)
        dt_start, dt_end = get_reservation_datetimes(scheduled_rgs[5][5], semester_start)
        assert_equal(dt_start, self.base_time + timedelta(hours=1, minutes=0, seconds=30))
        assert_equal(dt_end, self.base_time + timedelta(hours=1, minutes=0, seconds=30) + timedelta(seconds=1750))

    def test_normal_requests_dont_schedule_over_running_rr(self):
        ''' Verifies that a normal request will be blocked by a currently running RR
        '''
        rr_request_group_id = 99
        running_request_group = create_running_request_group(request_group_id=rr_request_group_id,
                                                            request_id=99,
                                                            resource=self.resource_3,
                                                            start=self.base_time,
                                                            end=self.base_time + timedelta(hours=2))
        result = self._schedule_requests([self.rr_request_group_1, ], [self.many_request_group_2, ],
                                         self.base_time - timedelta(hours=10), rr_loop=False,
                                         block_schedule_by_resource={},
                                         running_request_groups=[running_request_group, ],
                                         rapid_response_ids=[rr_request_group_id, ])
        scheduled_rgs = result.get_scheduled_requests_by_request_group_id()
        # Ensure request 3 could be scheduled, but request 4 could not because it overlapped with the scheduled RR
        assert_false(4 in scheduled_rgs)
        assert_false(3 in scheduled_rgs)
        assert_equal(scheduled_rgs, {})

    def test_normal_requests_can_schedule_after_rr(self):
        ''' Verifies that a normal request will respect a previously scheduled RR whose time overlaps with it's window.
            Ensures that the normal request starts after the end of the RR.
        '''
        rr_schedule = {self.resource_3: [(self.base_time + timedelta(hours=1, minutes=0),
                                          self.base_time + timedelta(hours=1, minutes=0, seconds=30)),]}
        scheduler_start = self.base_time - timedelta(hours=10)
        result = self._schedule_requests([self.rr_request_group_1, ], [self.many_request_group_2, ],
                                         scheduler_start, rr_loop=False,
                                         block_schedule_by_resource=rr_schedule)
        scheduled_rgs = result.get_scheduled_requests_by_request_group_id()
        # Ensure both requests can get scheduled, but request 4 is after the RR reservation in its window
        assert_true(4 in scheduled_rgs)
        assert_true(4 in scheduled_rgs[4])
        assert_true(3 in scheduled_rgs[4])
        semester_start = scheduler_start - timedelta(days=150)
        dt_start, dt_end = get_reservation_datetimes(scheduled_rgs[4][4], semester_start)
        assert_equal(dt_start, self.base_time + timedelta(hours=1, minutes=0, seconds=30))
        assert_equal(dt_end, self.base_time + timedelta(hours=1, minutes=0, seconds=30) + timedelta(seconds=1750))

    def test_normal_requests_can_schedule_after_running_rr(self):
        ''' Verifies that a normal request will respect a already running RR whose time overlaps with it's window.
            Ensures that the normal request starts after the end of the RR.
        '''
        rr_request_group_id = 99
        running_request_group = create_running_request_group(request_group_id=rr_request_group_id,
                                                            request_id=99,
                                                            resource=self.resource_3,
                                                            start=self.base_time,
                                                            end=self.base_time + timedelta(hours=1, minutes=0, seconds=30))
        scheduler_start = self.base_time - timedelta(hours=10)
        result = self._schedule_requests([self.rr_request_group_1, ], [self.many_request_group_2, ],
                                         scheduler_start, rr_loop=False,
                                         block_schedule_by_resource={},
                                         running_request_groups=[running_request_group, ],
                                         rapid_response_ids=[rr_request_group_id, ])
        scheduled_rgs = result.get_scheduled_requests_by_request_group_id()
        # Ensure request 4 is after the RR running request in its window, and request 3 is blocked by the running RR
        assert_true(4 in scheduled_rgs)
        assert_true(4 in scheduled_rgs[4])
        assert_false(3 in scheduled_rgs[4])
        semester_start = scheduler_start - timedelta(days=150)
        dt_start, dt_end = get_reservation_datetimes(scheduled_rgs[4][4], semester_start)
        assert_equal(dt_start, self.base_time + timedelta(hours=1, minutes=0, seconds=30))
        assert_equal(dt_end, self.base_time + timedelta(hours=1, minutes=0, seconds=30) + timedelta(seconds=1750))

    def test_one_rr_has_correct_cancel_date_list(self):
        ''' Schedules a single RR and verifies it's time appears in the cancellation date list on the resource
        '''
        scheduler_start = self.base_time - timedelta(hours=10)
        result = self._schedule_requests([self.rr_request_group_1, ], [self.many_request_group_2, ],
                                         scheduler_start, rr_loop=True,
                                         block_schedule_by_resource={})

        scheduled_rgs = result.get_scheduled_requests_by_request_group_id()
        assert_true(5 in scheduled_rgs)
        assert_true(5 in scheduled_rgs[5])

        semester_start = scheduler_start - timedelta(days=150)
        dt_start, dt_end = get_reservation_datetimes(scheduled_rgs[5][5], semester_start)
        scheduler_runner = SchedulerRunner(SchedulerParameters(dry_run=True), Mock(), Mock(), Mock(), Mock())
        scheduler_runner.semester_details = {'id': '2015A', 'start': semester_start,
                                             'end': scheduler_start + timedelta(days=150)}
        cancel_date_list_by_resource = scheduler_runner._determine_schedule_cancelation_list_from_new_schedule(result.schedule)

        assert_true('1m0a.doma.ogg' in cancel_date_list_by_resource)
        assert_equal(len(cancel_date_list_by_resource['1m0a.doma.ogg']), 1)
        assert_equal(cancel_date_list_by_resource['1m0a.doma.ogg'][0][0], dt_start)
        assert_equal(cancel_date_list_by_resource['1m0a.doma.ogg'][0][1], dt_end)

    def test_multiple_rr_has_correct_cancel_date_list(self):
        ''' Schedules three nearly back to back RRs. Checks that each of their scheduled time appears in the date list
            for the resource they are scheduled in when getting dates to cancel.
        '''
        scheduler_start = self.base_time - timedelta(hours=10)
        result = self._schedule_requests([self.rr_request_group_2, self.rr_request_group_1], [self.many_request_group_2, ],
                                         scheduler_start, rr_loop=True,
                                         block_schedule_by_resource={})

        scheduled_rgs = result.get_scheduled_requests_by_request_group_id()
        assert_true(5 in scheduled_rgs)
        assert_true(5 in scheduled_rgs[5])
        assert_true(6 in scheduled_rgs)
        assert_true(3 in scheduled_rgs[6])
        assert_true(1 in scheduled_rgs[6])

        semester_start = scheduler_start - timedelta(days=150)
        scheduler_runner = SchedulerRunner(SchedulerParameters(dry_run=True), Mock(), Mock(), Mock(), Mock())
        scheduler_runner.semester_details = {'id': '2015A', 'start': semester_start,
                                             'end': scheduler_start + timedelta(days=150)}
        cancel_date_list_by_resource = scheduler_runner._determine_schedule_cancelation_list_from_new_schedule(result.schedule)
        assert_true('1m0a.doma.ogg' in cancel_date_list_by_resource)
        assert_equal(len(cancel_date_list_by_resource['1m0a.doma.ogg']), 3)
        assert_equal(len(cancel_date_list_by_resource['1m0a.doma.ogg']), 3)

        dt_start, dt_end = get_reservation_datetimes(scheduled_rgs[6][1], semester_start)
        assert_equal(cancel_date_list_by_resource['1m0a.doma.ogg'][0][0], dt_start)
        assert_equal(cancel_date_list_by_resource['1m0a.doma.ogg'][0][1], dt_end)

        dt_start, dt_end = get_reservation_datetimes(scheduled_rgs[6][3], semester_start)
        assert_equal(cancel_date_list_by_resource['1m0a.doma.ogg'][1][0], dt_start)
        assert_equal(cancel_date_list_by_resource['1m0a.doma.ogg'][1][1], dt_end)

        dt_start, dt_end = get_reservation_datetimes(scheduled_rgs[5][5], semester_start)
        assert_equal(cancel_date_list_by_resource['1m0a.doma.ogg'][2][0], dt_start)
        assert_equal(cancel_date_list_by_resource['1m0a.doma.ogg'][2][1], dt_end)
