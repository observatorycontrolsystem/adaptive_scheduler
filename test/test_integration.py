#!/usr/bin/python
from __future__ import division

from datetime import datetime, timedelta

# Import the modules to test
from adaptive_scheduler.model2      import (SiderealTarget, Proposal, MoleculeFactory,
                                            Request, UserRequest,
                                            Windows, Window, Constraints)

from test_scheduler import create_scheduler_input_factory, create_running_user_request
from adaptive_scheduler.kernel.fullscheduler_gurobi import FullScheduler_gurobi
from adaptive_scheduler.scheduler_input import  SchedulerParameters
from adaptive_scheduler.scheduler import LCOGTNetworkScheduler, SchedulerRunner
from adaptive_scheduler.utils import get_reservation_datetimes
from adaptive_scheduler.kernel_mappings import region

from nose.tools import assert_equal, assert_not_equal, assert_true, assert_false
from mock import Mock

class TestIntegration(object):
    '''Unit tests for the adaptive scheduler Request object.'''

    def setup(self):
        region.delete("current_semester")
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
        self.resource_3 = '1m0a.doma.ogg'
        self.window_3 =  Window({'start': self.base_time + timedelta(hours=1, minutes=0),
                                 'end': self.base_time + timedelta(hours=1, minutes=30)}, self.resource_3)
        self.windows_3 = Windows()
        self.windows_3.append(self.window_3)

        self.request_1 = Request(target         = self.target,
                          molecules      = [self.molecule],
                          windows        = self.windows_1,
                          constraints    = self.constraints,
                          request_number = 1,
                          duration       = 1750)

        self.request_2 = Request(target         = self.target,
                          molecules      = [self.molecule],
                          windows        = self.windows_2,
                          constraints    = self.constraints,
                          request_number = 2,
                          duration       =1750)

        self.request_3 = Request(target         = self.target,
                          molecules      = [self.molecule],
                          windows        = self.windows_2,
                          constraints    = self.constraints,
                          request_number = 3,
                          duration       =1750)

        self.request_4 = Request(target         = self.target,
                          molecules      = [self.molecule],
                          windows        = self.windows_3,
                          constraints    = self.constraints,
                          request_number = 4,
                          duration       =1750)

        self.request_5 = Request(target         = self.target,
                          molecules      = [self.molecule],
                          windows        = self.windows_3,
                          constraints    = self.constraints,
                          request_number = 5,
                          duration       =1750)

        self.user_and_request_1 = UserRequest(operator='and', requests=[self.request_1, self.request_2],
                                              proposal=self.proposal, expires=datetime(2050, 1, 1),
                                              tracking_number=1, observation_type='NORMAL',
                                              ipp_value=1.0, group_id='ur 1', submitter='')
        self.user_and_request_2 = UserRequest(operator='and', requests=[self.request_3, self.request_4],
                                              proposal=self.proposal, expires=datetime(2050, 1, 1),
                                              tracking_number=2, observation_type='NORMAL',
                                              ipp_value=1.0, group_id='ur 2', submitter='')
        self.user_many_request_1 = UserRequest(operator='many', requests=[self.request_1, self.request_2],
                                              proposal=self.proposal, expires=datetime(2050, 1, 1),
                                              tracking_number=3, observation_type='NORMAL',
                                              ipp_value=1.5, group_id='ur 3', submitter='')
        self.user_many_request_2 = UserRequest(operator='many', requests=[self.request_3, self.request_4],
                                              proposal=self.proposal, expires=datetime(2050, 1, 1),
                                              tracking_number=4, observation_type='NORMAL',
                                              ipp_value=1.5, group_id='ur 4', submitter='')
        self.too_user_request_1 = UserRequest(operator='many', requests=[self.request_5],
                                              proposal=self.proposal, expires=datetime(2050, 1, 1),
                                              tracking_number=5, observation_type='TARGET_OF_OPPORTUNITY',
                                              ipp_value=1.5, group_id='ur 5', submitter='')
        self.too_user_request_2 = UserRequest(operator='many', requests=[self.request_1, self.request_3],
                                              proposal=self.proposal, expires=datetime(2050, 1, 1),
                                              tracking_number=6, observation_type='TARGET_OF_OPPORTUNITY',
                                              ipp_value=1.5, group_id='ur 6', submitter='')

    def _schedule_requests(self, too_ur_list, normal_ur_list, scheduler_time, too_loop=False,
                           block_schedule_by_resource={}, running_user_requests=[], too_tracking_numbers=[]):
        sched_params = SchedulerParameters(run_once=True, dry_run=True)
        event_bus_mock = Mock()
        scheduler = LCOGTNetworkScheduler(FullScheduler_gurobi, sched_params, event_bus_mock, self.telescopes)
        network_interface_mock = Mock()
        network_interface_mock.cancel = Mock(return_value=0)
        network_interface_mock.save = Mock(return_value=0)
        network_interface_mock.abort = Mock(return_value=0)
        network_interface_mock.get_current_events = Mock(return_value={})

        mock_input_factory = create_scheduler_input_factory(too_ur_list, normal_ur_list, block_schedule_by_resource,
                                                            running_user_requests, too_tracking_numbers)

        if too_loop:
            scheduler_input = mock_input_factory.create_too_scheduling_input()
        else:
            scheduler_input = mock_input_factory.create_normal_scheduling_input()
        scheduler_input.scheduler_now = scheduler_time
        scheduler_input.estimated_scheduler_end = scheduler_time + timedelta(minutes=15)
        fake_semester = {'id': '2015A', 'start': scheduler_time - timedelta(days=150),
                         'end': scheduler_time + timedelta(days=150)}

        result = scheduler.run_scheduler(scheduler_input, scheduler_time + timedelta(minutes=15), fake_semester,
                                         preemption_enabled=too_loop)

        return result


    def test_competing_and_requests(self):
        result = self._schedule_requests([], [self.user_and_request_1, self.user_and_request_2],
                                         self.base_time - timedelta(hours=10))

        scheduled_urs = result.get_scheduled_requests_by_tracking_num()

        # assert that either user request 1 or user request 2 were scheduled in full, with the other not being scheduled
        if 1 in scheduled_urs:
            # check that ur 1s requests are scheduled
            assert 1 in scheduled_urs[1]
            assert 2 in scheduled_urs[1]
            # and check that ur 2 is not scheduled
            assert 2 not in scheduled_urs
        else:
            assert 4 in scheduled_urs[2]
            assert 3 in scheduled_urs[2]
            assert 1 not in scheduled_urs

    def test_competing_many_requests(self):
        result = self._schedule_requests([], [self.user_many_request_1, self.user_many_request_2],
                                         self.base_time - timedelta(hours=10))
        scheduled_urs = result.get_scheduled_requests_by_tracking_num()

        # assert that user request 3 request 1 and user request 4 request 4 were scheduled ,
        # along with one of either 3-2 or 4-3.
        assert 3 in scheduled_urs
        assert 4 in scheduled_urs
        assert 1 in scheduled_urs[3]
        assert 4 in scheduled_urs[4]
        if 2 in scheduled_urs[3]:
            assert 3 not in scheduled_urs[4]
        else:
            assert 2 not in scheduled_urs[3]

    def test_competing_many_and_requests(self):
        normal_request_list = [self.user_and_request_1, self.user_many_request_2]
        result = self._schedule_requests([], normal_request_list, self.base_time - timedelta(hours=10))
        scheduled_urs = result.get_scheduled_requests_by_tracking_num()

        # assert the and request was taken in full, and the remaining many ur 4 request 2 was scheduled
        assert 1 in scheduled_urs
        assert 1 in scheduled_urs[1]
        assert 2 in scheduled_urs[1]
        assert 4 in scheduled_urs
        # the second request from the many was scheduled but the first was not
        assert 4 in scheduled_urs[4]
        assert 3 not in scheduled_urs[4]

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
                                request_number=int("11{}".format(days_out).rjust(10, '0')),
                                duration=1750)
            request_list.append(request)
            days_out += 1

        user_request = UserRequest(operator='and', requests=request_list, proposal=self.proposal,
                                    expires=datetime(2050, 1, 1), tracking_number=100,
                                   ipp_value=1.0, group_id='large ur', submitter='', observation_type='NORMAL')

        normal_request_list = [user_request,]
        result = self._schedule_requests([], normal_request_list, new_time - timedelta(hours=10))
        scheduled_urs = result.get_scheduled_requests_by_tracking_num()

        # assert that none of the and is scheduled (since it has an unschedulable request in it)
        # assert that both of the manys are scheduled
        assert 100 in scheduled_urs
        for req in request_list:
            # assert each child request is in the schedule (scheduler schedules past horizon for ands)
            assert req.request_number in scheduled_urs[100]


    def test_normal_requests_dont_schedule_over_too(self):
        ''' Verifies that a normal request will not schedule over a just scheduled too request
        '''
        too_schedule = {self.resource_3: [(self.base_time + timedelta(hours=1, minutes=0),
                                          self.base_time + timedelta(hours=1, minutes=25)),]}
        result = self._schedule_requests([self.too_user_request_1,], [self.user_many_request_2,],
                                         self.base_time - timedelta(hours=10), too_loop=False,
                                         block_schedule_by_resource=too_schedule)
        scheduled_urs = result.get_scheduled_requests_by_tracking_num()
        # Ensure request 3 could be scheduled, but request 4 could not because it overlapped with the scheduled too
        assert_true(4 in scheduled_urs)
        assert_true(4 not in scheduled_urs[4])
        assert_true(3 in scheduled_urs[4])


    def test_too_requests_dont_schedule_over_running_too(self):
        ''' Verifies that a too will not preempt a currently running too if it overlaps with its window completely 
        '''
        too_tracking_number = 99
        running_user_request = create_running_user_request(tracking_number=too_tracking_number,
                                                            request_number=99,
                                                            resource=self.resource_3,
                                                            start=self.base_time,
                                                            end=self.base_time + timedelta(hours=2))
        result = self._schedule_requests([self.too_user_request_1,], [self.user_many_request_2,],
                                         self.base_time - timedelta(hours=10), too_loop=True,
                                         block_schedule_by_resource={},
                                         running_user_requests=[running_user_request,],
                                         too_tracking_numbers=[too_tracking_number,])
        scheduled_urs = result.get_scheduled_requests_by_tracking_num()
        # Ensure no too was scheduled because the running user request was over it's time
        assert_false(5 in scheduled_urs)
        assert_equal(scheduled_urs, {})


    def test_too_requests_do_schedule_over_running_normal(self):
        ''' Verifies that a too will preempt a currently running normal request and be scheduled over it at its
            earliest time possible
        '''
        too_tracking_number = 777
        running_user_request = create_running_user_request(tracking_number=99,
                                                            request_number=99,
                                                            resource=self.resource_3,
                                                            start=self.base_time,
                                                            end=self.base_time + timedelta(hours=2))
        scheduler_start = self.base_time - timedelta(hours=10)
        result = self._schedule_requests([self.too_user_request_1,], [self.user_many_request_2,],
                                         scheduler_start, too_loop=True,
                                         block_schedule_by_resource={},
                                         running_user_requests=[running_user_request,],
                                         too_tracking_numbers=[too_tracking_number,])
        scheduled_urs = result.get_scheduled_requests_by_tracking_num()
        # Ensure too was scheduled at its first time even though it overlaps with the currently running normal request
        assert_true(5 in scheduled_urs)
        assert_true(5 in scheduled_urs[5])
        semester_start = scheduler_start - timedelta(days=150)
        dt_start, dt_end = get_reservation_datetimes(scheduled_urs[5][5], semester_start)
        assert_equal(dt_start, self.window_3.start)
        assert_equal(dt_end, self.window_3.start + timedelta(seconds=1750))


    def test_too_requests_schedule_after_running_too(self):
        ''' Verifies that a too will be scheduled after a currently running too if it is able
        '''
        too_tracking_number = 99
        running_user_request = create_running_user_request(tracking_number=too_tracking_number,
                                                            request_number=99,
                                                            resource=self.resource_3,
                                                            start=self.base_time,
                                                            end=self.base_time + timedelta(hours=1, minutes=0, seconds=30))
        scheduler_start = self.base_time - timedelta(hours=10)
        result = self._schedule_requests([self.too_user_request_1,], [self.user_many_request_2,],
                                         scheduler_start, too_loop=True,
                                         block_schedule_by_resource={},
                                         running_user_requests=[running_user_request,],
                                         too_tracking_numbers=[too_tracking_number,])
        scheduled_urs = result.get_scheduled_requests_by_tracking_num()
        # Ensure ToO was scheduled after the running ToO since there was still time
        assert_true(5 in scheduled_urs)
        assert_true(5 in scheduled_urs[5])
        semester_start = scheduler_start - timedelta(days=150)
        dt_start, dt_end = get_reservation_datetimes(scheduled_urs[5][5], semester_start)
        assert_equal(dt_start, self.base_time + timedelta(hours=1, minutes=0, seconds=30))
        assert_equal(dt_end, self.base_time + timedelta(hours=1, minutes=0, seconds=30) + timedelta(seconds=1750))


    def test_normal_requests_dont_schedule_over_running_too(self):
        ''' Verifies that a normal request will be blocked by a currently running too
        '''
        too_tracking_number = 99
        running_user_request = create_running_user_request(tracking_number=too_tracking_number,
                                                            request_number=99,
                                                            resource=self.resource_3,
                                                            start=self.base_time,
                                                            end=self.base_time + timedelta(hours=2))
        result = self._schedule_requests([self.too_user_request_1,], [self.user_many_request_2,],
                                         self.base_time - timedelta(hours=10), too_loop=False,
                                         block_schedule_by_resource={},
                                         running_user_requests=[running_user_request,],
                                         too_tracking_numbers=[too_tracking_number,])
        scheduled_urs = result.get_scheduled_requests_by_tracking_num()
        # Ensure request 3 could be scheduled, but request 4 could not because it overlapped with the scheduled too
        assert_false(4 in scheduled_urs)
        assert_false(3 in scheduled_urs)
        assert_equal(scheduled_urs, {})


    def test_normal_requests_can_schedule_after_too(self):
        ''' Verifies that a normal request will respect a previously scheduled ToO whose time overlaps with it's window.
            Ensures that the normal request starts after the end of the ToO.
        '''
        too_schedule = {self.resource_3: [(self.base_time + timedelta(hours=1, minutes=0),
                                          self.base_time + timedelta(hours=1, minutes=0, seconds=30)),]}
        scheduler_start = self.base_time - timedelta(hours=10)
        result = self._schedule_requests([self.too_user_request_1,], [self.user_many_request_2,],
                                         scheduler_start, too_loop=False,
                                         block_schedule_by_resource=too_schedule)
        scheduled_urs = result.get_scheduled_requests_by_tracking_num()
        # Ensure both requests can get scheduled, but request 4 is after the too reservation in its window
        assert_true(4 in scheduled_urs)
        assert_true(4 in scheduled_urs[4])
        assert_true(3 in scheduled_urs[4])
        semester_start = scheduler_start - timedelta(days=150)
        dt_start, dt_end = get_reservation_datetimes(scheduled_urs[4][4], semester_start)
        assert_equal(dt_start, self.base_time + timedelta(hours=1, minutes=0, seconds=30))
        assert_equal(dt_end, self.base_time + timedelta(hours=1, minutes=0, seconds=30) + timedelta(seconds=1750))


    def test_normal_requests_can_schedule_after_running_too(self):
        ''' Verifies that a normal request will respect a already running ToO whose time overlaps with it's window.
            Ensures that the normal request starts after the end of the ToO.
        '''
        too_tracking_number = 99
        running_user_request = create_running_user_request(tracking_number=too_tracking_number,
                                                           request_number=99,
                                                           resource=self.resource_3,
                                                           start=self.base_time,
                                                           end=self.base_time + timedelta(hours=1, minutes=0, seconds=30))
        scheduler_start = self.base_time - timedelta(hours=10)
        result = self._schedule_requests([self.too_user_request_1,], [self.user_many_request_2,],
                                         scheduler_start, too_loop=False,
                                         block_schedule_by_resource={},
                                         running_user_requests=[running_user_request,],
                                         too_tracking_numbers=[too_tracking_number,])
        scheduled_urs = result.get_scheduled_requests_by_tracking_num()
        # Ensure request 4 is after the too running request in its window, and request 3 is blocked by the running too
        assert_true(4 in scheduled_urs)
        assert_true(4 in scheduled_urs[4])
        assert_false(3 in scheduled_urs[4])
        semester_start = scheduler_start - timedelta(days=150)
        dt_start, dt_end = get_reservation_datetimes(scheduled_urs[4][4], semester_start)
        assert_equal(dt_start, self.base_time + timedelta(hours=1, minutes=0, seconds=30))
        assert_equal(dt_end, self.base_time + timedelta(hours=1, minutes=0, seconds=30) + timedelta(seconds=1750))



    def test_one_too_has_correct_cancel_date_list(self):
        ''' Schedules a single ToO and verifies it's time appears in the cancellation date list on the resource
        '''
        scheduler_start = self.base_time - timedelta(hours=10)
        result = self._schedule_requests([self.too_user_request_1,], [self.user_many_request_2,],
                                         scheduler_start, too_loop=True,
                                         block_schedule_by_resource={})

        scheduled_urs = result.get_scheduled_requests_by_tracking_num()
        assert_true(5 in scheduled_urs)
        assert_true(5 in scheduled_urs[5])

        semester_start = scheduler_start - timedelta(days=150)
        dt_start, dt_end = get_reservation_datetimes(scheduled_urs[5][5], semester_start)
        scheduler_runner = SchedulerRunner(SchedulerParameters(dry_run=True), Mock(), Mock(), Mock(), Mock())
        scheduler_runner.semester_details = {'id': '2015A', 'start': semester_start,
                                             'end': scheduler_start + timedelta(days=150)}
        cancel_date_list_by_resource = scheduler_runner._determine_schedule_cancelation_list_from_new_schedule(result.schedule)

        assert_true('1m0a.doma.ogg' in cancel_date_list_by_resource)
        assert_equal(len(cancel_date_list_by_resource['1m0a.doma.ogg']), 1)
        assert_equal(cancel_date_list_by_resource['1m0a.doma.ogg'][0][0], dt_start)
        assert_equal(cancel_date_list_by_resource['1m0a.doma.ogg'][0][1], dt_end)


    def test_multiple_too_has_correct_cancel_date_list(self):
        ''' Schedules three nearly back to back ToOs. Checks that each of their scheduled time appears in the date list
            for the resource they are scheduled in when getting dates to cancel.
        '''
        scheduler_start = self.base_time - timedelta(hours=10)
        result = self._schedule_requests([self.too_user_request_2, self.too_user_request_1], [self.user_many_request_2,],
                                         scheduler_start, too_loop=True,
                                         block_schedule_by_resource={})

        scheduled_urs = result.get_scheduled_requests_by_tracking_num()
        assert_true(5 in scheduled_urs)
        assert_true(5 in scheduled_urs[5])
        assert_true(6 in scheduled_urs)
        assert_true(3 in scheduled_urs[6])
        assert_true(1 in scheduled_urs[6])

        semester_start = scheduler_start - timedelta(days=150)
        scheduler_runner = SchedulerRunner(SchedulerParameters(dry_run=True), Mock(), Mock(), Mock(), Mock())
        scheduler_runner.semester_details = {'id': '2015A', 'start': semester_start,
                                             'end': scheduler_start + timedelta(days=150)}

        cancel_date_list_by_resource = scheduler_runner._determine_schedule_cancelation_list_from_new_schedule(result.schedule)
        assert_true('1m0a.doma.ogg' in cancel_date_list_by_resource)
        assert_equal(len(cancel_date_list_by_resource['1m0a.doma.ogg']), 3)

        dt_start, dt_end = get_reservation_datetimes(scheduled_urs[6][1], semester_start)
        assert_equal(cancel_date_list_by_resource['1m0a.doma.ogg'][0][0], dt_start)
        assert_equal(cancel_date_list_by_resource['1m0a.doma.ogg'][0][1], dt_end)

        dt_start, dt_end = get_reservation_datetimes(scheduled_urs[6][3], semester_start)
        assert_equal(cancel_date_list_by_resource['1m0a.doma.ogg'][1][0], dt_start)
        assert_equal(cancel_date_list_by_resource['1m0a.doma.ogg'][1][1], dt_end)

        dt_start, dt_end = get_reservation_datetimes(scheduled_urs[5][5], semester_start)
        assert_equal(cancel_date_list_by_resource['1m0a.doma.ogg'][2][0], dt_start)
        assert_equal(cancel_date_list_by_resource['1m0a.doma.ogg'][2][1], dt_end)
