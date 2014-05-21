from adaptive_scheduler.scheduler import Scheduler
from adaptive_scheduler.model2 import UserRequest

from nose.tools import assert_equal


class TestSchduler(object):
    
    def build_ur_list(self, *tracking_numbers):
        ur_list = []
        for tracking_number in tracking_numbers:
            ur = UserRequest(
                               operator='single',
                               requests=None,
                               proposal=None,
                               tracking_number='tracking_number',
                               group_id=None,
                               expires=None,
                             )

            ur_list.append(ur)
        
        return ur_list
    
    def test_blacklist_running_user_requests_returns_empty_list_when_only_request_running(self):
        
        scheduler = Scheduler(None, None)
        
        ur_tracking_numbers = ['0000000001']
        running_ur_tracking_numbers = ['0000000001']
        ur_list = self.build_ur_list(*ur_tracking_numbers)
        schedulable_urs = scheduler.blacklist_running_user_requests(ur_list, running_ur_tracking_numbers)
        assert_equal([], schedulable_urs)
        
        ur_tracking_numbers = ['0000000001']
        running_ur_tracking_numbers = ['0000000001', '0000000002']
        ur_list = self.build_ur_list(*ur_tracking_numbers)
        schedulable_urs = scheduler.blacklist_running_user_requests(ur_list, running_ur_tracking_numbers)
        assert_equal([], schedulable_urs)
        
    def test_blacklist_running_user_requests_returns_empty_list_with_empty_ur_list(self):
        
        scheduler = Scheduler(None, None)
        ur_tracking_numbers = []
        running_ur_tracking_numbers = []
        ur_list = self.build_ur_list(*ur_tracking_numbers)
        schedulable_urs = scheduler.blacklist_running_user_requests(ur_list, running_ur_tracking_numbers)
        assert_equal([], schedulable_urs)
        
        ur_tracking_numbers = []
        running_ur_tracking_numbers = ['0000000001']
        ur_list = self.build_ur_list(*ur_tracking_numbers)
        schedulable_urs = scheduler.blacklist_running_user_requests(ur_list, running_ur_tracking_numbers)
        assert_equal([], schedulable_urs)
        
    def test_blacklist_running_user_requests_returns_all_requests_when_none_running(self):
        
        scheduler = Scheduler(None, None)
        ur_tracking_numbers = []
        running_ur_tracking_numbers = []
        ur_list = self.build_ur_list(*ur_tracking_numbers)
        schedulable_urs = scheduler.blacklist_running_user_requests(ur_list, running_ur_tracking_numbers)
        assert_equal(len(ur_list), len(schedulable_urs))
        
        ur_tracking_numbers = ['0000000001']
        running_ur_tracking_numbers = []
        ur_list = self.build_ur_list(*ur_tracking_numbers)
        schedulable_urs = scheduler.blacklist_running_user_requests(ur_list, running_ur_tracking_numbers)
        assert_equal(len(ur_list), len(schedulable_urs))
        
        ur_tracking_numbers = ['0000000001']
        running_ur_tracking_numbers = ['0000000002']
        ur_list = self.build_ur_list(*ur_tracking_numbers)
        schedulable_urs = scheduler.blacklist_running_user_requests(ur_list, running_ur_tracking_numbers)
        assert_equal(len(ur_list), len(schedulable_urs))
        
        ur_tracking_numbers = ['0000000001', '0000000002']
        running_ur_tracking_numbers = []
        ur_list = self.build_ur_list(*ur_tracking_numbers)
        schedulable_urs = scheduler.blacklist_running_user_requests(ur_list, running_ur_tracking_numbers)
        assert_equal(len(ur_list), len(schedulable_urs))
        


