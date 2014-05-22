from adaptive_scheduler.model2          import ( UserRequest, Request, Window,
                                                 Windows, Telescope )

from mock import Mock, PropertyMock
from datetime import datetime, timedelta


def create_mock_proposal():
    mock_proposal = Mock(priority=10)
    
    return mock_proposal

def create_user_request(window_dicts, operator='and', resource_name='Martin', target=None, molecules=None, proposal=create_mock_proposal(), expires=None, duration=60):
        t1 = Telescope(
                        name = resource_name
                      )

        req_list = []
        window_list = []
        for req_windows in window_dicts:
            windows = Windows()
            for window_dict in req_windows:
                w = Window(
                            window_dict = window_dict,
                            resource    = t1
                          )
                windows.append(w)
                window_list.append(w)

            Request.duration = PropertyMock(return_value=duration)
            r  = Request(
                          target         = target,
                          molecules      = molecules,
                          windows        = windows,
                          constraints    = None,
                          request_number = '0000000005'
                        )
            
            r.get_duration = Mock(return_value=duration) 
                
            
            req_list.append(r)

        if len(req_list) == 1:
            operator = 'single'

        if expires:
            UserRequest.expires = PropertyMock(return_value=expires)
        else:
            UserRequest.expires = PropertyMock(return_value=datetime.utcnow() + timedelta(days=365))
        ur1 = UserRequest(
                           operator        = operator,
                           requests        = req_list,
                           proposal        = proposal,
                           expires         = None,
                           tracking_number = '0000000005',
                           group_id        = None
                         )

        return ur1, window_list