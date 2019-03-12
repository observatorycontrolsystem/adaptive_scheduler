from adaptive_scheduler.models          import (RequestGroup, Request, Window, Windows)

from mock import Mock, PropertyMock
from datetime import datetime, timedelta


def create_mock_proposal():
    mock_proposal = Mock(priority=10)
    
    return mock_proposal


def create_request_group(window_dicts, operator='and', resource_name='Martin', configurations=None, proposal=create_mock_proposal(), expires=None, duration=60, first_request_id=5, request_group_id=5):
        t1 = {'name': resource_name}

        req_list = []
        window_list = []
        next_request_id = int(first_request_id)
        for req_windows in window_dicts:
            windows = Windows()
            for window_dict in req_windows:
                w = Window(
                            window_dict = window_dict,
                            resource    = t1['name']
                          )
                windows.append(w)
                window_list.append(w)

            r  = Request(
                          configurations= configurations,
                          windows        = windows,
                          id= next_request_id
                        )

            r.get_duration = Mock(return_value=duration) 
                
            
            req_list.append(r)
            next_request_id += 1

        if len(req_list) == 1:
            operator = 'single'

        if expires:
            RequestGroup.expires = PropertyMock(return_value=expires)
        else:
            RequestGroup.expires = PropertyMock(return_value=datetime.utcnow() + timedelta(days=365))
        rg = RequestGroup(
                           operator        = operator,
                           requests        = req_list,
                           proposal        = proposal,
                           expires         = None,
                           id= request_group_id,
                           name= None,
                           ipp_value       = 1.0,
                           observation_type= 'NORMAL',
                           submitter       = '',
                         )

        return rg, window_list