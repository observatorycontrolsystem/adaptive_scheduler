from nose.tools import eq_
from datetime import datetime

from adaptive_scheduler import semester_service

def test_return_semester_start_when_in_semester_A():
    now = datetime(2013, 5, 1)
    semester_start = semester_service.get_semester_start(now)
    eq_(semester_start, datetime(2013, 4, 1))

def test_return_semester_end_when_in_semester_A():
    now = datetime(2013,5,1)
    semester_end = semester_service.get_semester_end(now)
    eq_(semester_end, datetime(2013, 10, 1))

def test_return_semester_start_when_in_semester_B():
    now = datetime(2013, 11, 1)
    semester_start = semester_service.get_semester_start(now)
    eq_(semester_start, datetime(2013, 10, 1))

    now = datetime(2014, 2, 1)
    semester_start = semester_service.get_semester_start(now)
    eq_(semester_start, datetime(2013, 10, 1))

def test_return_semester_end_when_in_semester_B():
    now = datetime(2013, 11, 1)
    semester_end = semester_service.get_semester_end(now)
    eq_(semester_end, datetime(2014, 4, 1))

    now = datetime(2014, 2, 1)
    semester_end = semester_service.get_semester_end(now)
    eq_(semester_end, datetime(2014, 4, 1))

def test_return_semester_block_when_in_semester_B():
    now            = datetime(2013, 2, 28)
    expected_start = datetime(2012, 10, 1)
    expected_end   = datetime(2013, 4, 1)
    semester_block = semester_service.get_semester_block(now)
    eq_(semester_block, (expected_start, expected_end))

def test_return_semester_code():
    now = datetime(2013, 2, 28)
    eq_(semester_service.get_semester_code(now), '2012B')

    now = datetime(2013, 5, 28)
    eq_(semester_service.get_semester_code(now), '2013A')

    now = datetime(2013, 11, 1)
    eq_(semester_service.get_semester_code(now), '2013B')
