from nose.tools import eq_
from datetime import datetime

from adaptive_scheduler import semester_service

def test_return_semester_start_when_in_semester_A():
    now = datetime(2012, 5, 1)
    semester_start = semester_service.get_semester_start(now)
    eq_(semester_start, datetime(2012, 4, 1))

def test_return_semester_end_when_in_semester_A():
    now = datetime(2012,5,1)
    semester_end = semester_service.get_semester_end(now)
    eq_(semester_end, datetime(2012, 10, 1))

def test_return_semester_start_when_in_semester_B():
    now = datetime(2012, 11, 1)
    semester_start = semester_service.get_semester_start(now)
    eq_(semester_start, datetime(2012, 10, 1))

    now = datetime(2013, 2, 1)
    semester_start = semester_service.get_semester_start(now)
    eq_(semester_start, datetime(2012, 10, 1))

def test_return_semester_end_when_in_semester_B():
    now = datetime(2012, 11, 1)
    semester_end = semester_service.get_semester_end(now)
    eq_(semester_end, datetime(2013, 4, 1))

    now = datetime(2012, 2, 1)
    semester_end = semester_service.get_semester_end(now)
    eq_(semester_end, datetime(2012, 4, 1))

def test_return_semester_block_when_in_semester_B():
    now            = datetime(2012, 2, 28)
    expected_start = datetime(2011, 10, 1)
    expected_end   = datetime(2012, 4, 1)
    semester_block = semester_service.get_semester_block(now)
    eq_(semester_block, (expected_start, expected_end))

def test_return_semester_code():
    now = datetime(2012, 2, 28)
    eq_(semester_service.get_semester_code(now), '2011B')

    now = datetime(2012, 5, 28)
    eq_(semester_service.get_semester_code(now), '2012A')

    now = datetime(2012, 11, 1)
    eq_(semester_service.get_semester_code(now), '2012B')

# Tests for special semesters 2013B and 2014A
def test_2013A_start_is_not_special():
    now = datetime(2013, 5, 1)
    semester_start = semester_service.get_semester_start(now)
    eq_(semester_start, datetime(2013, 4, 1))

def test_2013A_end_is_not_special():
    now = datetime(2013, 5, 1)
    semester_end = semester_service.get_semester_end(now)
    eq_(semester_end, datetime(2013, 10, 1))

def test_2013B_start_is_not_special():
    now = datetime(2013, 11, 1)
    semester_start = semester_service.get_semester_start(now)
    eq_(semester_start, datetime(2013, 10, 1))

def test_2013B_end_is_may():
    now = datetime(2013, 11, 1)
    semester_end = semester_service.get_semester_end(now)
    eq_(semester_end, datetime(2014, 5, 1))

def test_2014A_start_is_may():
    now = datetime(2014, 5, 1)
    semester_start = semester_service.get_semester_start(now)
    eq_(semester_start, datetime(2014, 5, 1))

def test_2014A_end_is_not_special():
    now = datetime(2014, 5, 1)
    semester_end = semester_service.get_semester_end(now)
    eq_(semester_end, datetime(2014, 10, 1))

def test_2014B_start_is_not_special():
    now = datetime(2014, 10, 1)
    semester_start = semester_service.get_semester_start(now)
    eq_(semester_start, datetime(2014, 10, 1))

def test_2013B_semester_block():
    now            = datetime(2013, 11, 1)
    expected_start = datetime(2013, 10, 1)
    expected_end   = datetime(2014, 5, 1)
    semester_block = semester_service.get_semester_block(now)
    eq_(semester_block, (expected_start, expected_end))

def test_2014A_semester_block():
    now            = datetime(2014, 6, 1)
    expected_start = datetime(2014, 5, 1)
    expected_end   = datetime(2014, 10, 1)
    semester_block = semester_service.get_semester_block(now)
    eq_(semester_block, (expected_start, expected_end))

def test_2013B_april_extension_semester_code():
    now = datetime(2014, 4, 1)
    eq_(semester_service.get_semester_code(now), '2013B')

def test_2014A_semester_code_from_may():
    now = datetime(2014, 5, 1)
    eq_(semester_service.get_semester_code(now), '2014A')

def test_2013B_last_day_semester_code():
    now = datetime(2014, 4, 30)
    eq_(semester_service.get_semester_code(now), '2013B')

