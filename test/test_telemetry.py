'''
test_telemetry.py - Test cases for the telemetry module.

description

Author: Martin Norbury
May 2013
'''
import os, inspect
from sqlalchemy.engine import create_engine
from nose.tools import eq_
from datetime   import datetime

from adaptive_scheduler.monitoring.telemetry import get_datum

def _get_db_path(db_file='harvest.sqlite'):
    abspath = os.path.abspath(inspect.getsourcefile(_get_db_path))
    path, _ = os.path.split(abspath)
    return '/'.join([path,db_file])

engine = create_engine('sqlite:////%s' % _get_db_path())

def test_reading_all_datums():
    eq_(len(get_datum('Weather Ok To Open',engine=engine)), 24)

def test_reading_selected_datum_instances():
    eq_(len(get_datum('Weather Ok To Open', 1, engine=engine)), 8)

def test_datum_keys():
    datum = get_datum('Weather Ok To Open', 1, engine=engine)[0]

    actual_keys   = set(datum._asdict().keys())
    expected_keys = set(['site','observatory','telescope',
                         'timestamp_changed','timestamp_measured',
                         'instance','value','persistence_model'])
    eq_(actual_keys, expected_keys)

def test_timestamps_are_datetimes_in_correct_order():
    oldest = get_datum('Weather Ok To Open', 1, engine=engine)[0]
    eq_(oldest.timestamp_changed , datetime(2013,3,14,22,3,8,593000))
    eq_(oldest.timestamp_measured, datetime(2013,3,14,22,4,9,220000))

    newest = get_datum('Weather Ok To Open', 1, engine=engine)[-1]
    eq_(newest.timestamp_changed , datetime(2013,5,30,21,15,31,516000))
    eq_(newest.timestamp_measured, datetime(2013,5,30,22,51,46,195000))
