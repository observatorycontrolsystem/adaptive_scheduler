'''
test_camera_mapping.py - Test cases for the camera_mapping module.

Authors:
    Martin Norbury (mnorbury@lcogt.net)

January 2013
'''
from nose.tools import eq_

from adaptive_scheduler.camera_mapping import create_camera_mapping

def test_find_by_location():
    camera_mapping = create_camera_mapping('camera_mappings.dat')
    cameras = camera_mapping.find_by_location('bpl','doma','1m0a')
    eq_( len(cameras), 2 )


def test_find_by_autoguider():
    camera_mapping = create_camera_mapping('camera_mappings.dat')
    camera = camera_mapping.find_by_autoguider('kb20')[0]
    eq_( camera['site'], 'bpl' )
    eq_( camera['observatory'], 'doma')
    eq_( camera['telescope'], '1m0a' )

def test_find_by_autoguider_type():
    camera_mapping = create_camera_mapping('camera_mappings.dat')
    cameras = camera_mapping.find_by_autoguider_type('OffAxis')
    eq_( len(cameras), 12 )

def test_find_by_camera():
    camera_mapping = create_camera_mapping('camera_mappings.dat')
    camera = camera_mapping.find_by_camera('kb72')[0]
    eq_( camera['site'], 'bpl' )
    eq_( camera['observatory'], 'doma')
    eq_( camera['telescope'], '1m0a' )

def test_find_by_camera_type():
    camera_mapping = create_camera_mapping('camera_mappings.dat')
    cameras = camera_mapping.find_by_camera_type('1m0-SciCam')
    eq_( len(cameras), 11 )
