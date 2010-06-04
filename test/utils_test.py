from nose.tools import eq_, assert_equal, raises

# Import the module to test
from adaptive_scheduler.utils import dt_windows_intersect

from datetime import datetime


class test_dt_windows_intersect():
    """
    test_dt_windows_intersect
    Code based on: 
    http://beautifulisbetterthanugly.com/posts/2009/oct/7/datetime-intersection-python/
    http://stackoverflow.com/questions/143552/comparing-date-ranges  
    
               |-------------------|         compare to this one
    1               |---------|              contained within
    2          |----------|                  contained within, equal start
    3                  |-----------|         contained within, equal end
    4          |-------------------|         contained within, equal start+end
    5     |------------|                     overlaps start but not end
    6                      |-----------|     overlaps end but not start
    7     |------------------------|         overlaps start, but equal end
    8          |-----------------------|     overlaps end, but equal start
    9     |------------------------------|   overlaps entire range

    10 |---|                                 not overlap, less than
    11 |-------|                             not overlap, end equal
    12                              |---|    not overlap, bigger than
    13                             |---|     not overlap, start equal
    """

    
    def test_contained_within(self):
        assert dt_windows_intersect(
            datetime(2009,10,1,6,0),    datetime(2009,10,1,7,0),
            datetime(2009,10,1,6,30),   datetime(2009,10,1,6,40),
        )

    def test_contained_within_equal_start(self):
        assert dt_windows_intersect(
            datetime(2009,10,1,6,0),    datetime(2009,10,1,7,0),
            datetime(2009,10,1,6,0),    datetime(2009,10,1,6,30),
        )

    def test_contained_within_equal_end(self):
        assert dt_windows_intersect(
            datetime(2009,10,1,6,0),    datetime(2009,10,1,7,0),
            datetime(2009,10,1,6,30),   datetime(2009,10,1,7,0),
        )

    def test_contained_within_equal_start_and_end(self):
        assert dt_windows_intersect(
            datetime(2009,10,1,6,0),    datetime(2009,10,1,7,0),
            datetime(2009,10,1,6,0),    datetime(2009,10,1,7,0),
        )

    def test_overlaps_start_but_not_end(self):
        assert dt_windows_intersect(
            datetime(2009,10,1,6,0),    datetime(2009,10,1,7,0),
            datetime(2009,10,1,5,30),   datetime(2009,10,1,6,30),
        )

    def test_overlaps_end_but_not_start(self):
        assert dt_windows_intersect(
            datetime(2009,10,1,6,0),    datetime(2009,10,1,7,0),
            datetime(2009,10,1,6,30),   datetime(2009,10,1,7,30),
        )

    def test_overlaps_start_equal_end(self):
        assert dt_windows_intersect(
            datetime(2009,10,1,6,0),    datetime(2009,10,1,7,0),
            datetime(2009,10,1,5,30),   datetime(2009,10,1,7,0),
        )

    def test_equal_start_overlaps_end(self):
        assert dt_windows_intersect(
            datetime(2009,10,1,6,0),    datetime(2009,10,1,7,0),
            datetime(2009,10,1,6,0),    datetime(2009,10,1,7,30),
        )

    def test_overlaps_entire_range(self):
        assert dt_windows_intersect(
            datetime(2009,10,1,6,0),    datetime(2009,10,1,7,0),
            datetime(2009,10,1,5,0),    datetime(2009,10,1,8,0),
        )

    def test_not_overlap_less_than(self):
        assert not dt_windows_intersect(
            datetime(2009,10,1,6,0),    datetime(2009,10,1,7,0),
            datetime(2009,10,1,5,0),    datetime(2009,10,1,5,30),
        )

    def test_not_overlap_end_equal(self):
        assert not dt_windows_intersect(
            datetime(2009,10,1,6,0),    datetime(2009,10,1,7,0),
            datetime(2009,10,1,5,0),    datetime(2009,10,1,6,0),
        )

    def test_not_overlap_greater_than(self):
        assert not dt_windows_intersect(
            datetime(2009,10,1,6,0),    datetime(2009,10,1,7,0),
            datetime(2009,10,1,7,30),    datetime(2009,10,1,8,0),
        )

    def test_not_overlap_start_equal(self):
        assert not dt_windows_intersect(
            datetime(2009,10,1,6,0),    datetime(2009,10,1,7,0),
            datetime(2009,10,1,7,0),    datetime(2009,10,1,8,0),
        )
