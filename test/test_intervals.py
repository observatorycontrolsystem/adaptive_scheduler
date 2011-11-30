#!/usr/bin/env python

'''
test_intervals.py

Author: Sotiria Lampoudi
August 2011
edited November 2011: added test_add_1(), test_add_2()
'''

from nose.tools import assert_equal

from kernel.timepoint import *
from kernel.intervals import *

class TestIntervals(object):
    
    def setup(self):
        # t1(1) ----  t2(3) 
        #       t3(2) ----- t4(4) 
        #                   t5(4) t6(5)
        t1=Timepoint(1,'start');
        t2=Timepoint(3, 'end');
        
        t3=Timepoint(2, 'start');
        t4=Timepoint(4, 'end');

        t5=Timepoint(4, 'start');
        t6=Timepoint(5, 'end');

        self.i1=Intervals([t1, t2, t5, t6], 'free')
        self.i2=Intervals([t3, t4])
        self.i3=Intervals([t1, t2, t5, t6, t1, Timepoint(2,'end')])
        self.i4=Intervals([t1, t2, t5, t6, Timepoint(6, 'start'), Timepoint(7, 'end')], 'free')
        self.i5=Intervals([t1, t6])
        self.i6=Intervals([t1, t2, t5, t6, Timepoint(2, 'start'), Timepoint(2,'end')])
        self.i8=Intervals([])
    

    def test_create(self):
        assert_equal(self.i1.timepoints[0].time, 1)
        assert_equal(self.i1.timepoints[0].type, 'start')
        assert_equal(self.i1.timepoints[1].time, 3)
        assert_equal(self.i1.timepoints[1].type, 'end')
        assert_equal(self.i1.timepoints[2].time, 4)
        assert_equal(self.i1.timepoints[2].type, 'start')
        assert_equal(self.i1.timepoints[3].time, 5)
        assert_equal(self.i1.timepoints[3].type, 'end')


    def test_add_1(self):
        self.i1.add([Timepoint(6, 'start'), Timepoint(7, 'end')])
        assert_equal(self.i1.timepoints[4].time, 6)
        assert_equal(self.i1.timepoints[4].type, 'start')
        assert_equal(self.i1.timepoints[5].time, 7)
        assert_equal(self.i1.timepoints[5].type, 'end')


    def test_add_2(self):
        '''
        Tests an add() that introduced two start-ends for removal
        by clean_up()
        '''
        self.i4.add([Timepoint(5, 'start'), Timepoint(6, 'end')])
        assert_equal(self.i4.timepoints[3].time, 7)
        assert_equal(self.i4.timepoints[3].type, 'end')


    def test_intersect_1(self):
        i = self.i1.intersect([self.i1])
        assert_equal(i.timepoints[0].time, 1)
        assert_equal(i.timepoints[0].type, 'start')
        assert_equal(i.timepoints[1].time, 3)
        assert_equal(i.timepoints[1].type, 'end')
        assert_equal(i.timepoints[2].time, 4)
        assert_equal(i.timepoints[2].type, 'start')
        assert_equal(i.timepoints[3].time, 5)
        assert_equal(i.timepoints[3].type, 'end')


    def test_intersect_2(self):
        i = self.i1.intersect([self.i2])
        assert_equal(i.timepoints[0].time, 2)
        assert_equal(i.timepoints[0].type, 'start')
        assert_equal(i.timepoints[1].time, 3)
        assert_equal(i.timepoints[1].type, 'end')


    def test_intersect_empty(self):
        i = self.i1.intersect([self.i8])
        assert_equal(i.timepoints, [])


    def test_complement(self):
        self.i1.complement(0,10)
        assert_equal(self.i1.timepoints[0].time, 0)
        assert_equal(self.i1.timepoints[0].type, 'start')
        assert_equal(self.i1.timepoints[1].time, 1)
        assert_equal(self.i1.timepoints[1].type, 'end')
        assert_equal(self.i1.timepoints[2].time, 3)
        assert_equal(self.i1.timepoints[2].type, 'start')
        assert_equal(self.i1.timepoints[3].time, 4)
        assert_equal(self.i1.timepoints[3].type, 'end')
        assert_equal(self.i1.timepoints[4].time, 5)
        assert_equal(self.i1.timepoints[4].type, 'start')
        assert_equal(self.i1.timepoints[5].time, 10)
        assert_equal(self.i1.timepoints[5].type, 'end')
        

    def test_subtract_1(self):
        i = self.i1.subtract(self.i2)
        assert_equal(i.timepoints[0].time, 1)
        assert_equal(i.timepoints[0].type, 'start')
        assert_equal(i.timepoints[1].time, 2)
        assert_equal(i.timepoints[1].type, 'end')
        assert_equal(i.timepoints[2].time, 4)
        assert_equal(i.timepoints[2].type, 'start')
        assert_equal(i.timepoints[3].time, 5)
        assert_equal(i.timepoints[3].type, 'end')


    def test_subtract_2(self):
        interval = Intervals([Timepoint(1,'start'), Timepoint(2, 'end')])
        i = self.i5.subtract(interval)
        assert_equal(i.timepoints[0].time, 2)
        assert_equal(i.timepoints[0].type, 'start')
        assert_equal(i.timepoints[1].time, 5)
        assert_equal(i.timepoints[1].type, 'end')
        

    def test_subtract_empty(self):
        i = self.i1.subtract(Intervals([]))
        assert_equal(i.timepoints[0].time, 1)
        assert_equal(i.timepoints[0].type, 'start')
        assert_equal(i.timepoints[1].time, 3)
        assert_equal(i.timepoints[1].type, 'end')
        assert_equal(i.timepoints[2].time, 4)
        assert_equal(i.timepoints[2].type, 'start')
        assert_equal(i.timepoints[3].time, 5)
        assert_equal(i.timepoints[3].type, 'end')
        

    def test_subtract_from_empty(self):
        i = self.i8.subtract(self.i1)
        assert_equal(i.timepoints, [])


    def test_clean_up_1(self):
        ''' 
        Nested interval clean up 
        '''
        self.i3.clean_up()
        assert_equal(self.i3.timepoints[0].time, 1)
        assert_equal(self.i3.timepoints[0].type, 'start')
        assert_equal(self.i3.timepoints[1].time, 3)
        assert_equal(self.i3.timepoints[1].type, 'end')
        assert_equal(self.i3.timepoints[2].time, 4)
        assert_equal(self.i3.timepoints[2].type, 'start')
        assert_equal(self.i3.timepoints[3].time, 5)
        assert_equal(self.i3.timepoints[3].type, 'end')


    def test_clean_up_2(self):
        '''
        Empty interval clean up
        '''
        self.i6.clean_up()
        assert_equal(self.i6.timepoints[0].time, 1)
        assert_equal(self.i6.timepoints[0].type, 'start')
        assert_equal(self.i6.timepoints[1].time, 3)
        assert_equal(self.i6.timepoints[1].type, 'end')
        assert_equal(self.i6.timepoints[2].time, 4)
        assert_equal(self.i6.timepoints[2].type, 'start')
        assert_equal(self.i6.timepoints[3].time, 5)
        assert_equal(self.i6.timepoints[3].type, 'end')


    def test_clean_up_empty(self):
        self.i8.clean_up()
        assert_equal(self.i8.timepoints, [])


    def test_get_total_time(self):
        t = self.i1.get_total_time()
        assert_equal(t, 3)


    def test_get_total_time_empty(self):
        t = self.i8.get_total_time()
        assert_equal(t,0)


    def test_get_quantum_starts_1(self):
        qs = self.i1.get_quantum_starts(1)
        assert_equal(qs[0], 1)
        assert_equal(qs[1], 2)
        assert_equal(qs[2], 4)


    def test_get_quantum_starts_2(self):
        '''
        Impossible to align w/ start of quantum
        '''
        qs = self.i1.get_quantum_starts(2)
        assert_equal(qs, [])


    def test_trim_to_time(self):
        self.i1.trim_to_time(2)
        assert_equal(self.i1.timepoints[0].time, 1)
        assert_equal(self.i1.timepoints[0].type, 'start')
        assert_equal(self.i1.timepoints[1].time, 3)
        assert_equal(self.i1.timepoints[1].type, 'end')


