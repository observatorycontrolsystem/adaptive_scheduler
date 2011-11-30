#!/usr/bin/env python

'''
test_contracts.py

Author: Sotiria Lampoudi
August 2011
'''

from nose.tools import assert_equal

from adaptive_scheduler.kernel.timepoint import *
from adaptive_scheduler.kernel.intervals import *
from adaptive_scheduler.kernel.reservation import *
from adaptive_scheduler.kernel.contracts import *

class TestContractualObligation(object):

    def setup(self):
        # c.o. 1: 3 hrs, between 6 and 12 in quantums of 1 hr
        self.co1 = ContractualObligation(3, 1, [Timepoint(6, 'start'),
                                                Timepoint(12, 'end')], 1, 'co1')
        # c.o. 2: same as c.o. 1 except priority 2 (lower than 1)
        self.co2 = ContractualObligation(3, 1, [Timepoint(6, 'start'),
                                                Timepoint(12, 'end')], 2, 'co2')


    def test_co_create(self):
        assert_equal(self.co1.total_time, 3)
        assert_equal(self.co1.time_to_schedule, 3)
        assert_equal(self.co1.min_quantum, 1)
        assert_equal(self.co1.max_quantum, None)
        assert_equal(self.co1.priority, 1)
        assert_equal(self.co1.name, 'co1')
        assert_equal(self.co1.free_windows.timepoints, [])
        assert_equal(self.co1.scheduled_windows, [])
        assert_equal(self.co1.possible_windows.timepoints[0].time, 6)
        assert_equal(self.co1.possible_windows.timepoints[1].time, 12)


    def test_co_sort(self):
        colist=[self.co2, self.co1]
        colist.sort()
        assert_equal(colist[0].priority,1)
        assert_equal(colist[1].priority,2)



class TestContractualObligationScheduler(object):

    def setup(self):
        # sched1: every 4th hour is busy
        self.sched1 = []
        for i in range(0,23,4):
            r       = Reservation(1, i, i) # duration, earliest, latest start
            r.start = i
            r.end   = i+1
            self.sched1.append(r)
        # sched2: contains one 2hr reservation starting @ 2
        self.sched2 = []
        r = Reservation(2, 2, 3)
        r.start     = 2
        r.end       = 4
        self.sched2.append(r)
        # sched3: 24 hrs completely full
        self.sched3 = []
        r = Reservation(24, 0, 0)
        r.start     = 0
        r.end       = 24
        self.sched3.append(r)
        # sched4: 24 hrs completely empty
        self.sched4 = []

        # c.o. 1: 3 hrs, between 6 and 12 in quantums of 1 hr
        self.co1 = ContractualObligation(3, 1, [Timepoint(6, 'start'),
                                                Timepoint(12, 'end')], 1, 'co1')
        # c.o. 2: same as c.o. 1 except priority 2 (lower than 1)
        self.co2 = ContractualObligation(3, 1, [Timepoint(6, 'start'),
                                                Timepoint(12, 'end')], 2, 'co2')
        # c.o. 3: 4 hrs, between 0 and 6 in quantums of 2 hrs
        self.co3 = ContractualObligation(4, 2, [Timepoint(0, 'start'),
                                                Timepoint(6, 'end')], 1, 'co3')
        # c.o. 4: 1 hr between 11 and 12
        self.co4 = ContractualObligation(1, 1, [Timepoint(11, 'start'),
                                                Timepoint(12, 'end')], 1, 'co4')
        # c.o. 5: 2 hrs between 11 and 13
        self.co5 = ContractualObligation(2, 1, [Timepoint(11, 'start'),
                                                Timepoint(13, 'end')], 1, 'co5')


    def test_create(self):
        '''
        Make sure schedule_free, free_windows and quantum get set properly
        '''
        cos = ContractualObligationScheduler(self.sched2,
                                             [Timepoint(0, 'start'),
                                              Timepoint(24, 'end')],
                                             [self.co3])
        assert_equal(cos.schedule_free.timepoints[0].time, 0)
        assert_equal(cos.schedule_free.timepoints[0].type, 'start')
        assert_equal(cos.schedule_free.timepoints[1].time, 2)
        assert_equal(cos.schedule_free.timepoints[1].type, 'end')
        assert_equal(cos.schedule_free.timepoints[2].time, 4)
        assert_equal(cos.schedule_free.timepoints[2].type, 'start')
        assert_equal(cos.schedule_free.timepoints[3].time, 24)
        assert_equal(cos.schedule_free.timepoints[3].type, 'end')

        assert_equal(self.co3.free_windows.timepoints[0].time, 0)
        assert_equal(self.co3.free_windows.timepoints[0].type, 'start')
        assert_equal(self.co3.free_windows.timepoints[1].time, 2)
        assert_equal(self.co3.free_windows.timepoints[1].type, 'end')
        assert_equal(self.co3.free_windows.timepoints[2].time, 4)
        assert_equal(self.co3.free_windows.timepoints[2].type, 'start')
        assert_equal(self.co3.free_windows.timepoints[3].time, 6)
        assert_equal(self.co3.free_windows.timepoints[3].type, 'end')


    def test_build_intervals_from_schedule_1(self):
        r1=Reservation(1, 1, 1)
        r1.start=1
        r1.end=2
        r2=Reservation(3, 3, 3)
        r2.start=3
        r2.end=6
        cos = ContractualObligationScheduler([r1,r2],
                                             [Timepoint(0, 'start'),
                                              Timepoint(24, 'end')],
                                             [self.co3])
        i = cos.build_intervals_from_schedule([r1,r2])
        assert_equal(i.timepoints[0].time, 1)
        assert_equal(i.timepoints[0].type, 'start')
        assert_equal(i.timepoints[1].time, 2)
        assert_equal(i.timepoints[1].type, 'end')
        assert_equal(i.timepoints[2].time, 3)
        assert_equal(i.timepoints[2].type, 'start')
        assert_equal(i.timepoints[3].time, 6)
        assert_equal(i.timepoints[3].type, 'end')


    def test_build_intervals_from_schedule_2(self):
        '''
        Stitching two reservations together
        '''
        r1=Reservation(1, 1, 1)
        r1.start=1
        r1.end=2
        r2=Reservation(3, 2, 2)
        r2.start=2
        r2.end=5
        cos = ContractualObligationScheduler([r1,r2],
                                             [Timepoint(0, 'start'),
                                              Timepoint(24, 'end')],
                                             [self.co3])
        i = cos.build_intervals_from_schedule([r1,r2])
        assert_equal(i.timepoints[0].time, 1)
        assert_equal(i.timepoints[0].type, 'start')
        assert_equal(i.timepoints[1].time, 5)
        assert_equal(i.timepoints[1].type, 'end')


    def test_find_uncontended_windows_1(self):
        '''
        Find uncontended windows when there are some
        '''
        cos = ContractualObligationScheduler(self.sched1,
                                             [Timepoint(0, 'start'),
                                              Timepoint(24, 'end')],
                                             [self.co1])

        uncontended = cos.find_uncontended_windows(self.co1)
        assert_equal(uncontended.timepoints[0].time, 6)
        assert_equal(uncontended.timepoints[1].time, 8)
        assert_equal(uncontended.timepoints[2].time, 9)
        assert_equal(uncontended.timepoints[3].time, 12)


    def test_find_uncontended_windows_2(self):
        ''' 
        Find uncontended windows when there are none
        '''
        cos = ContractualObligationScheduler(self.sched1,
                                             [Timepoint(0, 'start'),
                                              Timepoint(24, 'end')],
                                             [self.co1, self.co2])

        uncontended = cos.find_uncontended_windows(self.co1)
        assert_equal(uncontended.is_empty(), True)


    def test_find_contended_windows_1(self):
        '''
        Find contended windows when there are some
        '''
        cos = ContractualObligationScheduler(self.sched1,
                                             [Timepoint(0, 'start'),
                                              Timepoint(24, 'end')],
                                             [self.co1, self.co2])
        contended = cos.find_contended_windows(self.co1)
        assert_equal(contended.timepoints[0].time, 6)
        assert_equal(contended.timepoints[1].time, 8)
        assert_equal(contended.timepoints[2].time, 9)
        assert_equal(contended.timepoints[3].time, 12)


    def test_find_contended_windows_2(self):
        '''
        Find contended windows when there are none.
        '''
        cos = ContractualObligationScheduler(self.sched1,
                                             [Timepoint(0, 'start'),
                                              Timepoint(24, 'end')],
                                             [self.co1, self.co3])
        contended = cos.find_contended_windows(self.co1)
        assert_equal(contended.is_empty(), True)


    def test_schedule_1(self):
        '''
        Single c.o., possible
        '''
        cos = ContractualObligationScheduler(self.sched1,
                                             [Timepoint(0, 'start'),
                                              Timepoint(24, 'end')],
                                             [self.co1])
        subscheds = cos.schedule()
        co1sched = subscheds['co1']
        assert_equal(co1sched[0].start, 6)
        assert_equal(co1sched[0].end, 8)
        assert_equal(co1sched[1].start, 9)
        assert_equal(co1sched[1].end, 10)


    def test_schedule_2(self):
        '''
        Two identical c.o.'s, one unsatisfied
        '''
        cos = ContractualObligationScheduler(self.sched1,
                                             [Timepoint(0, 'start'),
                                              Timepoint(24, 'end')],
                                             [self.co1, self.co2])
        subscheds = cos.schedule()
        co1sched = subscheds['co1']
        assert_equal(co1sched[0].start, 6)
        assert_equal(co1sched[0].end, 8)
        assert_equal(co1sched[1].start, 9)
        assert_equal(co1sched[1].end, 10)
        co2sched = subscheds['co2']
        assert_equal(co2sched[0].start, 10)
        assert_equal(co2sched[0].end, 12)


    def test_schedule_3(self):
        '''
        two c.o.'s, the first can be satisfied w/ uncontended time,
        the second has to dip into contended time.
        '''
        cos = ContractualObligationScheduler(self.sched1,
                                             [Timepoint(0, 'start'),
                                              Timepoint(24, 'end')],
                                             [self.co1, self.co4])
        subscheds = cos.schedule()
        subscheds = cos.schedule()
        co1sched = subscheds['co1']
        assert_equal(co1sched[0].start, 6)
        assert_equal(co1sched[0].end, 8)
        assert_equal(co1sched[1].start, 9)
        assert_equal(co1sched[1].end, 10)
        co2sched = subscheds['co4']
        assert_equal(co2sched[0].start, 11)
        assert_equal(co2sched[0].end, 12)


    def test_schedule_4(self):
        '''
        two c.o.'s, the first can be satisfied w/ uncontended time,
        the second has to dip into contended time, and still leaves 1
        hr unsatisfied.
        '''
        cos = ContractualObligationScheduler(self.sched1,
                                             [Timepoint(0, 'start'),
                                              Timepoint(24, 'end')],
                                             [self.co1, self.co5])
        subscheds = cos.schedule()
        co1sched = subscheds['co1']
        assert_equal(co1sched[0].start, 6)
        assert_equal(co1sched[0].end, 8)
        assert_equal(co1sched[1].start, 9)
        assert_equal(co1sched[1].end, 10)
        co2sched = subscheds['co5']
        assert_equal(co2sched[0].start, 11)
        assert_equal(co2sched[0].end, 12)


    def test_schedule_full(self):
        '''
        schedule is completely full
        '''
        cos = ContractualObligationScheduler(self.sched3,
                                             [Timepoint(0, 'start'),
                                              Timepoint(24, 'end')],
                                             [self.co1, self.co5])
        subscheds = cos.schedule()
        co1sched = subscheds['co1']
        assert_equal(co1sched, [])
        co2sched = subscheds['co5']
        assert_equal(co2sched, [])


    def test_schedule_empty(self):
        '''
        schedule is completely empty
        '''
        cos = ContractualObligationScheduler(self.sched4,
                                             [Timepoint(0, 'start'),
                                              Timepoint(24, 'end')],
                                             [self.co1, self.co5])
        subscheds = cos.schedule()
        co1sched = subscheds['co1']
        assert_equal(co1sched[0].start, 6)
        assert_equal(co1sched[0].end, 9)
        co2sched = subscheds['co5']
        assert_equal(co2sched[0].start, 11)
        assert_equal(co2sched[0].end, 13)

