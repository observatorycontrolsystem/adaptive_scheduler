#!/usr/bin/env python

'''
Reservation_v3 and CompoundReservation_v2 classes for scheduling.

Author: Sotiria Lampoudi (slampoud@gmail.com)
December 2012

Reservation_v3 does not associate a single resource with each reservation. 
Instead, the possible_windows field has become possible_windows_dict, a 
dictionary mapping :
resource -> possible windows on that resource

Additionally, it is allowed to explicitly specify the resID, so as to 
keep a uniform ID space between this and other parts of the scheduler.
'''

import copy

class Reservation_v3(object):

    resID = 0

    def __init__(self, priority, duration, possible_windows_dict, resID=None, previous_solution_reservation=None):
        self.priority = priority
        self.duration = int(duration)
        self.previous_solution_reservation = previous_solution_reservation
        self.possible_windows_dict = possible_windows_dict
        # free_windows keeps track of which of the possible_windows 
        # are free.
        self.free_windows_dict    = copy.deepcopy(self.possible_windows_dict)
        # clean up free windows by removing ones that are too small:
        for resource in self.free_windows_dict.keys():
            self.clean_up_free_windows(resource)
        # set a unique resID. 
        # ALERT: possible pitfall! Mixing incremented resID's and assigned
        # resID's is unsafe, because they can clash. Pick one and stick with
        # it. Either always let the class do IDs or always assign them.
        if resID:
            self.resID                = resID
        else:
            Reservation_v3.resID     += 1
            self.resID                = Reservation_v3.resID
        # these fields are defined when the reservation is ultimately scheduled
        self.scheduled_start      = None
        self.scheduled_quantum    = None
        self.scheduled_resource   = None
        self.scheduled            = False
        self.scheduled_timepoints = None
        self.scheduled_by         = None
        # order is the parameter used for grouping & ordering in scheduling
        self.order                = 1
        self.compound_reservation_parent = None


    def schedule_anywhere(self):
        # find the first available spot & stick it there
        for resource in self.free_windows_dict.keys():
            start = self.free_windows_dict[resource].find_interval_of_length(self.duration)
            if start >=0:
                self.schedule(start, self.duration, resource, 
                              'reservation_v3.schedule_anywhere()')
                return True
        return False

    
    def schedule(self, start, quantum, resource, scheduler_description=None):
        self.scheduled          = True
        self.scheduled_start    = start
        self.scheduled_quantum  = quantum
        self.scheduled_resource = resource
        self.scheduled_timepoints = [{'time': start, 'type': 'start'}, {'time': start+self.duration, 'type': 'end'}]
        self.scheduled_by       = scheduler_description
        if self.compound_reservation_parent:
            self.compound_reservation_parent.schedule()


    def unschedule(self):
        self.scheduled_start    = None
        self.scheduled_quantum  = None
        self.scheduled_resource = None
        self.scheduled          = False
        self.scheduled_timepoints = None
        self.scheduled_by       = None
        if self.compound_reservation_parent:
            self.compound_reservation_parent.unschedule()


    def __str__(self):
        str = "Reservation ID: {0} \
        \n\tpriority: {1} \
        \n\tduration: {2} \
        \n\tpossible windows dict: {3}\
        \n\tis scheduled: {4}\n".format(self.resID, self.priority, 
                                        self.duration, 
                                        self.possible_windows_dict, 
                                        self.scheduled)
        if self.scheduled:
            str += "\t\tscheduled start: {0}\n\t\tscheduled quantum: {1}\n\t\tscheduled resource: {2}\n\t\tscheduled by: {3}\n". format(self.scheduled_start, self.scheduled_quantum, self.scheduled_resource, self.scheduled_by)
        return str


    def __repr__(self):
        return str(self.serialise())


    def serialise(self):
        serialised_windows = dict([(k, v.serialise()) for k,v in self.possible_windows_dict.items()])
        return dict(
#                          resID                 = self.resID,
                      priority              = self.priority,
                      duration              = self.duration,
                      possible_windows_dict = serialised_windows,
                      scheduled             = self.scheduled
                    )


    def __lt__(self, other):
        ''' Higher priority number is higher priority. 
        If priority numbers are equal, then reservation belonging to 
        c.r.s are ranked as and < single < oneof '''
        if self.priority == other.priority:
            if (self.compound_reservation_parent) and (other.compound_reservation_parent):
                selftype  = self.compound_reservation_parent.type
                othertype = other.compound_reservation_parent.type
                if selftype == othertype:
                    return self.priority > other.priority
                elif selftype == 'and':
                    return True
                elif othertype == 'and':
                    return False
                elif selftype == 'oneof':
                    return False
                elif othertype == 'oneof':
                    return True
            else: 
                return self.priority > other.priority
        else: 
            return self.priority > other.priority

    
    def get_ID(self):
        return self.resID


    def remove_from_free_windows(self, interval, resource):
        self.free_windows_dict[resource] = self.free_windows_dict[resource].subtract(interval)
        self.clean_up_free_windows(resource)

        
    def clean_up_free_windows(self, resource):
        self.free_windows_dict[resource].remove_intervals_smaller_than(self.duration)


class CompoundReservation_v2(object):

    valid_types = {
        'single' : 'A single one of the provided blocks is to be scheduled',
        'oneof'  : 'One of the provided blocks are to be scheduled',
        'and'    : 'All of the provided blocks are to be scheduled',
        'many'   : 'Any of the provided blocks are to be scheduled individually'
        }

    def __init__(self, reservation_list, type='single'):
        self.reservation_list = reservation_list
        for r in self.reservation_list:
            r.compound_reservation_parent = self
        self.type = type
        # allowed types are:
        # single
        # oneof
        # and
        self.size       = len(reservation_list)
        if type == 'single' and self.size > 1:
            msg = ( "Initializing a CompoundReservation as 'single' but with %d "
                    "individual reservations. Ignoring all but the first."
                    % self.size )
            print(msg)
            self.size = 1
            self.reservation_list = [reservation_list.pop(0)]
        if (type == 'and') and (self.size == 1):
            msg = ( "Initializing a CompoundReservation as 'and' but with %d "
                    "individual reservation."
                    % self.size )
            print(msg)
        if type == 'oneof' and self.size == 1:
            msg = ( "Initializing a CompoundReservation as 'oneof' but with %d "
                    "individual reservation."
                    % self.size )
            print(msg)
        self.scheduled = False


    def schedule(self):
        if self.type == 'single':
            self.scheduled = True
        elif self.type == 'oneof':
            self.scheduled = True
        elif self.type == 'and':
            count = 0
            for r in self.reservation_list:
                if r.scheduled:
                    count += 1
            if count == self.size:
                self.scheduled = True

    
    def unschedule(self):
        if self.type == 'single':
            self.scheduled = False
        elif self.type == 'oneof':
            self.scheduled = False
            for r in self.reservation_list:
                if r.scheduled:
                    self.scheduled = True
        elif self.type == 'and':
            self.scheduled = False


    def issingle(self):
        if self.type == "single":
            return True
        else:
            return False


    def isoneof(self):
        if self.type == "oneof":
            return True
        else:
            return False


    def isand(self):
        if self.type == "and":
            return True
        else:
            return False


    def __repr__(self):
        return str(self.serialise())

    def serialise(self):
        reservation_list_repr = [r.serialise() for r in self.reservation_list]

        return dict(
                     type             = str(self.type),
                     size             = int(self.size),
                     scheduled        = bool(self.scheduled),
                     reservation_list = reservation_list_repr
                    )
