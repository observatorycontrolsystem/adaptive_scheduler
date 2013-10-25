'''
eventbus.py - An event bus implementation.

This module provides an event bus implementation to allow application-level
events to be shared. The event bus is single instance and is accessed 
via the get_eventbus method.

Author: Martin Norbury
October 2013
'''

import abc
import weakref


# Event bus instance
__EVENTBUS_BY_NAME = {}

def get_eventbus(name='default'):
    ''' Return an event bus instance. 

        Retrieving the event bus via this method ensures only one instance
        is used throughout the application.
    '''
    return __EVENTBUS_BY_NAME.setdefault(name, _EventBus())


class BaseListener(object):
    ''' Main listener superclass. 

        User-defined listeners should subclass BaseListener, define an _Event
        inner-class and provide an event update API.
    '''
    __metaclass__ = abc.ABCMeta

    @classmethod
    def event_type(cls):
        ''' Return the event type. '''
        return cls._Event

    def is_update_required(self, last_event, event):
        ''' Return True if an update is required. 

            Subclasses may override this method to control update conditions.
        '''
        return True

class OnChangeListener(BaseListener):
    ''' On change event listener.

        A listener implementation that will only call the update method if the
        latest event is different from the previous event.
    '''

    def is_update_required(self, last_event, event):
        return last_event is not event

class Event(object):
    ''' Event superclass.

        All listeners should have an inner-class _Event which extends this
        class. 
    '''
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def dispatch(self, listener):
        ''' The dispatch behaviour.

            This method is defined in the implementing class where it
            can call the event-specific update method.
        '''
        pass

class _EventBus(object):
    ''' EventBus implementation. 

        This class allows listeners to be registered for specific events.
        All listeners are stored as weak references to allow them to be pruned
        when they fall out of the main application scope.
    '''

    def __init__(self):
        self.listeners_by_type = {}
        self.last_events = {}

    def add_listener(self, listener, persist=False, event_type=None):
        ''' Add a listener to the event bus. 

            The event type this listener is registered against is determined
            via the listener_event_type classmethod.

            By default listeners are stored as weak references to prevent the
            event bus holding on to instances which are eligible for garbage
            collection. If persist is True then the reference is stored as a 
            regular strong reference.
        '''
        listener_event_type = event_type or listener.__class__.event_type()

        listeners = self.listeners_by_type.setdefault(listener_event_type, [])

        storage_function = weakref.ref
        if persist:
            storage_function = _strongref

        listeners.append(storage_function(listener))

    def remove_listener(self, listener):
        ''' Remove a listener from the event bus.

            Loops through the collection of listeners and removes any that
            match the instance to be removed.
        '''
        listener_event_type = listener.__class__.event_type()

        listeners = self.listeners_by_type.get(listener_event_type, [])

        pruned_listeners = [x for x in listeners if x() is not listener]

        self.listeners_by_type[listener_event_type] = pruned_listeners

    def number_of_listeners(self, event_type):
        ''' Return the number of listeners for a given event type.

            This method first prunes the listener collection.
        '''

        self._prune_dead_listeners(event_type)

        return len(self.listeners_by_type.get(event_type, []))

    def fire_event(self, event):
        ''' Fire an event to all registered listeners. 

            This method updates all registered listeners and then prunes
            any dead weak references.
        '''

        # Get listeners_by_type for this event
        listeners = self.listeners_by_type.get(event.__class__, [])

        has_dead_listeners = self._update_listeners(event, listeners)

        # Clear dead listeners
        if has_dead_listeners:
            self._prune_dead_listeners(event.__class__)

        # Store event
        self.last_events[event.__class__] = event

    def _update_listeners(self, event, listeners):
        ''' Update listeners. 

            Return True if we have dead references i.e. eligible for pruning.
        '''
        last_event = self.last_events.get(event.__class__, event)

        has_dead_listeners = False
        for listener_reference in listeners:
            listener = listener_reference()
            if listener:
                if listener.is_update_required(last_event, event):
                    event.dispatch(listener)
            else:
                has_dead_listeners = True
        
        return has_dead_listeners

    def _prune_dead_listeners(self, event_type):
        ''' Remove weak references that have been garbage collected. '''

        listeners = self.listeners_by_type.get(event_type, [])

        live_listeners = [x for x in listeners if x()]

        self.listeners_by_type[event_type] = live_listeners

        return

def _strongref(x):
    ''' A simple closure to mimic the weakref.ref functionality. '''
    def wrapper():
        return x
    return wrapper

if __name__ == '__main__':
    # Example usage.

    class MyListener(BaseListener):
        ''' Example listener. '''
        class _Event(Event):
            ''' Simple event. '''

            def __init__(self, message):
                ''' Message to broadcast. '''
                self.message = message

            def dispatch(self, listener):
                ''' Send message to listeners. '''
                listener.on_message_update(self.message)

        @classmethod
        def create_event(cls, message):
            return cls._Event(message)

        def on_message_update(self, message):
            print message

    # Create eventbus
    eventbus = get_eventbus()

    # Create listener
    listener = MyListener()

    # Register listener
    eventbus.add_listener(listener)

    # Send event
    eventbus.fire_event(MyListener.create_event("Hello World!"))
