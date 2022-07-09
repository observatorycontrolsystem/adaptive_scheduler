'''
test_eventbus.py - Unit tests for the eventbus.py module.

Author: Martin Norbury
October 2013
'''

from adaptive_scheduler import eventbus

import gc


class FakeListener1(eventbus.BaseListener):
    class _Event(eventbus.Event):
        def __init__(self, number):
            self.number = number

        def dispatch(self, listener):
            listener.on_number_update(self.number)

        def __repr__(self):
            return '%s<%r>' % (self.__class__, self.__dict__)

    def __init__(self):
        self.last_update = None

    @classmethod
    def create_event(cls, number):
        return cls._Event(number)

    def on_number_update(self, number):
        self.last_update = number


# Module level test variables
event_type = FakeListener1.event_type()
listener = FakeListener1()


class TestEventBus(object):

    def setup(self):
        self.eventbus = eventbus._EventBus()

    def test_returns_same_eventbus(self):
        eventbus1 = eventbus.get_eventbus()
        eventbus2 = eventbus.get_eventbus()

        assert eventbus1 is eventbus2

    def test_adding_listener(self):
        self.eventbus.add_listener(listener)

        assert self.eventbus.number_of_listeners(event_type) == 1

    def test_removing_listener(self):
        self.eventbus.add_listener(listener)
        self.eventbus.remove_listener(listener)

        assert self.eventbus.number_of_listeners(event_type) ==  0

    def test_listener_removed_after_garbage_collection(self):
        listener1 = FakeListener1()
        event = FakeListener1.create_event(10)

        self.eventbus.add_listener(listener1)

        # Force the listener to be garbage collected
        del (listener1)
        gc.collect()
        self.eventbus.fire_event(event)

        assert self.eventbus.number_of_listeners(event_type) ==  0

    def test_listener_not_removed_after_garbage_collection_if_persistent(self):
        listener1 = FakeListener1()
        event = FakeListener1.create_event(10)

        self.eventbus.add_listener(listener1, persist=True)

        # Force the listener to be garbage collected
        del (listener1)
        gc.collect()
        self.eventbus.fire_event(event)

        assert self.eventbus.number_of_listeners(event_type) == 1

    def test_adding_multiple_listeners(self):
        listener1 = FakeListener1()
        listener2 = FakeListener1()

        self.eventbus.add_listener(listener1)
        self.eventbus.add_listener(listener2)

        assert self.eventbus.number_of_listeners(event_type) == 2

    def test_firing_an_event(self):
        event = FakeListener1.create_event(10)

        self.eventbus.add_listener(listener)
        self.eventbus.fire_event(event)

        assert listener.last_update == 10
