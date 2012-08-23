import log
import threading

# Create module log 
logger = log.create_logger("monitor")

class PlainMessageEvent(object):
    def __init__(self, message):
        self.message = message
    def __repr__(self):
        return '%s(%r)' % (self.__class__, self.__dict__)

class _TimerThread(threading.Thread):
    def __init__(self, period, name="Timer Thread"):
        super(_TimerThread, self).__init__(name=name)
        self.period = period
        self.event = threading.Event()
        self.setDaemon(True)

    def run(self):
        logger.info("Starting timer")
        while not self.event.is_set():
            self.action()
            self.event.wait(self.period)
        logger.info("Stopping timer")

    def stop(self):
        self.event.set()

    def action(self):
        raise NotImplementedError("Override in sub-class")

class _MonitoringThread(_TimerThread):
    def __init__(self, period, queue, name="Monitoring Thread"):
        super(_MonitoringThread, self).__init__(period)
        self.queue = queue

    def action(self):
        logger.info("Do periodic action")

        # Do period stuff here

        # Post results to controller
        self.queue.put(PlainMessageEvent("An event with a simple message"))

def create_monitor(period, queue):
    return _MonitoringThread(period, queue)
