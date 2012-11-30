import log
import threading

from adaptive_scheduler.orchestrator import get_requests_from_json as get_requests

# Create module log 
logger = log.create_logger("monitor")

class RequestUpdateEvent(object):
    def __init__(self, requests):
        self.requests = requests
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
        logger.info("Getting latest requests")

        # Do periodic stuff here
        requests = get_requests('requests.json','dummy arg')

        # Post results to controller
        self.queue.put(RequestUpdateEvent(requests))

def create_monitor(period, queue):
    return _MonitoringThread(period, queue)
