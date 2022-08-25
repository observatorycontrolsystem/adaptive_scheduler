import abc

from adaptive_scheduler.monitoring.opensearch_seeing import get_seeing



class SeeingMonitor(object):
    '''Abstract class for updating and retrieving current seeing value on a resource. Resources are meant to be updated once at the
       beginning of each scheduling run, and the retrieved as needed throughout that run. The structure of seeing data should look like this:

       seeing_by_resource = {
         'telescope.enclosure.site': {
            'time': datetime object,
            'seeing': value in arcseconds
         }
       }
    '''

    __metaclass__ = abc.ABCMeta

    def __init__(self, seeing_valid_time_period, configdb_interface):
        self.seeing_valid_time_period = seeing_valid_time_period
        self.configdb_interface = configdb_interface
        self.current_seeing_by_resource = {}

    def get_resources(self):
        ''' Retrieve list of resource names from configdb, in the format of telescope.enclosure.site. 
        '''
        return self.configdb_interface.get_telescope_info().keys()

    def retrieve_data(self):
        ''' Retrieve seeing values. The output of calling retrieve() will be a dictionary of resource names to seeing values.
            If a resource doesn't have seeing data, then it should not appear in the dictionary.
        '''
        return self.current_seeing_by_resource

    @abc.abstractmethod
    def update_data(self):
        ''' Update seeing values. Update() will populate the internal dictionary of resource names to seeing values.
            If a resource doesn't have seeing data, then it should not appear in the dictionary. Returns True if seeing
            values have changed since last run, and returns False otherwise.
        '''
        pass



class DummySeeingMonitor(SeeingMonitor):
    def __init__(self):
        super().__init__(None, None)

    def update_data(self):
        # This is a dummy monitor so it always returns that there was no change in seeing.
        return False


class OpenSearchSeeingMonitor(SeeingMonitor):
    def __init__(self, seeing_valid_time_period, configdb_interface, opensearch_url, os_index, os_excluded_observatories):
        super().__init__(seeing_valid_time_period, configdb_interface)
        self.opensearch_url = opensearch_url
        self.os_index = os_index
        self.os_excluded_observatories = os_excluded_observatories

    def update_data(self):
        previous_seeing_by_resource = self.current_seeing_by_resource
        self.current_seeing_by_resource = get_seeing(self.get_resources(), self.opensearch_url, self.os_index, self.os_excluded_observatories, self.seeing_valid_time_period)
        if previous_seeing_by_resource == self.current_seeing_by_resource:
            return False
        return True
