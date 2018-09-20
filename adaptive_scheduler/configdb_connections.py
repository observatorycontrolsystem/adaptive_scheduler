from adaptive_scheduler.utils import metric_timer, SendMetricMixin, case_insensitive_equals

import logging
import json
import requests


log = logging.getLogger(__name__)


class ConfigDBError(Exception):
    pass


class ConfigDBInterface(object, SendMetricMixin):
    ''' Class for providing access to information in configdb. Used to replace both the camera_mappings file and
        the telescopes file. It saves/loads a local file of each from disk to use in case configdb is down.
        Proper usage is to call the update_configdb_structures once each scheduling run, then get the loaded
        in data as needed.
    '''
    def __init__(self, configdb_url, telescopes_file='/data/adaptive_scheduler/telescopes.json',
                 active_instruments_file='/data/adaptive_scheduler/active_instruments.json'):
        self.configdb_url = configdb_url
        if not self.configdb_url.endswith('/'):
            self.configdb_url += '/'
        self.telescopes_file = telescopes_file
        self.active_instruments_file = active_instruments_file
        self.active_instruments = {}
        self.telescope_info = {}
        self.update_configdb_structures()

    def update_configdb_structures(self):
        self.update_active_instruments()
        self.update_telescope_info()

    def update_active_instruments(self):
        try:
            new_active_instruments = self.get_all_active_instruments()
            self.active_instruments = new_active_instruments
        except ConfigDBError as e:
            log.warning("update_active_instruments error {}. Reusing previous structures".format(repr(e)))

        if not self.active_instruments:
            # First time loading from configdb failed so attempt to read from file
            with open(self.active_instruments_file, 'r') as active_instruments_cache:
                self.active_instruments = json.load(active_instruments_cache)
        else:
            # save the active_instruments each update
            with open(self.active_instruments_file, 'w') as active_instruments_cache:
                json.dump(self.active_instruments, active_instruments_cache)

    def update_telescope_info(self):
        try:
            new_telescope_info = self._generate_telescope_info()
            self.telescope_info = new_telescope_info
        except ConfigDBError as e:
            log.warning("update_telescope_info error {}. Reusing previous structures".format(repr(e)))

        if not self.telescope_info:
            # First time loading from configdb failed so attempt to read from file
            with open(self.telescopes_file, 'r') as telescopes_cache:
                self.telescope_info = json.load(telescopes_cache)
        else:
            # save the active_instruments each update
            with open(self.telescopes_file, 'w') as telescopes_cache:
                json.dump(self.telescope_info, telescopes_cache)

    def get_all_active_instruments(self):
        '''
            Function calls the configdb endpoint to get the list of instruments and their attributes. Dogpile.cache is
            used to cache the dictionary decoded response. This is necessary because json decoding time is large.
        :return: json list of instruments
        '''
        try:
            r = requests.get(self.configdb_url + 'instruments/?state=SCHEDULABLE', timeout=120)
        except requests.exceptions.RequestException as e:
            msg = "{}: {}".format(e.__class__.__name__, "get_all_active_instruments failed: ConfigDB connection down")
            raise ConfigDBError(msg)
        except requests.exceptions.Timeout as te:
            msg = "{}: {}".format(te.__class__.__name__, "get_all_active_instruments failed: Timeout connecting to Configdb")
            raise ConfigDBError(msg)
        r.encoding = 'UTF-8'
        if not r.status_code == 200:
            raise ConfigDBError("get_all_active_instruments failed: ConfigDB status code {}".format(r.status_code))
        json_results = r.json()
        if not 'results' in json_results:
            raise ConfigDBError("get_all_active_instruments failed: ConfigDB returned no results")
        return json_results['results']

    def get_specific_instrument(self, instrument_type, site, observatory, telescope):
        matched_instrument_type = False
        for instrument in self.active_instruments:
            inst_type = instrument['science_camera']['camera_type']['code']
            if case_insensitive_equals(instrument_type, inst_type):
                matched_instrument_type = True
                split_string = instrument['__str__'].lower().split('.')
                temp_site, temp_observatory, temp_telescope, _ = split_string
                if (case_insensitive_equals(site, temp_site) and
                        case_insensitive_equals(observatory, temp_observatory) and
                        case_insensitive_equals(telescope, temp_telescope)):
                    return instrument['science_camera']['code']

        if matched_instrument_type:
            raise ConfigDBError("get_specific_instrument failed: unable to find instrument type {} at location {}"
                                .format(instrument_type, '.'.join([site, observatory, telescope])))
        # If no specific instrument was found, return the instrument_type which might be its specific name
        return instrument_type

    def get_autoguider_for_instrument(self, instrument_name, ag_name=''):
        if case_insensitive_equals(ag_name, instrument_name):
            # always allow self-guiding
            return instrument_name
        for instrument in self.active_instruments:
            if case_insensitive_equals(instrument_name, instrument['science_camera']['code']):
                if not ag_name or case_insensitive_equals(ag_name, instrument['autoguider_camera']['code']):
                    return instrument['autoguider_camera']['code']
                elif case_insensitive_equals(ag_name, instrument['science_camera']['camera_type']['code']):
                    return instrument['science_camera']['code']

        raise ConfigDBError("get_autoguider_for_instrument failed: unable to find autoguider {} for instrument {}"
                            .format(ag_name, instrument_name))

    def get_telescopes_for_instrument(self, instrument_name, filters, location_dict):
        '''Gets the set of telescopes that a request can be observed on given its instrument class, filter set, and
            location info
        '''
        telescopes = set()
        for instrument in self.active_instruments:
            instrument_type = instrument['science_camera']['camera_type']['code']
            if (case_insensitive_equals(instrument_name, instrument['science_camera']['code']) or
                    case_insensitive_equals(instrument_name, instrument_type)):
                camera_filters = {x.lower() for x in instrument['science_camera']['filters'].split(',')}
                if set(filters).issubset(camera_filters):
                    split_string = instrument['__str__'].lower().split('.')
                    site, observatory, telescope, _ = split_string
                    telescope_class = telescope[:3]
                    if ('telescope_class' in location_dict and
                            not case_insensitive_equals(telescope_class, location_dict['telescope_class'])):
                        continue
                    if 'site' in location_dict and not case_insensitive_equals(site, location_dict['site']):
                        continue
                    if ('observatory' in location_dict and
                            not case_insensitive_equals(observatory, location_dict['observatory'])):
                        continue
                    if ('telescope' in location_dict and
                            not case_insensitive_equals(telescope, location_dict['telescope'])):
                        continue
                    # add telescope of the form site.obs.tel to the list of available ones
                    telescopes.add('.'.join(reversed(split_string[:3])))

        return telescopes

    def get_all_sites(self):
        '''
            Function returns the current structure of sites we can use for telescope info
        '''
        try:
            r = requests.get(self.configdb_url + 'sites/', timeout=120)
        except requests.exceptions.RequestException as e:
            msg = "{}: {}".format(e.__class__.__name__, 'get_all_sites failed: ConfigDB connection down')
            raise ConfigDBError(msg)
        except requests.exceptions.Timeout as te:
            msg = "{}: {}".format(te.__class__.__name__, 'get_all_sites failed: ConfigDB connection timed out')
            raise ConfigDBError(msg)
        r.encoding = 'UTF-8'
        if not r.status_code == 200:
            raise ConfigDBError("get_all_sites failed: ConfigDB status code {}".format(r.status_code))
        json_results = r.json()
        if 'results' not in json_results:
            raise ConfigDBError("get_all_sites failed: ConfigDB returned no results")
        return json_results['results']

    def _generate_telescope_info(self):
        '''Generates the structure for telescope_info using the site data from configdb'''
        telescope_info = {}
        site_data = self.get_all_sites()
        for site in site_data:
            for enclosure in site['enclosure_set']:
                for telescope in enclosure['telescope_set']:
                    name = '.'.join([telescope['code'], enclosure['code'], site['code']])
                    active = telescope['active'] and enclosure['active'] and site['active']
                    telescope_info[name] = {'name': name,
                                            'tel_class': telescope['code'][:3],
                                            'latitude': telescope['lat'],
                                            'longitude': telescope['long'],
                                            'horizon': telescope['horizon'],
                                            'ha_limit_neg': telescope['ha_limit_neg'],
                                            'ha_limit_pos': telescope['ha_limit_pos'],
                                            'events': [],
                                            'status': 'online' if active else 'offline'}

        return telescope_info

    def get_telescope_info(self):
        if not self.telescope_info:
            self.telescope_info = self._generate_telescope_info()

        return self.telescope_info



