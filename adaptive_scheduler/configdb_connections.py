import logging
import json
from collections import defaultdict

import requests

from adaptive_scheduler.utils import SendMetricMixin, case_insensitive_equals, join_location

log = logging.getLogger(__name__)


class ConfigDBError(Exception):
    pass


class ConfigDBInterface(SendMetricMixin):
    """ Class for providing access to information in configdb. Used to replace both the camera_mappings file and
        the telescopes file. It saves/loads a local file of each from disk to use in case configdb is down.
        Proper usage is to call the update_configdb_structures once each scheduling run, then get the loaded
        in data as needed.
    """

    def __init__(self, configdb_url, telescopes_file='/data/adaptive_scheduler/telescopes.json',
                 active_instruments_file='/data/adaptive_scheduler/active_instruments.json'):
        self.configdb_url = configdb_url
        if not self.configdb_url.endswith('/'):
            self.configdb_url += '/'
        self.telescopes_file = telescopes_file
        self.active_instruments_file = active_instruments_file
        self.active_instruments = None
        self.telescope_info = None
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

        if self.active_instruments is None:
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

        if self.telescope_info is None:
            # First time loading from configdb failed so attempt to read from file
            with open(self.telescopes_file, 'r') as telescopes_cache:
                self.telescope_info = json.load(telescopes_cache)
        else:
            # save the active_instruments each update
            with open(self.telescopes_file, 'w') as telescopes_cache:
                json.dump(self.telescope_info, telescopes_cache)

    def get_all_active_instruments(self):
        """Function calls the configdb endpoint to get the list of instruments and their attributes.

        :return: json list of instruments
        """
        try:
            r = requests.get(self.configdb_url + 'instruments/', timeout=120)
        except requests.exceptions.Timeout as te:
            msg = "{}: {}".format(
                te.__class__.__name__, "get_all_active_instruments failed: Timeout connecting to Configdb"
            )
            raise ConfigDBError(msg)
        except requests.exceptions.RequestException as e:
            msg = "{}: {}".format(e.__class__.__name__, "get_all_active_instruments failed: ConfigDB connection down")
            raise ConfigDBError(msg)
        r.encoding = 'UTF-8'
        if not r.status_code == 200:
            raise ConfigDBError("get_all_active_instruments failed: ConfigDB status code {}".format(r.status_code))
        json_results = r.json()
        if 'results' not in json_results:
            raise ConfigDBError("get_all_active_instruments failed: ConfigDB returned no results")
        return json_results['results']

    def get_specific_instrument(self, instrument_type_code, site, enclosure, telescope):
        """Get the specific instrument name.

        Parameters:
            instrument_type: Instrument type
            site: 3-letter site code
            enclosure: 4-letter enclosure code
            telescope: 4-letter telescope code
        Returns:
            The matching specific instrument name
        Raises:
            ConfigDBError: If the specific instrument name is not found
        """
        fallback_instrument = ''
        for instrument in self.active_instruments:
            if instrument['state'] != ['DISABLED']:
                temp_instrument_type = instrument['instrument_type']['code']
                if case_insensitive_equals(instrument_type_code, temp_instrument_type):
                    split_string = instrument['__str__'].lower().split('.')
                    temp_site, temp_observatory, temp_telescope, _ = split_string
                    if (
                            case_insensitive_equals(site, temp_site) and
                            case_insensitive_equals(enclosure, temp_observatory) and
                            case_insensitive_equals(telescope, temp_telescope)
                    ):
                        if instrument['state'] == 'SCHEDULABLE':
                            return instrument['code']
                        else:
                            fallback_instrument = instrument['code']

        if fallback_instrument:
            return fallback_instrument

        raise ConfigDBError(
            'get_specific_instrument failed: unable to find instrument type {} at location {}'
                .format(instrument_type_code, '.'.join([site, enclosure, telescope]))
        )

    def get_autoguider_for_instrument(self, instrument_name, self_guide):
        """Get the autoguider instrument name.

        Parameters:
            instrument_name: Science camera instrument name
            self_guide: Boolean indicating whether to self-guide
        Returns:
             Instrument name to be used for autoguiding
        Raises:
            ConfigDBError: If unable to determine a suitable autoguider
        """
        fallback_instrument = ''
        for instrument in self.active_instruments:
            if instrument['state'] != ['DISABLED']:
                if case_insensitive_equals(instrument_name, instrument['code']):
                    if instrument['state'] == 'SCHEDULABLE':
                        if not self_guide:
                            return instrument['autoguider_camera']['code']
                        elif instrument['instrument_type']['allow_self_guiding']:
                            return instrument['code']
                    else:
                        if not self_guide:
                            fallback_instrument = instrument['autoguider_camera']['code']
                        elif instrument['instrument_type']['allow_self_guiding']:
                            fallback_instrument = instrument['code']

        if fallback_instrument:
            return fallback_instrument

        raise ConfigDBError(
            'get_autoguider_for_instrument failed: unable to find autoguider for instrument {} where self_guide={}'
                .format(instrument_name, self_guide)
        )

    @staticmethod
    def _parse_instrument_string(instrument_string):
        split_string = instrument_string.lower().split('.')
        site, enclosure, telescope, _ = split_string
        return {
            'telescope_location': join_location(site, enclosure, telescope),
            'site': site,
            'enclosure': enclosure,
            'telescope': telescope,
            'telescope_class': telescope[:3]
        }

    @staticmethod
    def _location_available(location_info, location_constraints):
        for constraint in ['telescope_class', 'site', 'enclosure', 'telescope']:
            if (
                    constraint in location_constraints and
                    not case_insensitive_equals(location_info[constraint], location_constraints[constraint])
            ):
                return False
        return True

    @staticmethod
    def _location_fully_set(location_constraints):
        for constraint in ['telescope_class', 'site', 'enclosure', 'telescope']:
            if constraint not in location_constraints or not location_constraints.get(constraint):
                return False
        return True

    @staticmethod
    def _elements_available(elements_by_type, available_element_groups):
        # Assume that the elements_by_type have only valid element types.
        for element_type in elements_by_type:
            available_element_group_types = [eg['type'].lower() for eg in available_element_groups]
            if element_type.lower() not in available_element_group_types:
                return False

            lowercase_elements_of_type = set([e.lower() for e in elements_by_type[element_type]])
            for element_group in available_element_groups:
                if case_insensitive_equals(element_type, element_group['type']):
                    codes = {oe['code'].lower() for oe in element_group['optical_elements'] if oe['schedulable']}
                    if not lowercase_elements_of_type.issubset(codes):
                        return False
                    break
        return True

    def get_telescopes_for_instruments(self, instrument_types_to_requirements, location, is_staff=False):
        """Get the set of telescopes on which a request can be observed.

        The main dictionary passed in contains instrument requirements by instrument type. The requirements include
        the science and guide camera optical elements needed, and whether the observation is planning to self-guide.
        The optical elements sub-structures can contain any number of lists of different elements, keyed by element type

        Parameters:
            instrument_types_to_requirements: dict of Instrument type to corresponding sets of science_optical_elements,
                guiding_optical_elements, and self_guide fields
            location: Dictionary with any location restrictions
        Returns:
            Set of available telescopes
        """
        loc_is_set = self._location_fully_set(location)
        telescope_sets = defaultdict(set)
        for instrument in self.active_instruments:
            if instrument['state'] == 'SCHEDULABLE' or (instrument['state'] != 'DISABLED' and is_staff and loc_is_set):
                instrument_location = self._parse_instrument_string(instrument['__str__'])
                for instrument_type, instrument_requirements in instrument_types_to_requirements.items():
                    if (case_insensitive_equals(instrument_type,
                                                instrument['instrument_type']['code']) and
                            self._location_available(instrument_location, location)):
                        # This instrument is a candidate, now the optical elements just need to match
                        self_guide = instrument_requirements['self_guide']
                        these_imager_element_groups = []
                        for science_camera in instrument['science_cameras']:
                            these_imager_element_groups.extend(science_camera['optical_element_groups'])

                        if self_guide and instrument['instrument_type']['allow_self_guiding']:
                            these_guider_element_groups = these_imager_element_groups
                        elif not self_guide:
                            these_guider_element_groups = instrument['autoguider_camera']['optical_element_groups']
                        else:
                            # There is no available guider on this telescope
                            continue

                        if (
                                self._elements_available(instrument_requirements['science_optical_elements'],
                                                         these_imager_element_groups) and
                                self._elements_available(instrument_requirements['guiding_optical_elements'],
                                                         these_guider_element_groups)
                        ):
                            telescope_sets[instrument_type].add(
                                instrument_location['telescope_location'])

        telescope_sets = list(telescope_sets.values())
        if len(telescope_sets) > 1:
            return telescope_sets[0].intersection(*telescope_sets[1:])
        else:
            return telescope_sets[0] if telescope_sets else set()

    def get_all_sites(self):
        """Function returns the current structure of sites we can use for telescope info"""
        try:
            r = requests.get(self.configdb_url + 'sites/', timeout=120)
        except requests.exceptions.Timeout as te:
            msg = "{}: {}".format(te.__class__.__name__, 'get_all_sites failed: ConfigDB connection timed out')
            raise ConfigDBError(msg)
        except requests.exceptions.RequestException as e:
            msg = "{}: {}".format(e.__class__.__name__, 'get_all_sites failed: ConfigDB connection down')
            raise ConfigDBError(msg)
        r.encoding = 'UTF-8'
        if not r.status_code == 200:
            raise ConfigDBError("get_all_sites failed: ConfigDB status code {}".format(r.status_code))
        json_results = r.json()
        if 'results' not in json_results:
            raise ConfigDBError("get_all_sites failed: ConfigDB returned no results")
        return json_results['results']

    def _generate_telescope_info(self):
        """Generates the structure for telescope_info using the site data from configdb"""
        telescope_info = {}
        site_data = self.get_all_sites()
        for site in site_data:
            for enclosure in site['enclosure_set']:
                for telescope in enclosure['telescope_set']:
                    name = '.'.join([telescope['code'], enclosure['code'], site['code']])
                    active = telescope['active'] and enclosure['active'] and site['active']
                    telescope_info[name] = {
                        'name': name,
                        'tel_class': telescope['code'][:3],
                        'latitude': telescope['lat'],
                        'longitude': telescope['long'],
                        'horizon': telescope['horizon'],
                        'ha_limit_neg': telescope['ha_limit_neg'],
                        'ha_limit_pos': telescope['ha_limit_pos'],
                        'zenith_blind_spot': telescope['zenith_blind_spot'],
                        'events': [],
                        'status': 'online' if active else 'offline'
                    }
        return telescope_info

    def get_telescope_info(self):
        if not self.telescope_info:
            self.telescope_info = self._generate_telescope_info()
        return self.telescope_info
