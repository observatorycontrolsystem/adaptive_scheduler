'''
camera_mapping.py - Module for reading in LCOGT camera attribute information.

This module reads in camera attribute information (e.g. available binnings) and
provides an interface for querying by a variety of different criteria. The primary
use is to resolve a generic camera type (e.g. SCICAM) to the specific science
camera at a given resource, so that a valid POND block may be constructed.

See camera_mappings.dat for an example of the input data.

Authors:
    Martin Norbury (mnorbury@lcogt.net)
    Eric Saunders

November 2012

'''
from collections import defaultdict
from datetime    import datetime
import sys
import shutil
import urllib2


def update_mapping():
    MAPPING_FILENAME  = 'camera_mappings.dat'
    CONFIGDB_ENDPOINT = 'http://lcogt.net/configdb/camera_mappings/'
    content = get_http_data(CONFIGDB_ENDPOINT)
    replace_mapping_file(MAPPING_FILENAME, content)

    return


def replace_mapping_file(mapping_filename, content):
    src = mapping_filename
    dst = mapping_filename + '.old'
    shutil.copyfile(src, dst)

    out_fh = open(mapping_filename, 'w')
    out_fh.write(content)
    out_fh.close()

    return


def get_http_data(url):

    try:
        response = urllib2.urlopen(url)
    except urllib2.URLError as e:
        msg = "Received an error from server: %s" % str(e.reason)
        msg += "\nYour url may be wrong, or the endpoint"
        msg += " at '%s' may be down." % url
        raise ConnectionError(msg)

    except ValueError as e:
        msg = str(e)
        msg += "\nThe url you provided ('%s') is garbled or missing." % url
        raise ConnectionError(msg)

    # Catch-all for rare other errors allowed to bleed through by urllib2
    except Exception as e:
        msg += "Unexpected error:", sys.exc_info()[0]
        raise ConnectionError(msg)

    try:
        response_data = response.read()
        response.close()
    except urllib2.URLError as e:
        msg = "Received an error code from server: %s" % str(e.reason)
        raise ConnectionError(msg)

    # Catch-all for rare other errors allowed to bleed through by urllib2
    except Exception as e:
        msg += "Unexpected error:", sys.exc_info()[0]
        raise ConnectionError(msg)

    return response_data


def create_camera_mapping(resource):
    ''' Factory method for creating camera mappings.

        resource - Either a file stream or filename.
    '''
    try:
        stream = open(resource)
    except TypeError:
        stream = resource
    with stream as fsock:
        return _CameraMapping(fsock.readlines())


class _CameraMapping(object):
    def __init__(self, lines):
        self._last_modified = self._parse_last_modified(lines[0])
        self._headings = self._parse_headings(lines[1])
        self._data = [self._parse_line(line) for line in lines[2:]]

    def _parse_last_modified(self, last_modified_line):
        # Expecting this:# Last modified: 2012-11-30 23:18:24
        return datetime.strptime(last_modified_line.strip(),
                                 '# Last modified: %Y-%m-%d %H:%M:%S')

    def _parse_headings(self, heading_line):
        results = heading_line.strip('# ').split()
        transform = self._transform_heading
        return [transform(heading) for heading in results]

    def _transform_heading(self, heading):
        if '(' in heading:
            heading = heading.split('(')[0]
        return _convert_camel_case(heading)

    def _parse_line(self, line):
        split_line = line.split()
        datum = {}
        column_parser = _create_column_parser_mapping()
        for index, header in enumerate(self._headings):
            datum[header] = column_parser[header](index, split_line)
        return datum

    def _base_find(self, custom_filter):
        return [datum for datum in self._data if custom_filter(datum)]

    def find_by_location(self, site, observatory, telescope):
        filter_by_location = lambda x: x['site'] == site and\
                                       x['observatory'] == observatory and\
                                       x['telescope'] == telescope
        return self._base_find(filter_by_location)

    def find_by_autoguider(self, autoguider):
        filter_by_autoguider = lambda x: x['autoguider'] == autoguider
        return self._base_find(filter_by_autoguider)

    def find_by_autoguider_type(self, autoguider_type):
        filter_by_autoguider_type = lambda x: x['autoguider_type'] == autoguider_type
        return self._base_find(filter_by_autoguider_type)

    def find_by_camera(self, camera):
        filter_by_camera = lambda x: x['camera'] == camera
        return self._base_find(filter_by_camera)

    def find_by_camera_type(self, camera):
        filter_by_camera_type = lambda x: x['camera_type'].lower() == camera.lower()
        return self._base_find(filter_by_camera_type)

    def find_by_camera_type_and_location(self, site, observatory, telescope, camera):
        filter_by_camera_type = lambda x: x['camera_type'].lower() == camera.lower() and\
                                          x['site'] == site and\
                                          x['observatory'] == observatory and\
                                          x['telescope'] == telescope
        return self._base_find(filter_by_camera_type)


def _create_column_parser_mapping():
    '''
    Create the column parser mappings.

    Each column is parsed either by a default or
    custom parser.
    '''
    def default_parse_column(index, columns):
        ''' Return column for given index.'''
        return columns[index]

    def parse_comma_seperated(index, columns):
        ''' Return a comma delimited list for column.'''
        return columns[index].split(',')

    column_parser = defaultdict(lambda: default_parse_column)
    column_parser['filters'] = parse_comma_seperated
    column_parser['binning_available'] = parse_comma_seperated

    return column_parser


def _convert_camel_case(input_value):
    ''' Convert camel case to underscore format e.g.

    BinningsAvailable -> binnings_available
    '''

    # First locate capital letters e.g.
    # BinningsAvailable would become _Binnings_Available
    first_pass = ''
    for letter in input_value.replace('.', ''):
        if letter.isupper():
            letter = '_' + letter
        first_pass += letter

    # Second pass to preserve joined capitals e.g.
    # AGType needs to be ag_type
    output_value = ''
    for word in first_pass.split('_'):
        if len(word) > 1:
            word = '_' + word
        output_value += word

    return output_value.strip('_ ').lower()


class ConnectionError(Exception):
    '''Wrapper for URL/HTTP transport or server errors.'''

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


if __name__ == '__main__':
    # Update the camera mapping file
    update_mapping()

    # Example usage of this module

    site        = 'bpl'
    observatory = 'aqwa'
    telescope   = '0m4a'
    camera      = 'tn22'
    camera_type = '0m4-SCICAM'

    mapping = create_camera_mapping("camera_mappings.dat")


    # Make some searches
    results = mapping.find_by_location(site, observatory, telescope)
    message = 'Found {0} results by location {1}-{2}-{3}'
    message_args = len(results), site, observatory, telescope
    print message.format(*message_args)
    print
    print results
    print

    results = mapping.find_by_camera(camera)
    print 'Found {0} results by camera {1}'.format(len(results), camera)
    print
    print results
    print

    results = mapping.find_by_camera_type_and_location(site, observatory,
                                                       telescope, camera_type)
    print 'Found {0} results by camera_type {1}'.format(len(results), camera_type)
    print
    for cam in results:
        print cam['camera']
    print
