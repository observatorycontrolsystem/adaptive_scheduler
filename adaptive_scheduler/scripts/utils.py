#!/usr/bin/env python

'''
utils.py - Utility functions for miscellaneous scripts

This module contains utility functions used for adaptive scheduler scripting tasks.
Entry points:
    * do_archiving(mtime_days, log_dir, archive_log_dir) - Archive scheduler User
                                                           Request logs

Author: Eric Saunders
February 2015
'''

import os
import gzip
from datetime import datetime, timedelta


def lsdir_mtime_sorted(search_dir):
    ''' Return a list of (filename, mtime) tuples, sorted by ascending mtime,
        representing the files present in search_dir. Sub-directories are ignored.'''

    filenames = [f for f in os.listdir(search_dir)]
    filenames = [os.path.join(search_dir, f) for f in filenames]
    filenames = [f for f in filenames if os.path.isfile(f)]
    filenames.sort(key=os.path.getmtime)
    mtimes = [datetime.utcfromtimestamp(os.path.getmtime(f)) for f in filenames]

    files = zip(filenames, mtimes)

    return files



def zip_file(source, dest):
    ''' Replace the source file with a zipped version of that file.'''

    with open(source, 'r') as source_fh:
        with gzip.open(dest, 'w') as dest_fh:
            dest_fh.writelines(source_fh)
    os.remove(source)

    return



def append_to_archive(log_file_path, archive_file_path):
    '''Unzip archived_filename, concatenate filename to it, and then zip it
       again.'''

    orig_archive_file    = archive_file_path
    tmp_archive_filename = orig_archive_file + '.tmp'

    with gzip.open(archive_file_path, 'r') as archive_fh:
        with gzip.open(tmp_archive_filename, 'w') as new_archive_fh:
            new_archive_fh.writelines(archive_fh)

            existing_unzipped_file = log_file_path
            with open(existing_unzipped_file, 'r') as unzipped_fh:
                new_archive_fh.writelines(unzipped_fh)

    os.rename(tmp_archive_filename, orig_archive_file)
    os.remove(existing_unzipped_file)

    return



def compress_old_file(archive_log_dir, log_file_path):
    '''Compress the selected log file and store in archive_log_dir.'''

    log_file_name          = os.path.basename(log_file_path)
    archive_file_path = os.path.join(archive_log_dir, log_file_name) + '.gz'

    # If we've never archived this file before (normal expected behaviour)...
    if not os.path.isfile(archive_file_path):
        # Compress and store the file
        zip_file(log_file_path, archive_file_path)

    # Otherwise, somehow we already have this archived...
    else:
        append_to_archive(log_file_path, archive_file_path)

    # Remove the original
    print "Archived: %s -> %s" % (log_file_path, archive_file_path)



def do_archiving(mtime_days, log_dir, archive_log_dir, now=None):
    ''' Entry point for archiving scheduler User Request logs.'''

    now = now if now else datetime.utcnow()

    TOO_OLD_DT = now - timedelta(days=mtime_days)

    # Get the contents of the log directory, sorted by mtime
    files = lsdir_mtime_sorted(log_dir)

    # Create archive dir if necessary
    if not os.path.exists(archive_log_dir):
        os.mkdir(archive_log_dir)

    for log_file_path, mtime in files:

        # If the log file is ready for archiving...
        if mtime < TOO_OLD_DT:
            compress_old_file(archive_log_dir, log_file_path)

        # Files are sorted by modification time, so no other files will need archiving
        else:
            break
