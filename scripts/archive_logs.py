#!/usr/bin/env python

'''
archive_logs.py - Compress log files that aren't being updated anymore

description

Author: Eric Saunders
January 2015
'''

import os
from shutil import copyfile
from datetime import datetime, timedelta
from zipfile import ZipFile

TOO_OLD_DT = datetime.utcnow() - timedelta(days=60)
#TOO_OLD_DT = datetime.utcnow() - timedelta(seconds=1)

log_dir = './logs'
archive_log_dir = './logs_archive'


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


def concatenate_files(output_file, *in_files):
    '''Append the contents of in_files into a single output_file.'''

    # If the caller is concatenating in-place, we have to handle this
    if output_file in in_files:
        orig_output_file = output_file
        output_file = output_file + '.tmp'
        is_temp = True

    # Build up a concatenated file
    with open(output_file, 'w') as out_fh:
        for filename in in_files:
            with open(filename) as in_fh:
                for line in in_fh:
                    out_fh.write(line)

    # Complete the in-place concatenation
    if is_temp:
        os.rename(output_file, orig_output_file)

    return


def zip_file(source, archived_filename):

    # Zip files from the current working directory, otherwise you get directories in
    # your zip archive!
    temp_moved_filename = os.path.join('.', os.path.basename(source))
    copyfile(source, temp_moved_filename)

    # Compress the file
    with ZipFile(archived_filename, 'w') as zipped_file:
        zipped_file.write(temp_moved_filename)
    os.remove(temp_moved_filename)

    return



if __name__ == '__main__':

    # Get the contents of the log directory, sorted by mtime
    files = lsdir_mtime_sorted(log_dir)

    # Create archive dir if necessary
    if not os.path.exists(archive_log_dir):
        os.mkdir(archive_log_dir)

    for full_path, mtime in files:
        filename = os.path.basename(full_path)

        # If the log file is ready for archiving...
        if mtime < TOO_OLD_DT:
            archived_filename = os.path.join(archive_log_dir, filename) + '.gz'

            # If we've never archived this file before (normal expected behaviour)...
            if not os.path.isfile(archived_filename):
                # Compress and store the file
                zip_file(full_path, archived_filename)

            # Otherwise, somehow we already have this archived...
            else:
                # Unzip the existing archive...
                with ZipFile(archived_filename, 'r') as zipped_file:
                    zipped_file.extractall(path=archive_log_dir)

                # Concatenate the log file to the existing previously archived file
                existing_unzipped_file = os.path.join(archive_log_dir, filename)
                concatenate_files(existing_unzipped_file, existing_unzipped_file, full_path)

                # Compress and store the file
                zip_file(existing_unzipped_file, archived_filename)
                os.remove(existing_unzipped_file)

            # Remove the original
            os.remove(full_path)
            print "Archived: %s -> %s" % (full_path, archived_filename)

        # Files are sorted by modification time, so no other files will need archiving
        else:
            break
