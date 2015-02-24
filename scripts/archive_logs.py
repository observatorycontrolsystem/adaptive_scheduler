#!/usr/bin/env python

'''
archive_logs.py - Compress log files that aren't being updated anymore

description

Author: Eric Saunders
January 2015
'''

import argparse
import os
from shutil   import copyfile
from datetime import datetime, timedelta
from zipfile  import ZipFile
import sys

sys.path.append(os.path.normpath(os.path.dirname(os.path.abspath(__file__)) + '/..'))
from adaptive_scheduler.scripts.utils import do_archiving


def parse_args(argv):
    arg_parser = argparse.ArgumentParser(
                            formatter_class=argparse.RawDescriptionHelpFormatter,
                            description=__doc__)
    arg_parser.add_argument('-m', '--mtime', type=float, default=60,
          dest='mtime_days', help='archive files older than this (units=days)')
    arg_parser.add_argument('-l', '--log_dir', type=str, default='./logs',
          dest='log_dir', help='directory containing logs to archive')
    arg_parser.add_argument('-a', '--archive_dir', type=str, default='./logs_archive',
          dest='archive_log_dir', help='directory to store archived_logs')

    # Handle command line arguments
    args = arg_parser.parse_args(argv)

    return args


if __name__ == '__main__':
    args = parse_args(sys.argv[1:])
    do_archiving(args)
