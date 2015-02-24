'''
test_archive_logs.py - Integration test for log archiving of request logs

description

Author: Eric Saunders
February 2015
'''

from __future__ import division

from nose.tools import assert_equal
import mock
import tempfile
import os
import shutil
from collections import namedtuple
from datetime import datetime
import gzip

from adaptive_scheduler.scripts import utils

Configuration = namedtuple('Configuration',
                          ['mtime_days', 'log_dir', 'archive_log_dir'])





class TestArchiveLogs(object):

    def setup(self):
        self.my_log_dir     = tempfile.mkdtemp()
        self.my_archive_dir = tempfile.mkdtemp(dir=self.my_log_dir)
        self.log_files      = None

        self._create_test_files(
                                 (
                                  ('1.dat', datetime(2015, 1, 1)),
                                  ('2.dat', datetime(2015, 1, 2)),
                                 )
                               )


    def teardown(self):
        #shutil.rmtree(self.my_log_dir)
        pass


    def _create_test_files(self, files):
        # Create some sample log files to archive

        self.log_files = []
        for name, mtime in files:
            self.log_files.append((os.path.join(self.my_log_dir, name), mtime))

        for lf_path, _ in self.log_files:
            # Create empty log files
            with open(lf_path, 'a') as lf_fh:
                lf_fh.write(lf_path + '\n')

        return


    def _fake_lsdir_mtime_sorted(self, log_dir):
        return self.log_files


    def _assert_files_exist(self, dirname, files):
        for f in files:
            full_f_path = os.path.join(dirname, f)
            assert os.path.exists(full_f_path), 'Expected %s to exist' % full_f_path


    def _assert_files_dont_exist(self, dirname, files):
        for f in files:
            full_f_path = os.path.join(dirname, f)
            assert not os.path.exists(full_f_path), 'Expected %s to not exist' % full_f_path


    def test_do_archiving_old_files_archived(self):
        utils.lsdir_mtime_sorted = self._fake_lsdir_mtime_sorted

        args = Configuration(
                              mtime_days=1,
                              log_dir=self.my_log_dir,
                              archive_log_dir=self.my_archive_dir,
                            )

        self._assert_files_exist(self.my_log_dir, ('1.dat', '2.dat'))
        self._assert_files_dont_exist(self.my_archive_dir, ('1.dat.gz', '2.dat.gz'))
        utils.do_archiving(args)
        self._assert_files_dont_exist(self.my_log_dir, ('1.dat', '2.dat'))
        self._assert_files_exist(self.my_archive_dir, ('1.dat.gz', '2.dat.gz'))


    def test_do_archiving_recent_files_not_archived(self):
        utils.lsdir_mtime_sorted = self._fake_lsdir_mtime_sorted

        args = Configuration(
                              mtime_days=10,
                              log_dir=self.my_log_dir,
                              archive_log_dir=self.my_archive_dir,
                            )

        self._assert_files_exist(self.my_log_dir, ('1.dat', '2.dat'))
        self._assert_files_dont_exist(self.my_archive_dir, ('1.dat.gz', '2.dat.gz'))
        utils.do_archiving(args, now=datetime(2015,1,3))
        self._assert_files_exist(self.my_log_dir, ('1.dat', '2.dat'))
        self._assert_files_dont_exist(self.my_archive_dir, ('1.dat.gz', '2.dat.gz'))


    def test_do_archiving_only_some_files_archived(self):
        utils.lsdir_mtime_sorted = self._fake_lsdir_mtime_sorted

        args = Configuration(
                              mtime_days=1.5,
                              log_dir=self.my_log_dir,
                              archive_log_dir=self.my_archive_dir,
                            )

        self._assert_files_exist(self.my_log_dir, ('1.dat', '2.dat'))
        self._assert_files_dont_exist(self.my_archive_dir, ('1.dat.gz', '2.dat.gz'))
        utils.do_archiving(args, now=datetime(2015,1,3))
        self._assert_files_exist(self.my_archive_dir, ('1.dat.gz',))
        self._assert_files_exist(self.my_log_dir, ('2.dat',))
        self._assert_files_dont_exist(self.my_log_dir, ('1.dat',))
        self._assert_files_dont_exist(self.my_archive_dir, ('2.dat.gz',))


    def test_concatenating_to_existing_archive_file(self):
        utils.lsdir_mtime_sorted = self._fake_lsdir_mtime_sorted
        args = Configuration(
                              mtime_days=1.5,
                              log_dir=self.my_log_dir,
                              archive_log_dir=self.my_archive_dir,
                            )

        utils.do_archiving(args, now=datetime(2015,1,3))
        self._create_test_files(
                                 (
                                  ('1.dat', datetime(2015, 1, 3)),
                                 )
                               )
        utils.do_archiving(args, now=datetime(2015,1,5))
        with gzip.open(os.path.join(self.my_archive_dir, '1.dat.gz'), 'r') as gzip_fh:
                contents = gzip_fh.readlines()
        gzip_n_lines = len(contents)
        assert_equal(gzip_n_lines, 2)
