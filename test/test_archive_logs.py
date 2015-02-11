'''
test_archive_logs.py - Integration test for log archiving of request logs

description

Author: Eric Saunders
February 2015
'''

import tempfile
import os

archive_script = './scripts/archive_logs.py'

my_log_dir     = tempfile.mkdtemp()
my_archive_dir = tempfile.mkdtemp(dir=my_log_dir)

# Create some sample log files to archive
log_files = ('1.dat', '2.dat')

for lf in log_files:
    lf_path = os.path.join(my_log_dir, lf)

    # Create empty log files
    open(lf_path, 'a').close()

    # TODO: Pass arguments
    os.system(archive_script)



# Clean up
os.remove(my_log_dir)
