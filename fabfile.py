#!/usr/bin/env python

'''
fabfile.py - Build and deployment for the adaptive scheduler.

description

Author: Eric Saunders
November 2013
'''

import os

def virtualenv_is(name=None):
    if not 'VIRTUAL_ENV' in os.environ:
        return False

    if os.environ['VIRTUAL_ENV'] == name:
        return True

    return False




'''
# Target 1 (example): build and deploy rise_set from trunk
# -----------------------------------------------
# Checkout the code from version control.
# If the virtualenv exists, move it to a .old location so we can build fresh
# Create the new virtualenv.
# Activate the virtualenv.
# cd to the location of the requirements file.
# Run pip on the requirements file (make sure upgrades happen).
# Run the unit tests.
# Run setup.py install.
'''


'''
# Target 2: Uprev an existing install
# -----------------------------------
# Are we in the right virtualenv?
# If not, activate the virtualenv. Complain if it doesn't exist.
# cd to the source dir.
# Stop any running instance.
# Record the current svn position.
# Checkout the new code.
# Run the pip requirments file.
# Run the unit tests.
# If any tests fail
#   revert to previous checkout.
#   run the old pip requirements file.
# Run the unit tests again to confirm all ok.
# Restart the running instance.
'''


'''
# Target 3: build and deploy the scheduler from trunk
# Steps kike the first example. Note that
# * We depend on scipy, which requires some C libraries to be present
# * We depend on cvxopt/openopt, which are in our local Pypi, but need
# tweaking to be installable under Pip
# * One of these modules has a config file that needs GLPK = True to be set
# GLPK needs to be built from source. It needs to be the correct version (glpk 4.47).
# The library produced by GLPK must be visible on the library path.
# See also INSTALL in adaptive_scheduler top-level dir.
# Extra step: Put the init.d from scheduler-dev into SVN. Deploy the init.d.
