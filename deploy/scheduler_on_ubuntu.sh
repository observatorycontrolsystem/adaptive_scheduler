#!/bin/bash

# Install the adaptive scheduler on an Ubuntu machine
# Eric Saunders, February 2014

OUT_LOG=$HOME/out.log
rm -f $OUT_LOG

set -e

# Install Fortran, Scipy, Python and MySQL dependencies
sudo apt-get install gfortran
sudo apt-get install libblas3gf libblas-dev liblapack3gf liblapack-dev
sudo apt-get install python-dev
sudo apt-get install mysql-client mysql-server libmysqlclient-dev

# Create a download directory if necessary
cd
if [ ! -d software ]; then
    mkdir software
    echo Creating directory 'software' to download packages >> $OUT_LOG
fi
cd software

# Remove glpk source dir if it exists
if [ -d glpk-4.47 ]; then
    rm -rf glpk-4.47
fi

# Download glpk if necessary
glpk_tarball=glpk-4.47.tar.gz
if [ ! -f $glpk_tarball ]; then
    echo Downloading $glpk_tarball >> $OUT_LOG
    wget http://ftp.gnu.org/gnu/glpk/$glpk_tarball
fi

# Build glpk
tar xvzf $glpk_tarball
cd glpk-4.47
./configure

# Install glpk to system
make
sudo make install
sudo ldconfig

# Create scheduler virtualenv
cd
if [ ! -d v_envs ]; then
    mkdir v_envs
    echo Creating directory 'v_envs' to store virtualenvs >> $OUT_LOG
fi
cd v_envs

if [ ! -d scheduler ]; then
    virtualenv scheduler
    echo Creating virtualenv 'scheduler' >> $OUT_LOG
fi

source scheduler/bin/activate

# Down-rev the Pip version so we can access our local repo
pip install -U pip==1.3.1

# Check out scheduler source code
project_dir=$HOME/projects
as_dir=$project_dir/adaptive_scheduler
cd
if [ ! -d $project_dir ]; then
    mkdir $project_dir
    echo Creating directory '$project_dir' for SVN checkouts >> $OUT_LOG
fi
cd $project_dir

as_svn=http://versionsba/svn/telsoft/Lco/adaptive_scheduler/trunk
if [ ! -d $as_dir ]; then
    svn checkout $as_svn adaptive_scheduler
    echo Checking out adaptive scheduler from $as_svn >> $OUT_LOG
else
    cd $as_dir
    svn update
    echo Updating checked out scheduler source code >> $OUT_LOG
fi

cd $as_dir
pip install -r requirements.pip
echo Installing Python dependencies from requirements.pip >> $OUT_LOG
nosetests test
echo Running unit tests >> $OUT_LOG
echo Installation complete. >> $OUT_LOG
