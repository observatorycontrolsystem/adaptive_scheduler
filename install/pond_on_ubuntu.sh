#!/bin/bash

# Install the POND on an Ubuntu machine
# Eric Saunders, February 2014

OUT_LOG=$HOME/out.log
rm -f $OUT_LOG

set -e

cd
source v_envs/scheduler/bin/activate

project_dir=$HOME/projects
pond_dir=$project_dir/pond
cd $project_dir

pond_svn=http://versionsba/svn/telsoft/Lco/obsdb/branches/release
if [ ! -d $pond_dir ]; then
    svn checkout $pond_svn pond
    echo Checking out pond from $pond_svn >> $OUT_LOG
else
    cd $pond_dir
    svn update
    echo Updating checked out pond source code >> $OUT_LOG
fi

cd $pond_dir
pip install -r requirements.txt
