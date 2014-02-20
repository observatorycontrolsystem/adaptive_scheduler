#!/bin/bash

# Install the RequestDB on an Ubuntu machine
# Eric Saunders, February 2014

OUT_LOG=$HOME/out.log
rm -f $OUT_LOG

set -e

sudo apt-get install sqlite3 libsqlite3-dev

cd
source v_envs/scheduler/bin/activate

project_dir=$HOME/projects
reqdb_dir=$project_dir/requestdb
cd $project_dir

reqdb_svn=http://versionsba/svn/user/mbecker/scheduler/service/trunk/
if [ ! -d $reqdb_dir ]; then
    svn checkout $reqdb_svn requestdb
    echo Checking out requestdb from $reqdb_svn >> $OUT_LOG
else
    cd $reqdb_dir
    svn update
    echo Updating checked out requestdb source code >> $OUT_LOG
fi

cd $reqdb_dir/requestdb/server
pip install -r requirements.txt


# Create the databases
mysql_call='mysql -u root -ptootyrooty '
${mysql_call} -e 'create database scheduler_requests;'
${mysql_call} -e 'create database rbauth;'
${mysql_call} -e 'create database proposaldb;'
