#!/bin/bash
# Set the paths to development versions of needed Python projects
# Eric Saunders, May 2012

export PYTHONPATH=$HOME/projects/requestdb/shared/src
export PYTHONPATH=$HOME/programming/python/rise_set:$PYTHONPATH
export PYTHONPATH=$HOME/projects:$PYTHONPATH

echo "PYTHONPATH set to:" $PYTHONPATH
