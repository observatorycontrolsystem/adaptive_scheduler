FROM centos:centos7
MAINTAINER Jon Nation <jnation@lco.global>

# setup the python environment
ENV APPLICATION_ROOT /lco/adaptive_scheduler

# set up env variables for gurobi
ENV GUROBI_HOME /lco/adaptive_scheduler/gurobi
ENV PATH ${PATH}:${GUROBI_HOME}/bin
ENV LD_LIBRARY_PATH ${LD_LIBRARY_PATH}:${GUROBI_HOME}/lib

# install and update packages
RUN yum -y install epel-release \
        && yum -y install gcc glpk-devel fftw-devel gsl-devel suitesparse-devel openblas-devel lapack-devel blas-devel python-devel python-pip mysql-devel mysql supervisor\
        && yum -y update \
        && yum -y clean all

# install python libs (and set cvxopt to install glpk)
COPY requirements.pip $APPLICATION_ROOT/requirements.pip
RUN pip install numpy && CVXOPT_BUILD_GLPK=1 pip --trusted-host buildsba.lco.gtn install -r $APPLICATION_ROOT/requirements.pip

# copy the stuff
COPY . $APPLICATION_ROOT

### BONUS STEP: copy the correct UTF-32 gurobipy library over the one that was pip installed
### This is necessary because the buildsba pypi apparently is UTF16, so it chooses the wrong version
### of the lib to install when it installs gurobi.
RUN cp $GUROBI_HOME/lib/gurobipy.so /usr/lib/python2.7/site-packages/gurobipy/

# create eng user necessary to run scheduler and use gurobi
RUN useradd -ms /bin/bash eng
RUN mkdir $APPLICATION_ROOT/logs
RUN chown -R eng:eng /lco/

WORKDIR $APPLICATION_ROOT

CMD ["supervisord", "-n"]

