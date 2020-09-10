FROM observatorycontrolsystem/adaptive_scheduler_base:1.0.0

# setup the python environment
ENV APPLICATION_ROOT /lco/adaptive_scheduler

# set up env variables for gurobi
ENV GUROBI_HOME /lco/adaptive_scheduler/gurobi
ENV PATH ${PATH}:${GUROBI_HOME}/bin
ENV LD_LIBRARY_PATH ${LD_LIBRARY_PATH}:${GUROBI_HOME}/lib

# install and update packages
RUN yum -y install epel-release \
        && yum -y install gcc glpk-devel fftw-devel gsl-devel suitesparse-devel openblas-devel lapack-devel blas-devel supervisor\
        && yum -y update \
        && yum -y clean all

# install python libs (and set cvxopt to install glpk)
COPY requirements.pip $APPLICATION_ROOT/requirements.pip
RUN pip3.4 install 'numpy<1.17.0' && CVXOPT_BUILD_GLPK=1 pip install -r $APPLICATION_ROOT/requirements.pip

# copy the stuff
COPY . $APPLICATION_ROOT

# create eng user necessary to run scheduler and use gurobi
RUN useradd -ms /bin/bash eng
RUN chown -R eng:eng /lco/

WORKDIR $APPLICATION_ROOT
