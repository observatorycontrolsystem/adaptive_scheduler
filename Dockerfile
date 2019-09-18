FROM centos:centos7
MAINTAINER Jon Nation <jnation@lco.global>

# setup the python environment
ENV APPLICATION_ROOT /lco/adaptive_scheduler

# set up env variables for gurobi
ENV GUROBI_HOME /lco/adaptive_scheduler/gurobi811/linux64/
ENV PATH ${PATH}:${GUROBI_HOME}/bin
ENV LD_LIBRARY_PATH ${LD_LIBRARY_PATH}:${GUROBI_HOME}/lib

# install and update packages
RUN yum -y install epel-release \
        && yum -y install gcc glpk-devel fftw-devel gsl-devel suitesparse-devel openblas-devel lapack-devel blas-devel python-devel python-pip supervisor\
        && yum -y update \
        && yum -y clean all

# install python libs (and set cvxopt to install glpk)
COPY requirements.pip $APPLICATION_ROOT/requirements.pip
COPY gurobi/gurobi811 $APPLICATION_ROOT/gurobi811
RUN pip install --upgrade pip
RUN pip install numpy && CVXOPT_BUILD_GLPK=1 pip --trusted-host buildsba.lco.gtn install -r $APPLICATION_ROOT/requirements.pip
RUN cd $APPLICATION_ROOT/gurobi811/linux64 && python setup.py install
# copy the stuff
COPY . $APPLICATION_ROOT

# create eng user necessary to run scheduler and use gurobi
RUN useradd -ms /bin/bash eng
RUN chown -R eng:eng /lco/

WORKDIR $APPLICATION_ROOT

CMD ["supervisord", "-n"]

