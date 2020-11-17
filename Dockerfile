FROM observatorycontrolsystem/adaptive_scheduler_base:1.1.0

# setup the python environment
ENV SCHEDULER_ROOT /ocs/adaptive_scheduler

# install and update packages
RUN yum -y install epel-release \
        && yum -y install gcc gcc-gfortran fftw-devel gsl-devel \
        && yum -y update \
        && yum -y clean all

# install python libs (numpy needed for pyslalib)
COPY requirements.pip $SCHEDULER_ROOT/requirements.pip
RUN pip3 install numpy
RUN pip3 install -r $SCHEDULER_ROOT/requirements.pip

# copy the stuff
COPY . $SCHEDULER_ROOT

# # eng user will run scheduler and use gurobi
RUN chown -R eng:eng /ocs/adaptive_scheduler

WORKDIR $SCHEDULER_ROOT
