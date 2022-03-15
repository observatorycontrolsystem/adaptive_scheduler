FROM centos:centos8

# Fix for centos appstream issue https://stackoverflow.com/questions/70963985/error-failed-to-download-metadata-for-repo-appstream-cannot-prepare-internal/71020440#71020440
RUN cd /etc/yum.repos.d/
RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
RUN sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*

# setup the python environment
ENV APPLICATION_ROOT /ocs

RUN yum -y groupinstall 'Development Tools'
RUN yum -y install pkgconfig epel-release
RUN yum -y install python36-devel python3-wheel gcc gcc-gfortran fftw-devel gsl-devel swig which cmake autoconf zlib-devel wget glpk redhat-lsb-core maven protobuf openssl-devel
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN ln -s /usr/bin/pip3 /usr/bin/pip

# create eng user necessary to run scheduler and use gurobi
WORKDIR $APPLICATION_ROOT
RUN wget https://github.com/Kitware/CMake/releases/download/v3.17.3/cmake-3.17.3.tar.gz
RUN tar -zxvf cmake-3.17.3.tar.gz
WORKDIR $APPLICATION_ROOT/cmake-3.17.3
RUN ./bootstrap
RUN make && make install

RUN useradd -ms /bin/bash eng
RUN chown -R eng:eng /ocs/

WORKDIR $APPLICATION_ROOT
RUN git clone https://github.com/google/or-tools

RUN wget http://ftp.gnu.org/gnu/glpk/glpk-4.65.tar.gz
RUN tar -xzvf glpk-4.65.tar.gz
WORKDIR $APPLICATION_ROOT/glpk-4.65
RUN ./configure
RUN make prefix=/ocs/glpk CFLAGS=-fPIC install
ENV UNIX_GLPK_DIR /ocs/glpk

WORKDIR $APPLICATION_ROOT/or-tools
RUN git checkout v8.1
COPY Makefile.local .

RUN make third_party
RUN make python
RUN make install_python

WORKDIR /ocs/or-tools/temp_python3.6/ortools/
RUN mkdir -p /usr/local/lib64/python3.6/site-packages
RUN python3 setup.py install

ENV SCHEDULER_ROOT /ocs/adaptive_scheduler
# install python libs (numpy needed for pyslalib)
COPY requirements.pip $SCHEDULER_ROOT/requirements.pip
RUN pip3 install numpy
RUN pip3 install -r $SCHEDULER_ROOT/requirements.pip
RUN pip3 install --ignore-installed six

# copy the stuff
COPY . $SCHEDULER_ROOT

# # eng user will run scheduler and use gurobi
RUN chown -R eng:eng /ocs/adaptive_scheduler

WORKDIR $SCHEDULER_ROOT
