# syntax = docker/dockerfile:1.4

ARG PYTHON_IMAGE_TAG=3.9-slim
ARG GUROBI_VERSION=9.1.2


FROM alpine as gurobi-src
ARG GUROBI_VERSION

RUN apk add --update --no-cache curl tar

WORKDIR /src

# download & extract gurobi
RUN <<EOT
#!/bin/sh -ex
curl --retry 5 -L "https://packages.gurobi.com/${GUROBI_VERSION%.*}/gurobi${GUROBI_VERSION}_linux64.tar.gz" -o gurobi.tar.gz
tar -xf gurobi.tar.gz
rm -f gurobi.tar.gz
mv -f gurobi* gurobi
EOT


FROM python:${PYTHON_IMAGE_TAG} as base

RUN --mount=type=cache,target=/var/cache/apt --mount=type=cache,target=/var/lib/apt <<EOT
#!/bin/bash -ex
apt-get update
apt-get install --no-install-recommends -y ca-certificates libgfortran5 strace
update-ca-certificates
EOT

# create venv for the app
RUN python -m venv /venv --symlinks

ENV PATH="/venv/bin:${PATH}"

# setup gurobi env
ENV GUROBI_HOME=/opt/gurobi/linux64 PATH="/opt/gurobi/linux64/bin:${PATH}" LD_LIBRARY_PATH="/opt/gurobi/linux64/lib:${LD_LIBRARY_PATH}"

# install gurobi
RUN --mount=type=bind,target=/src/gurobi,from=gurobi-src,source=/src/gurobi,readwrite <<EOT
#!/bin/bash -ex

pip install --upgrade pip wheel

mkdir -p /opt/gurobi/linux64
cp -r /src/gurobi/linux64/lib /opt/gurobi/linux64/lib
cp -r /src/gurobi/linux64/bin /opt/gurobi/linux64/bin
rm -rf /opt/gurobi/linux64/{lib, bin}/python*

cd /src/gurobi/linux64
python setup.py install

EOT

# standard container friendly python env vars
ENV PYTHONBUFFERED=1 PYTHONFAULTHANDLER=1


FROM base as build

RUN --mount=type=cache,target=/var/cache/apt --mount=type=cache,target=/var/lib/apt <<EOT
#!/bin/bash -ex
apt-get install --no-install-recommends -y gfortran
EOT

RUN --mount=type=cache,target=/root/.cache/pip /usr/local/bin/pip install --upgrade pip "poetry ~= 1.1"


WORKDIR /src

COPY ./README.md ./pyproject.toml ./poetry.lock ./ortools-glpk-reqs.txt .

# install python dependencies
RUN --mount=type=cache,target=/root/.cache/pip <<EOT
#!/bin/bash -ex
pip install -r <(poetry export | grep "numpy")
pip install -r <(poetry export)
pip install -r ortools-glpk-reqs.txt
EOT

COPY . .

# install app
RUN --mount=type=cache,target=/root/.cache/pip  <<EOT
#!/bin/bash -ex
pip install .
EOT


FROM build as dev

# install dev dependencies
RUN --mount=type=cache,target=/root/.cache/pip <<EOT
#!/bin/bash -ex
pip install -r <(poetry export --dev)
EOT


FROM base as app

RUN adduser --shell /bin/bash --home /home/app app

COPY --link --from=build /venv /venv

# smoke test ortools & gurobi
RUN <<EOT
#!/bin/bash -ex

python -c "from ortools.linear_solver import pywraplp as p; p.Solver.CreateSolver('SCIP')"
python -c "from ortools.linear_solver import pywraplp as p; p.Solver.CreateSolver('GLPK')"

# assumption: if it's trying to read the licence, it's probably linked properly
strace -e openat python -c "from ortools.linear_solver import pywraplp as p; p.Solver.CreateSolver('GUROBI')" 2>&1 | grep -q gurobi.lic
EOT

USER app

WORKDIR /app

ENTRYPOINT ["adaptive-scheduler"]

# set metadata
ARG PYTHON_IMAGE_TAG
ARG GUROBI_VERSION
LABEL python-image.tag=$PYTHON_IMAGE_TAG gurobi.version=$GUROBI_VERSION
