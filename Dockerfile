FROM postgres:14

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    devscripts \
    equivs \
    git-buildpackage \
    git \
    lsb-release \
    make \
    openssh-client \
    pristine-tar \
    rake \
    rsync \
    ruby \
    ruby-dev \
    rubygems \
    wget


RUN apt-get install -y postgresql-server-dev-14

COPY . /pg/testdir
WORKDIR /pg/testdir

RUN make USE_PGXS=1
RUN make USE_PGXS=1 install
# RUN make USE_PGXS=1 installcheck

#OUR filse are here:

#/usr/lib/postgresql/14/lib/rum.so
#/usr/share/postgresql/14/extension/rum--1.2--1.3.sql
#/usr/share/postgresql/14/extension/rum--1.0--1.1.sql
#/usr/share/postgresql/14/extension/rum--1.3.sql
#/usr/share/postgresql/14/extension/rum.control
#/usr/share/postgresql/14/extension/rum--1.1--1.2.sql
#/usr/share/postgresql/14/extension/rum--1.0.sql
