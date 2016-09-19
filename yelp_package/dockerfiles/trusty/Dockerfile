FROM docker-dev.yelpcorp.com/trusty_yelp
MAINTAINER Justin Cunningham <justinc@yelp.com>
# Heavily based on kwa's work for paasta-tools

# Make sure we get a package suitable for building this package correctly.
# Per dnephin we need https://github.com/spotify/dh-virtualenv/pull/20
# Which at this time is in this package
RUN apt-get update && apt-get -y install dpkg-dev python-tox python-setuptools \
  python-dev debhelper dh-virtualenv python-yaml python-pytest \
  pyflakes python2.7 python2.7-dev help2man libffi-dev uuid-dev libuuid1 \
  libssl0.9.8 git libmysqlclient-dev libssl-dev

ENV HOME /work
ENV PWD /work
WORKDIR /work
