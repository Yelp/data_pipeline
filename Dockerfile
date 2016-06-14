FROM ubuntu:14.04.1
MAINTAINER justinc@yelp.com

run apt-get update && apt-get upgrade -y
run apt-get install -y wget language-pack-en-base

run locale-gen en_US en_US.UTF-8 && dpkg-reconfigure locales

run mkdir /src

workdir /src

run wget https://bitbucket.org/pypy/pypy/downloads/pypy-5.1.1-linux64.tar.bz2
run bunzip2 pypy-5.1.1-linux64.tar.bz2
run tar xvf pypy-5.1.1-linux64.tar

ENV PATH $PATH:/src/pypy-5.1.1-linux64/bin/

run wget https://bootstrap.pypa.io/get-pip.py
run pypy get-pip.py

run apt-get update && apt-get install -y build-essential git vim libpq5 libpq-dev docker \
    libmysqlclient-dev libsnappy-dev


run ln -s /usr/bin/gcc /usr/local/bin/cc

run pip install virtualenv tox

# Setup clientlib
WORKDIR /data_pipeline
add requirements.d/dev.txt /data_pipeline/requirements.d/dev.txt
add requirements.d/tools.txt /data_pipeline/requirements.d/tools.txt
add requirements.txt /data_pipeline/requirements.txt
add setup.py /data_pipeline/setup.py
add data_pipeline/__init__.py /data_pipeline/data_pipeline/__init__.py
add README.rst /data_pipeline/README.rst
add HISTORY.rst /data_pipeline/HISTORY.rst
add bin/ /data_pipeline/bin

# Install dependencies
run mkdir /dp_reqs
run virtualenv /dp_reqs/venv
run /dp_reqs/venv/bin/pip install -i https://pypi.yelpcorp.com/simple/ -r /data_pipeline/requirements.d/dev.txt

ADD . /data_pipeline

VOLUME ["/data_pipeline"]
