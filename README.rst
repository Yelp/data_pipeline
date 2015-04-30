=============================
Data Pipeline Clientlib
=============================

Provides an interface to tail and publish to data pipeline topics.

Developing
----------

The clientlib is setup for TDD.  A `Guardfile`, used to trigger commands on
file changes is included, as is a `Procfile`, which is useful for running 
development services.

To run guard, which will run tests and build docs as files are modified::

  bundle exec guard

To run dependent processes, run::

  bundle exec foreman start

This will start a docker container with Kafka and a documentation server, 
accessible at `http://HOST:8001/docs/build/html/`.  Tests will automatically
reuse a running Kafka container instead of starting a new one, which can
decrease test time dramatically.

It is recommended to run `foreman` in a background tab, and `guard` in a
foreground tab, as `foreman` will let you inspect the logs of the running
`kafka` containers, while `guard` will show test results as files are changed.

Features
--------

* A fast producer for Kafka

