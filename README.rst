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

Installing a more modern ruby version may be necessary.  To do this on a dev 
machine, from the data_pipeline project root, install ruby-build, ruby 2.2.2, 
and bundler, then the bundled gems::

  git clone https://github.com/sstephenson/ruby-build.git ~/.rbenv/plugins/ruby-build
  export RBENV_ROOT=$HOME/.rbenv
  rbenv install 2.2.2
  rbenv local 2.2.2
  rbenv rehash
  eval "$(rbenv init -)"
  gem install bundler
  rbenv rehash
  bundle install --path=.bundle

This will start a docker container with Kafka and a documentation server, 
accessible at `http://HOST:8001/docs/build/html/`.  Tests will automatically
reuse a running Kafka container instead of starting a new one, which can
decrease test time dramatically.

It is recommended to run `foreman` in a background tab, and `guard` in a
foreground tab, as `foreman` will let you inspect the logs of the running
`kafka` containers, while `guard` will show test results as files are changed.
