#!/bin/bash

export RBENV_ROOT=$HOME/.rbenv
if [ ! -d "$HOME/.rbenv/plugins/ruby-build" ]; then
    git clone https://github.com/sstephenson/ruby-build.git $HOME/.rbenv/plugins/ruby-build
fi
rbenv install 2.2.2 -s
rbenv local 2.2.2
rbenv rehash
eval "$(rbenv init -)"
gem install bundler
rbenv rehash
eval "$(rbenv init -)"
bundle install --path=.bundle
