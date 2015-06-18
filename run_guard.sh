#!/bin/bash

./setup_bundles.sh
export RBENV_ROOT=$HOME/.rbenv
eval "$(rbenv init -)"
bundle exec guard
