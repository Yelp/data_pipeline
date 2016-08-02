CURRENT_VERSION=$(strip $(shell sed -n -r "s/__version__ = '(.+)'/\1/p" $(CURDIR)/data_pipeline/__init__.py))
NEXT_VERSION=$(shell echo $(CURRENT_VERSION) | awk -F. '/[0-9]+\./{$$NF+=1;OFS=".";print}')

REBUILD_FLAG =

.PHONY: help all production clean clean-pyc clean-build clean-docs clean-vim lint test docs coverage install-hooks release prepare-release compose-prefix

help:
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "clean-docs - remove doc creation artifacts"
	@echo "clean-vim - remove vim swap file artifacts"
	@echo "test - run tests quickly with the default Python"
	@echo "coverage - check code coverage"
	@echo "docs - generate Sphinx HTML documentation, including API docs"
	@echo "compose-prefix - generates a preconfigured docker-compose command"
	@echo "prepare-release - Bump the version number and add a changelog entry (pushmasters only)"
	@echo "release - Commit the latest version, tag the commit, and push it (pushmasters only)"
	@echo "get-venv-update - fetched the latest version of venv-update"

all: production install-hooks

production:
	@true

clean: clean-build clean-pyc clean-docs

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr *.egg-info

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +

clean-docs:
	rm -rf docs/build/*
	rm -rf docs/code/*

clean-vim:
	find . -name '*.swp' -exec rm -f {} +
	find . -name '*.swo' -exec rm -f {} +

test:
	# This will timeout after 15 minutes, in case there is a hang on jenkins
	PULL_CONTAINERS=true FORCE_FRESH_CONTAINERS=true timeout -9 900 tox $(REBUILD_FLAG)

docs: clean-docs 
	tox -e docs $(REBUILD_FLAG)

coverage: test

install-hooks:
	tox -e pre-commit -- install -f --install-hooks

# See the makefile in yelp_package/Makefile for packaging stuff
itest_%:
	make -C yelp_package $@

# Steps to release (Don't do this if you are not a pushmaster - see "Pushing Code"
# on y/datapipeline)
# 1. `make prepare-release`
# 2. `make release`
LAST_COMMIT_MSG = $(shell git log -1 --pretty=%B )
prepare-release:
	dch -v $(NEXT_VERSION) --changelog debian/changelog "Commit: $(LAST_COMMIT_MSG)"
	sed -i -r "s/__version__ = '(.+)'/__version__ = '$(NEXT_VERSION)'/" data_pipeline/__init__.py
	@git diff

release:
	git commit -a -m "Released $(CURRENT_VERSION) via make release"
	git tag v$(CURRENT_VERSION)
	git push --tags origin master && git push origin master

compose-prefix:
	@python -c "from data_pipeline.testing_helpers.containers import Containers; print Containers.compose_prefix()"
