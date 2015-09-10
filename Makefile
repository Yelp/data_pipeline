REBUILD_FLAG =

.PHONY: help all production clean clean-pyc clean-build clean-docs clean-vim lint test docs coverage install-hooks

help:
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "clean-docs - remove doc creation artifacts"
	@echo "clean-vim - remove vim swap file artifacts"
	@echo "lint - check style with flake8"
	@echo "test - run tests quickly with the default Python"
	@echo "coverage - check code coverage"
	@echo "docs - generate Sphinx HTML documentation, including API docs"

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

lint:
	tox -e style

test:.venv.touch
	# This will timeout after 15 minutes, in case there is a hang on jenkins
	timeout -9 900 tox $(REBUILD_FLAG)

docs: clean-docs
	tox -e docs

coverage: test

.venv.touch: setup.py requirements-dev.txt
	$(eval REBUILD_FLAG := --recreate)
	touch .venv.touch

install-hooks:
	tox -e pre-commit -- install -f --install-hooks
