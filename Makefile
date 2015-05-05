REBUILD_FLAG =

.PHONY: help all production clean clean-pyc clean-build clean-docs lint test docs coverage install-hooks

help:
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "clean-docs - remove doc creation artifacts"
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

lint:
	tox -e style

test:.venv.touch
	tox $(REBUILD_FLAG)

docs:
	tox -e docs

coverage: test

.venv.touch: setup.py requirements-dev.txt
	$(eval REBUILD_FLAG := --recreate)
	touch .venv.touch

install-hooks:
	tox -e pre-commit -- install -f --install-hooks
