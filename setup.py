#!/usr/bin/env python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import data_pipeline


readme = open('README.rst').read()
doclink = """
Documentation
-------------

The full documentation is at http://servicedocs.yelpcorp.com/docs/data_pipeline/"""
history = open('HISTORY.rst').read().replace('.. :changelog:', '')

setup(
    name='data_pipeline',
    version=data_pipeline.__version__,
    description="Provides an interface to consume and publish to data pipeline topics.",
    long_description=readme + '\n\n' + doclink + '\n\n' + history,
    author=data_pipeline.__author__,
    author_email=data_pipeline.__email__,
    url='http://servicedocs.yelpcorp.com/docs/data_pipeline/',
    packages=[
        'data_pipeline',
    ],
    package_dir={'data_pipeline': 'data_pipeline'},
    include_package_data=True,
    install_requires=[
        'avro>=1.7.7',
        'cached-property>=0.1.2',
        'enum34>=1.0.4',
    ],
    zip_safe=False,
    keywords='data_pipeline',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
)
