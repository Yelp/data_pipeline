# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from setuptools import find_packages
from setuptools import setup

import data_pipeline

readme = open('README.rst').read()
doclink = """
Documentation
-------------

The full documentation is at http://servicedocs.yelpcorp.com/docs/data_pipeline/"""
history = open('HISTORY.rst').read().replace('.. :changelog:', '')

setup(
    # py2 + setuptools asserts isinstance(name, str) so this needs str()
    name=str('data_pipeline'),
    version=data_pipeline.__version__,
    description="Provides an interface to consume and publish to data pipeline topics.",
    long_description=readme + '\n\n' + doclink + '\n\n' + history,
    author=data_pipeline.__author__,
    author_email=data_pipeline.__email__,
    url='http://servicedocs.yelpcorp.com/docs/data_pipeline/',
    packages=find_packages(exclude=['tests*', 'benchmarks*']),
    include_package_data=True,
    install_requires=[
        'avro>=1.7.7',
        'cached-property>=0.1.5',
        'cffi>=1.1.2',
        'enum34>=1.0.4',
        'kafka-python==0.9.4.post1',
        'PyStaticConfiguration>=0.9.0',
        'simplejson>=2.1.2',
        'swaggerpy>=0.7.6',
        'yelp-kafka>=4.1.0',
        'yelp-lib>=11.0.2',
        'yelp-servlib>=4.3.0',
    ],
    zip_safe=False,
    keywords='data_pipeline',
    package_data={
        str('data_pipeline'): [
            'schemas/*.avsc'
        ]
    },
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
)
