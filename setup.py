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
        'avro==1.9.0-yelp3',
        'cached-property>=0.1.5',
        'cffi>=1.1.2',
        'enum34>=1.0.4',
        'kafka-python==0.9.5.post4',
        'psutil==4.2.0',
        'pycrypto>=2.6.1',
        'PyStaticConfiguration>=0.9.0',
        'simplejson>=2.1.2',
        'swaggerpy>=0.7.6',
        'yelp-avro>=0.1.1',
        'yelp-kafka>=4.2.0',
        'yelp-lib>=10.0.5',
        'yelp-servlib>=4.3.0',
        'cryptography<=1.3.4',
    ],
    extras_require={
        'tools': [
            # yelp_clog isn't compatible with triftpy 0.2.0, which is an
            # implicit dependency of yelp_batch
            'thriftpy<0.2.0',
            'yelp_batch>=0.19.4',
            'yelp_conn>=7.0.0',
            'yelp-kafka-tool>=0.8.2'
        ],
        'testing_helpers': [
            'docker-compose==1.5.2',
            'docker-py==1.6.0',
            # requests is locked at <2.7 to satisfy a docker-compose requirement
            'requests<2.7'
        ]
    },
    zip_safe=False,
    keywords='data_pipeline',
    package_data={
        str('data_pipeline'): [
            'schemas/*.avsc',
            'testing_helpers/docker-compose.yml'
        ],
    },
    scripts=[
        'bin/data_pipeline_tailer',
        'bin/data_pipeline_refresh_runner',
        'bin/data_pipeline_refresh_manager',
        'bin/data_pipeline_refresh_job',
        'bin/data_pipeline_compaction_setter',
        'bin/data_pipeline_introspector'
    ],
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
