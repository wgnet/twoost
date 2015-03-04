#!/usr/bin/env python

import twoost

try:
    from setuptools import setup
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup

setup(
    name='twoost',
    version=twoost.__version__,
    author="a_zhlobich",
    author_email="a_zhlobich@wargaming.net",
    packages=['twoost'],
    platforms=['Independant'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.x',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    install_requires=[
        'twisted>=13.2',
        'pika>=0.9.12',
        'psutil>=2.0',
        'argparse>=1.2',
        'msgpack-python>=0.4',
        'raven>=4.0',
        'crontab>=0.20',
    ],
)
