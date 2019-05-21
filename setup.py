# !/usr/bin/env python3
# -*- coding: UTF-8 -*-

import sys

from setuptools import find_packages, setup

sys.path.insert(0, './')
import matplotlib
matplotlib.use('PS')
import broker

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(name='pitt_broker',
      version=broker.__version__,
      packages=find_packages(),
      keywords='LSST ZTF broker',
      description='A cloud based data broker for LSST and ZTF',
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Science/Research',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3.7',
          'Topic :: Scientific/Engineering :: Astronomy'
      ],

      python_requires='>=3.7',
      install_requires=requirements,
      include_package_data=False)
