#!/usr/bin/env python

# -----------------------------------------------------------------------------
# Copyright (c) 2020, Qiita development team.
#
# Distributed under the terms of the BSD 3-clause License License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------
from setuptools import setup
from glob import glob

__version__ = "2022.04"

classes = """
    Development Status :: 3 - Alpha
    License :: OSI Approved :: BSD License
    Topic :: Scientific/Engineering :: Bio-Informatics
    Topic :: Software Development :: Libraries :: Application Frameworks
    Topic :: Software Development :: Libraries :: Python Modules
    Programming Language :: Python
    Programming Language :: Python :: 3.9
    Programming Language :: Python :: Implementation :: CPython
    Operating System :: POSIX :: Linux
    Operating System :: MacOS :: MacOS X
"""

with open('README.rst') as f:
    long_description = f.read()

classifiers = [s.strip() for s in classes.split('\n') if s]

setup(name='qp-samtools-sort',
      version=__version__,
      long_description=long_description,
      license="BSD",
      description='Qiita Plugin: qp-samtools-sort',
      author="Grant Cheng",
      author_email="gxcheng@ucsd.edu",
      url='https://github.com/CatFish47/qp-samtools-sort/',
      test_suite='nose.collector',
      packages=['qp_samtools_sort'],
      package_data={'qp_samtools_sort': [
        'support_files/*', 'support_files/raw_data/*']},
      scripts=glob('scripts/*'),
      extras_require={'test': ["nose >= 0.10.1", "pep8"]},

      install_requires=['click >= 3.3', 'future', 'pandas >= 0.15',
                        'qiita_client @ https://github.com/qiita-spots/'
                        'qiita_client/archive/master.zip'],
      classifiers=classifiers
      )
