#!/usr/bin/env python

from setuptools import setup

setup(name='arrowgen',
      version='1.0',
      description='Generate C++ code to convert protobuf messages to arrow table',
      author='0x26res',
      url='https://github.com/0x26res/arrowgen',
      packages=['arrowgen'],
      package_data={'arrowgen': ['arrowgen/templates/*']},
      entry_points={'arrowgen': ['arrowgen = arrowgen.__main__:main']},
      setup_requires=[],
      install_requires=[],  # TODO: put requirements.txt?
      classifiers=[
          'Natural Language :: English',
          'Operating System :: POSIX :: Linux',
          'Programming Language :: Python :: 3',
      ]
      )
