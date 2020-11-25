#!/usr/bin/env python

import setuptools
from pip._internal.req import parse_requirements

with open("README.md", "r") as fh:
    long_description = fh.read()

install_reqs = parse_requirements('requirements.txt', session='hack')

setuptools.setup(
    name='arrowgen',
    version='0.1.1',
    description='Generate C++ code to convert protobuf messages to arrow table',
    author='0x26res',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/0x26res/arrowgen',
    packages=setuptools.find_packages(),
    package_data={'arrowgen': ['arrowgen/templates/*']},
    entry_points={'console_scripts': ['arrowgen = arrowgen.__main__:main']},
    setup_requires=[
        "protobuf",
        "pyarrow",
        "jinja2"
    ],
    install_requires=install_reqs,
    license="MIT License",
    classifiers=[
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3',
    ]
)
