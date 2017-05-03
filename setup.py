#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Setup for autoreq."""

import io
import sys
from setuptools import setup
from setuptools import find_packages


reqs = []
with open('requirements.txt') as ifile:
    for i in ifile:
        if i.strip() and not i.strip()[0] == '#':
            reqs.append(i)

INSTALL_REQUIRES = (
    reqs +
    (['argparse'] if sys.version_info < (2, 7) else [])
)


def version():
    return "0.1"


with io.open('README.rst') as readme:
    setup(
        name='django-p',
        version=version(),
        description="A task pipeline system",
        long_description=readme.read(),
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Environment :: Console',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2',
            'Programming Language :: Python :: 2.6',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.2',
            'Programming Language :: Python :: 3.3',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Topic :: Software Development :: Libraries :: Python Modules',
            'Topic :: Software Development :: Quality Assurance',
        ],
        keywords='automation, requirements, format',
        install_requires=INSTALL_REQUIRES,
        py_modules=['django_p'],
        packages=find_packages(),
        zip_safe=False,
    )
