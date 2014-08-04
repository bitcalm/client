#!/usr/bin/python
import sys

from setuptools import setup, find_packages

from bitcalm import __version__


if sys.version_info < (2, 7):
    exit('Please upgrade your python to 2.7 or newer')

with open('req.txt') as req:
    install_requires = [s.strip() for s in req.readlines()]

setup(name = 'bitcalm',
      version = __version__,
      packages = find_packages(),
      install_requires = install_requires,
      entry_points = {'console_scripts': ['bitcalm = bitcalm.backupd:main',]},
      data_files = [('/etc', ['default/bitcalm.conf',]),
                    ('/var/lib/bitcalm', ['default/data',]),
                    ('/etc/init.d', ['default/bitcalmd',])]
      )
