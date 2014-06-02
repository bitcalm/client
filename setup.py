#!/usr/bin/python
from setuptools import setup, find_packages

from bitcalm import __version__


with open('req.txt') as req:
    install_requires = [s.strip() for s in req.readlines()]

setup(name = 'bitcalm',
      version = __version__,
      packages = find_packages(),
      install_requires = install_requires,
      entry_points = {'console_scripts': ['bitcalm = bitcalm.backupd:main',]},
      data_files = [('/etc', ['default/bitcalm.conf',]),
                    ('/var/lib/bitcalm', ['default/data',])]
      )
