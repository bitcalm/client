#! /usr/bin/env python2.7
import os
import sys
import shutil

from setuptools import setup, find_packages

from bitcalm import __version__


if sys.version_info < (2, 6):
    exit('Please upgrade your python to 2.6 or newer')

with open('req.txt') as req:
    install_requires = [s.strip() for s in req.readlines()]

setup(name = 'bitcalm',
      version = __version__,
      packages = find_packages(),
      install_requires = install_requires,
      entry_points = {'console_scripts': ['bitcalm = bitcalm.backupd:main',]},
      data_files = [('/etc/init.d', ['default/bitcalmd',])]
      )

data_dir = '/var/lib/bitcalm'
if not os.path.exists(data_dir):
    os.makedirs(data_dir, mode=0755)
for path, item in (('/etc', 'default/bitcalm.conf'),
                   (data_dir, 'default/data')):
    dst = os.path.join(path, os.path.basename(item))
    if not os.path.exists(dst):
        shutil.copyfile(item, dst)
