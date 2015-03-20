import os
from shutil import copyfile

from .base import Config, Status


DATA = '/var/lib/bitcalm/data'
DATA_BACKUP = DATA + '.bak'


config = Config('/etc/bitcalm.conf')

try:
    status = Status(DATA)
except EOFError, e:
    if os.path.exists(DATA_BACKUP):
        copyfile(DATA_BACKUP, DATA)
        status = Status(DATA)
    else:
        raise e
else:
    copyfile(DATA, DATA_BACKUP)
