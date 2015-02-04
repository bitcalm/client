import os
import subprocess
from datetime import datetime

VERSION = (0, 1, 0, 'dev', 23)


def get_version(version=VERSION):
    parts = 3 if version[2] else 2
    main = '.'.join(str(x) for x in version[:parts])
    
    if version[3]:
        assert version[3] in ('alpha', 'beta', 'rc', 'dev')
    else:
        return main

    if version[3] == 'dev':
        sub = '.dev'
        git_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        git = subprocess.Popen('git log --pretty=format:%ct --quiet -1 HEAD',
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   cwd=git_path, shell=True)
        ts = git.communicate()[0]
        try:
            ts = int(ts)
        except ValueError:
            pass
        else:
            sub += datetime.utcfromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    else:
        mapping = {'alpha': 'a', 'beta': 'b', 'rc': 'c'}
        sub = '%s%i' % (mapping.get(version[3]), version[4])
    return main + sub

__version__ = get_version()
