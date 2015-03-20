import re
import time
import platform
import subprocess

from bitcalm.const import DAY, MICROSEC


COMPRESSED = ('gz', 'bz', 'bz2', 'z', 'lzma', 'gzip', 'lz', 'lzo', 'xz',
              '7z', 'zip', 'tbz', 'tbz2', 'tgz', 'rar', 'sfx', 'bzip', 'bzip2',
              'deb', 'rpm', 'mint', 'pet',
              'lzm', 'ar',
              'jpg', 'jpeg', 'gif', 'png',
              'mp3', 'ogg')
COMPRESSED_PARTS = (r'7z\.\d{3}', r'r\d{2}', r'z\d{2}')
COMPRESSED_RE = re.compile(r'.*\.(?:%s)$' \
                                % '|'.join(COMPRESSED + COMPRESSED_PARTS))


def total_seconds(td):
    return td.days * DAY + td.seconds + td.microseconds * MICROSEC


def is_file_compressed(path):
    return bool(COMPRESSED_RE.match(path))


def try_exec(func, args=(), kwargs={}, exc=Exception, tries=3, pause=60):
    while tries:
        tries -= 1
        try:
            return func(*args, **kwargs)
        except exc, e:
            if tries:
                time.sleep(pause)
            else:
                raise e


def get_system_info():
    """unit of measurement of memory is kB"""
    data = {'kernel': '%s %s' % (platform.system(), platform.release()),
            'proc_type': platform.machine(),
            'python': platform.python_version()}
    distr = ' '.join(filter(None, platform.linux_distribution()))
    if distr:
        data['distribution'] = distr
    for cmd, r, g, name in (('head -n 1 /proc/meminfo', '\d+', 0, 'memory'),
                            ('df --total', 'total\s+(\d+)', 1, 'space')):
        p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
        if p.poll():
            continue
        result = re.search(r, p.stdout.read())
        if result:
            data[name] = result.group(g)
    return data
