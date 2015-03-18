import re
import time
import platform

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
    return {'distribution': ' '.join(platform.linux_distribution()),
            'kernel': '%s %s' % (platform.system(), platform.release()),
            'proc_type': platform.machine(),
            'python': platform.python_version()}
