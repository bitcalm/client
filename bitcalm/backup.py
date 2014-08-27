import os
import math
import gzip
import tarfile

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from filechunkio import FileChunkIO

from bitcalm.config import status


CHUNK_SIZE = 32 * 1024 * 1024


def next_date():
    s = next_schedule()
    return s.next_backup if s else None


def next_schedule():
    if status.schedules:
        return min([s for s in status.schedules if not s.exclude])
    return None


def get_bucket():
    conn = S3Connection(status.amazon['key_id'],
                        status.amazon['secret_key'])
    return conn.get_bucket(status.amazon['bucket'])


def compress(filename, gzipped=None):
    if not gzipped:
        gzipped = '/tmp/%s.gz' % os.path.basename(filename)
    if not os.path.exists(filename):
        return ''
    with open(filename, 'rb') as f:
        with gzip.open(gzipped, 'wb') as gz:
            gz.write(f.read())
    return gzipped


def upload(key_name, filepath, delete=True):
    bucket = get_bucket()
    size = os.stat(filepath).st_size
    if size > CHUNK_SIZE:
        chunks = int(math.ceil(size / float(CHUNK_SIZE)))
        mp = bucket.initiate_multipart_upload(key_name, encrypt_key=True)
        for i in xrange(chunks):
            offset = CHUNK_SIZE * i
            psize = min(CHUNK_SIZE, size - offset)
            with FileChunkIO(filepath, mode='r',
                             offset=offset, bytes=psize) as f:
                mp.upload_part_from_file(f, part_num=i+1)
        mp.complete_upload()
    else:
        k = Key(bucket)
        k.key = key_name
        size = k.set_contents_from_filename(filepath, encrypt_key=True)
    if delete:
        os.remove(filepath)
    return size


def backup(key_name, filename):
    gzipped = compress(filename)
    return upload(key_name, gzipped) if gzipped else 0


def restore(key, paths=None):
    bucket = get_bucket()
    k = bucket.lookup(key)
    if not k:
        return 'There is no key "%s" in the bucket' % key

    tmp = '/tmp/'
    if available_space() < k.size:
        return 'Not enough available space in %s' % tmp

    tmp_file = tmp + key
    k.get_contents_to_filename(tmp_file)
    tar = tarfile.open(tmp_file, 'r:gz')
    if paths:
        def contains(path, member):
            path, member = map(lambda p: filter(None, p.split('/')),
                               (path, member.name))
            if len(path) > len(member):
                return False
            for p_node, m_node in zip(path, member):
                if p_node != m_node:
                    return False
            return True

        def contained(member, paths):
            for path in paths:
                if contains(path, member):
                    return True
            return False

        members = filter(lambda m, paths=paths: contained(m, paths),
                         tar.getmembers())
        if not members:
            os.remove(tmp_file)
            return 'Paths not found in the backup'
    else:
        members = None
    tar.extractall(path='/', members=members)
    tar.close()
    os.remove(tmp_file)
    return None


def available_space(path='/tmp/'):
    stats = os.statvfs(path)
    return stats.f_bavail * stats.f_frsize
