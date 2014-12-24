import os
import math
import gzip
from hashlib import sha384 as sha

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from filechunkio import FileChunkIO

from bitcalm import log
from bitcalm.api import api
from bitcalm.config import status
from bitcalm.config.base import BackupData
from bitcalm.utils import is_file_compressed
from bitcalm.database import get_credentials, import_db


CHUNK_SIZE = 32 * 1024 * 1024
MB = 1024 * 1024
RESTORE_DB_PATH = '/tmp/bitcalm_restore.db'


class PREFIX_TYPE:
    FS = 'filesystem/'
    DB = 'databases/'


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


def get_prefix(backup_id, ptype=''):
    return '/'.join((status.amazon['username'].encode('ascii'),
                     'backup_%i' % backup_id,
                     ptype))


def get_prefixes(backup_id):
    root_prefix = get_prefix(backup_id)
    return (root_prefix,
            root_prefix + PREFIX_TYPE.FS,
            root_prefix + PREFIX_TYPE.DB)


def make_path_fs_key(prefix, path, compressed=True):
    return ''.join((prefix, path.lstrip('/'), '.gz' if compressed else ''))


def make_hash_fs_key(prefix, path):
    return prefix + sha(path).hexdigest()


def make_db_key(prefix, path):
    return prefix + os.path.basename(path)


def chunks(fileobj, chunk_size=4*MB):
    chunk = fileobj.read(chunk_size)
    while chunk:
        yield chunk
        chunk = fileobj.read(chunk_size)


def compress(filename, gzipped=None):
    if not gzipped:
        gzipped = '/tmp/bitcalm_compress.gz'
    if not os.path.exists(filename):
        return ''
    gz = gzip.open(gzipped, 'wb')
    with open(filename, 'rb') as f:
        for chunk in chunks(f):
            gz.write(chunk)
    gz.close()
    return gzipped


def decompress(zipped, unzipped=None, delete=True):
    if not unzipped:
        unzipped = zipped[:-3]
    dirname = os.path.dirname(unzipped)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    gz = gzip.open(zipped, 'rb')
    with open(unzipped, 'wb') as f:
        for chunk in chunks(gz):
            f.write(chunk)
    gz.close()
    if delete:
        os.remove(zipped)
    return unzipped


def upload(key_name, filepath, bucket=None, **kwargs):
    if not bucket:
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
    return size


def download(key, path, check_space=True):
    """ Returns:
            -1 if key wasn't found;
            0 on success;
            number of bytes in key if there is not enough free space for it.
    """
    if not isinstance(key, Key):
        b = get_bucket()
        k = b.lookup(key)
        if k:
            key = k
        else:
            return -1
    if check_space and available_space(path=os.path.dirname(path)) < key.size:
        return key.size
    key.get_contents_to_filename(path)
    return 0


class BackupHandler(object):
    def __init__(self, backup_id):
        self.id = backup_id
        self.bucket = get_bucket()
        self.prefix, self.prefix_fs, self.prefix_db = get_prefixes(self.id)
        self.size = 0
        self.files_count = 0
        self.db_count = 0

    def __enter__(self):
        if not self.bucket:
            self.bucket = get_bucket()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.files_count:
            self.upload_fs_info()
        self.upload_stats()

    def get_fs_keyname(self, filename):
        return make_hash_fs_key(self.prefix_fs, filename)

    def get_db_keyname(self, filename):
        return make_db_key(self.prefix_db, filename)

    def upload_file(self, filename):
        """ compress if necessary and upload file
        """
        key_name = self.get_fs_keyname(filename)
        size, compress = backup(key_name, filename, bucket=self.bucket)
        self.files_count += 1
        self.size += size
        return size, compress

    def upload_db(self, path):
        """ upload dump file
        """
        size = upload(self.get_db_keyname(path), path, bucket=self.bucket)
        self.db_count += 1
        self.size += size
        return size

    def upload_fs_info(self):
        return backup(self.prefix + os.path.basename(status.backupdb.db),
                      status.backupdb.db,
                      bucket=self.bucket)

    def upload_stats(self):
        if not self.has_stats():
            return True
        if api.update_backup_stats(self.id,
                                   size=self.size,
                                   files=self.files_count,
                                   db=self.db_count) == 200:
            self.reset_stats()
            return True
        return False

    def has_stats(self):
        return any((self.size, self.files_count, self.db_count))

    def reset_stats(self):
        self.size = self.files_count = self.db_count = 0


def backup(key_name, filename, bucket=None):
    need_to_compress = not is_file_compressed(filename)
    if need_to_compress:
        filename = compress(filename)
        if not filename:
            return 0, False
    try:
        size = upload(key_name, filename, bucket=bucket)
    finally:
        if need_to_compress:
            os.remove(filename)
    return size, need_to_compress


def get_database(backup_id, path=None):
    """ path is path where to decompress database; default is main db.
        Returns:
            -1 if database wasn't found;
            0 on success;
            number of bytes of compressed database if there is not enough free space for it.
    """
    basename = os.path.basename(status.backupdb.db)
    key = get_prefix(backup_id) + basename
    gzipped = os.path.join('/tmp', basename)
    result = download(key, gzipped)
    if result:
        return result
    decompress(gzipped, unzipped=path or status.backupdb.db)
    return 0


def get_files(backup_id):
    error = get_database(backup_id, path=RESTORE_DB_PATH)
    if error:
        return None
    files = BackupData(dbpath=RESTORE_DB_PATH).files(backup_id=backup_id,
                                                     iterator=True)
    return files


def restore(backup_id):
    bucket = get_bucket()
    files = status.backupdb.files(backup_id=backup_id,
                                  iterator=True) or get_files(backup_id)
    if not files:
        s, files = api.get_files_info(backup_id)
        if s == 200:
            files = ((path, b_id, True) for path, b_id in files.items())
        else:
            return 'Failed to request the list of files'
    backup_prefixes = {}
    low_space_msg = 'Need at least %i bytes free'
    for path, b_id, hash_key, compressed in files:
        prefix = backup_prefixes.get(b_id)
        if not prefix:
            prefix = get_prefix(b_id, ptype=PREFIX_TYPE.FS)
            backup_prefixes[b_id] = prefix
        if hash_key:
            keyname = make_hash_fs_key(prefix, path)
        else:
            keyname = make_path_fs_key(prefix, path, compressed=compressed)
        key = bucket.get_key(keyname)
        if not key:
            continue
        if compressed:
            gzipped = '/tmp' + os.path.basename(path)
            if download(key, gzipped):
                return low_space_msg % key.size
            decompress(gzipped, path)
        else:
            dirname = os.path.dirname(path)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            av_space = available_space(path=dirname)
            if os.path.exists(path):
                av_space += os.stat(path).st_size
            if av_space < key.size:
                return low_space_msg
            download(key, path, check_space=False)

    if os.path.exists(RESTORE_DB_PATH):
        os.remove(RESTORE_DB_PATH)

    prefix = get_prefix(backup_id, ptype=PREFIX_TYPE.DB)
    db_keys = bucket.get_all_keys(prefix=prefix)
    db_creds = {}
    for k in db_keys:
        basename = os.path.basename(k.key)
        host, port, name = basename.split('_', 3)[:3]
        port = int(port)
        db_key = '%s:%i' % (host, port)
        if db_key not in db_creds:
            try:
                db_creds[db_key] = get_credentials(host, port)
            except ValueError:
                log.error('There are no credentials for %s:%i' % (host, port))
                continue

        gzipped = '/tmp/' + basename
        if download(k, gzipped):
            return 'Need at least %i bytes free' % k.size
        filename = decompress(gzipped)

        user, passwd = db_creds[db_key]
        if import_db(filename, user, host, passwd, port, name):
            os.remove(filename)
        else:
            log.error('Failed to import %s to %s:%i' % (name, host, port))
    return None


def available_space(path='/tmp/'):
    try:
        stats = os.statvfs(path)
    except OSError, e:
        if e.errno == 2:
            return available_space(os.path.dirname(path))
        else:
            raise e
    return stats.f_bavail * stats.f_frsize
