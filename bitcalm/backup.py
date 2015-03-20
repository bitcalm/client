import os
import math
import gzip
from hashlib import sha384 as sha
from cStringIO import StringIO

from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
from boto.s3.key import Key
from filechunkio import FileChunkIO

from bitcalm import log
from bitcalm.api import api
from bitcalm.config import status
from bitcalm.config.base import BackupData
from bitcalm.utils import is_file_compressed, try_exec
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


def chunks(path, chunk_size=CHUNK_SIZE):
    size = os.stat(path).st_size
    total_chunks = int(math.ceil(size / float(chunk_size)))
    for i in xrange(total_chunks):
        offset = chunk_size * i
        psize = min(chunk_size, size - offset)
        chunk = FileChunkIO(path, mode='r', offset=offset, bytes=psize)
        yield chunk
        chunk.close()


def compress_chunks(chunks, chunk_size=CHUNK_SIZE):
    chunk = StringIO()
    gz = gzip.GzipFile(fileobj=chunk, mode='wb')
    for c in chunks:
        gz.write(c.read())
        if gz.tell() > chunk_size:
            chunk.seek(0)
            yield chunk
            chunk.seek(0)
            chunk.truncate()
    gz.close()
    chunk.seek(0)
    yield chunk


def compress(fileobj):
    c = StringIO()
    gz = gzip.GzipFile(fileobj=c, mode='wb')
    fileobj.seek(0)
    gz.write(fileobj.read())
    gz.close()
    c.seek(0)
    return c


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


def upload_multipart(key_name, parts, bucket=None):
    mp = bucket.initiate_multipart_upload(key_name, encrypt_key=True)
    size = 0
    for i, part in enumerate(parts):
        try:
            size += try_exec(mp.upload_part_from_file,
                             args=(part,), kwargs={'part_num': i+1},
                             exc=S3ResponseError).size
        except Exception, e:
            mp.cancel_upload()
            log.error('Upload of part %i failed: %s' % (i, str(e)))
            return 0
    mp.complete_upload()
    return size


def upload(key_name, fileobj, bucket=None):
    if not bucket:
        bucket = get_bucket()
    k = Key(bucket)
    k.key = key_name
    size = try_exec(k.set_contents_from_file,
                    args=(fileobj,), kwargs={'encrypt_key': True},
                    exc=S3ResponseError)
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
        self.db_names = []

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
        need_to_compress = not is_file_compressed(filename)
        multipart = os.stat(filename).st_size > CHUNK_SIZE
        if multipart:
            data = chunks(filename)
            if need_to_compress:
                data = compress_chunks(data)
            size = upload_multipart(key_name, data, bucket=self.bucket)
        else:
            with open(filename, 'r') as f:
                if need_to_compress:
                    f = compress(f)
                size = upload(key_name, f, bucket=self.bucket)

        self.files_count += 1
        self.size += size
        return size, need_to_compress

    def upload_db(self, path):
        """ upload dump file
        """
        with open(path, 'r') as f:
            size = upload(self.get_db_keyname(path), f, bucket=self.bucket)
        self.db_names.append(os.path.basename(path))
        self.size += size
        return size

    def upload_fs_info(self):
        with open(status.backupdb.db, 'r') as f:
            return upload(self.prefix + os.path.basename(status.backupdb.db),
                          compress(f),
                          bucket=self.bucket)

    def upload_stats(self):
        if not self.has_stats():
            return True
        if api.update_backup_stats(self.id,
                                   size=self.size,
                                   files=self.files_count,
                                   db_names=self.db_names) == 200:
            self.reset_stats()
            return True
        return False

    def has_stats(self):
        return any((self.size, self.files_count, self.db_names))

    def reset_stats(self):
        self.size = self.files_count = 0
        del self.db_names[:]


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
