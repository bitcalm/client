import os
import math
import tarfile
import calendar
from datetime import datetime, date, timedelta

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from filechunkio import FileChunkIO

from bitcalm.config import status


TMP_FILEPATH = '/tmp/backup.tar.gz'
CHUNK_SIZE = 32 * 1024 * 1024

class SCHEDULE:
    DAILY = 'daily'
    WEEKLY = 'weekly'
    MONTHLY = 'monthly'
    TYPES = (DAILY, WEEKLY, MONTHLY)


def next_daily():
    period = status.schedule.get(SCHEDULE.DAILY)
    if not period:
        return None
    if not status.prev_backup:
        return datetime.utcnow()
    prev_date = date(*map(int, status.prev_backup.split('.')))
    next_date = prev_date + timedelta(days=period)
    return datetime(next_date.year,
                    next_date.month,
                    next_date.day,
                    *status.schedule['time'])


def next_weekly():
    days = status.schedule.get(SCHEDULE.WEEKLY)
    if not days:
        return None
    today = date.today()
    today_index = today.isoweekday() % 7
    
    backup_days = []
    for i in range(7):
        if days & 1 << i:
            backup_days.append(i)
    
    curr_week = filter(lambda x, t=today_index: x >= t, backup_days)
    if status.prev_backup == today.strftime('%Y.%m.%d'):
        curr_week = curr_week[1:]
    next_day = curr_week[0] if curr_week else backup_days[0] + 7
    next_date = today + timedelta(days=next_day-today_index)
    return datetime(next_date.year,
                    next_date.month,
                    next_date.day,
                    *status.schedule['time'])


def next_monthly():
    day = status.schedule.get(SCHEDULE.MONTHLY)
    if not day:
        return None
    today = date.today()
    month = today.month if day >= today.day else today.month + 1
    year = today.year if month >= today.month else today.year + 1
    if day >= 29:
        day = min(calendar.monthrange(year, month)[1], day)
    return datetime(year, month, day, *status.schedule['time'])


def get_next():
    for key in SCHEDULE.TYPES:
        if key in status.schedule:
            func = {SCHEDULE.DAILY: next_daily,
                    SCHEDULE.WEEKLY: next_weekly,
                    SCHEDULE.MONTHLY: next_monthly}
            return func[key]
    return None


def next_date():
    func = get_next()
    return func() if func else None


def get_bucket():
    conn = S3Connection(status.amazon['key_id'],
                        status.amazon['secret_key'])
    return conn.get_bucket(status.amazon['bucket'])


def compress(tmp_file=TMP_FILEPATH):
    with tarfile.open(tmp_file, 'w:gz') as tar:
        for path in status.files:
            tar.add(path)
    return tmp_file


def upload(filepath=TMP_FILEPATH, delete=True):
    bucket = get_bucket()
    size = os.stat(filepath).st_size
    key_name = os.path.basename(filepath)
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
    return key_name, size


def backup(filepath=TMP_FILEPATH):
    return upload(compress(filepath))


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
