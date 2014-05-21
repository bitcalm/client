import os
import tarfile
import calendar
from datetime import datetime, date, timedelta

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from config import status


class SCHEDULE:
    DAILY = 'daily'
    WEEKLY = 'weekly'
    MONTHLY = 'monthly'
    TYPES = (DAILY, WEEKLY, MONTHLY)


def next_daily():
    period = status.schedule.get(SCHEDULE.DAILY)
    if not period:
        return None
    last = status.schedule.get('last')
    if not last:
        return datetime.now()
    prev_date = date(*map(int, last.split('.')))
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
    for d in backup_days:
        if d >= today_index:
            next_day = d
            break
    else:
        next_day = backup_days[0]
    return today + timedelta(days=next_day-today_index)


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


def backup():
    tmp_file = '/tmp/backup.tar.gz'
    with tarfile.open(tmp_file, 'w:gz') as tar:
        for path in status.files:
            tar.add(path)
    
    conn = S3Connection(status.amazon['key_id'],
                        status.amazon['secret_key'])
    bucket = conn.get_bucket(status.amazon['bucket'])
    k = Key(bucket)
    k.key = 'backup.tar.gz'
    k.set_contents_from_filename(tmp_file, encrypt_key=True)
    os.remove(tmp_file)
