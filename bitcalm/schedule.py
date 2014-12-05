import os
import calendar
from datetime import datetime, date, time, timedelta

from bitcalm.const import IGNORE_DIRS


class Schedule(object):
    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
        self.time = time(*kwargs.get('time'))
        self.files = kwargs.get('files', [])
        self.clean_files()
        self.databases = kwargs.get('db', [])
        self.prev_backup = None
        self.next_backup = self.get_next()
        self.exclude = False
    
    def __cmp__(self, other):
        return cmp(self.next_backup, other.next_backup)

    def get_next(self):
        return datetime.combine(date.today(), self.time)

    def clean_files(self):
        self.files = set(self.files)
        if '/' in self.files:
            self.files.remove('/')
            root = os.listdir('/')
            for item in root:
                if item not in IGNORE_DIRS:
                    self.files.add(os.path.join('/', item))

    def update(self, **kwargs):
        self.time = time(*kwargs.get('time'))
        self.files = kwargs.get('files', [])
        self.clean_files()
        self.databases = kwargs.get('db', [])
        self.next_backup = self.get_next()

    def done(self):
        self.prev_backup = datetime.utcnow()
        self.next_backup = self.get_next()


class DailySchedule(Schedule):
    def __init__(self, **kwargs):
        self.period = kwargs.pop('day')
        Schedule.__init__(self, **kwargs)

    def get_next(self):
        if not self.prev_backup:
            return Schedule.get_next(self)
        next_date = self.prev_backup.date() + timedelta(days=self.period)
        return datetime.combine(next_date, self.time)

    def update(self, **kwargs):
        self.period = kwargs.pop('day')
        Schedule.update(self, **kwargs)


class WeeklySchedule(Schedule):
    def __init__(self, **kwargs):
        self.days = self._convert_days(kwargs.pop('days'))
        Schedule.__init__(self, **kwargs)

    def _convert_days(self, days):
        d = []
        for i in range(7):
            if days & 1 << i:
                d.append(i)
        return d

    def get_next(self):
        today = date.today()
        today_index = today.isoweekday() % 7
        curr_week = filter(lambda x, t=today_index: x >= t, self.days)
        if self.prev_backup and self.prev_backup.date() == today:
            curr_week = curr_week[1:]
        next_day = curr_week[0] if curr_week else self.days[0] + 7
        next_date = today + timedelta(days=next_day-today_index)
        return datetime.combine(next_date, self.time)

    def update(self, **kwargs):
        self.days = self._convert_days(kwargs.pop('days'))
        Schedule.update(self, **kwargs)


class MonthlySchedule(Schedule):
    def __init__(self, **kwargs):
        self.day = kwargs.pop('day')
        Schedule.__init__(self, **kwargs)

    def get_next(self):
        today = date.today()
        month = today.month if self.day >= today.day else today.month + 1
        year = today.year if month >= today.month else today.year + 1
        if self.day >= 29:
            day = min(calendar.monthrange(year, month)[1], self.day)
        else:
            day = self.day
        return datetime(year, month, day, self.time.hour, self.time.minute)

    def update(self, **kwargs):
        self.day = kwargs.pop('day')
        Schedule.update(self, **kwargs)
