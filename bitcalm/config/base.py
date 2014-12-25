import os
import re
import pickle
import sqlite3
from uuid import uuid1
from datetime import datetime, timedelta

from .exceptions import ConfigEntryError, ConfigSyntaxError


DB_RE = re.compile('^((?:[\.\w]+)|(?:(?:\d{1,3}\.){3}\d{1,3}))(?::(\d+))?;(\w+)(?:;(\w+))?$')


class Config:
    DEFAULT_CONF = '/etc/bitcalm.conf'
    COMMENT_SYMBOL = '#'
    REQUIRED = ('uuid',)
    ALLOWED = ('uuid', 'host', 'port', 'database', 'https')
    VALIDATOR = {'uuid': re.compile('^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$'),
                 'database': DB_RE}
    ENTRY = {'host': {'default': 'bitcalm.com'},
             'port': {'default': 443, 'type': int},
             'https': {'default': 1, 'type': int},
             'database': {'default': [], 'multiple': True}}
    
    @staticmethod
    def validate(entry, value):
        if entry not in Config.ALLOWED:
            raise ConfigEntryError(entry, 'Disallowed entry: %s' % entry)
        validator = Config.VALIDATOR.get(entry)
        if validator and not validator.match(value):
            raise ConfigEntryError(entry, 'Wrong %s: %s' % (entry, value))
    
    @classmethod
    def get_default(cls, entry):
        data = cls.ENTRY.get(entry)
        return data.get('default') if data else None
    
    @classmethod
    def get_type(cls, entry):
        data = cls.ENTRY.get(entry)
        return data.get('type') if data else None
    
    @classmethod
    def is_multiple(cls, entry):
        data = cls.ENTRY.get(entry)
        return data and data.get('multiple', False)
    
    def __init__(self, filename=DEFAULT_CONF):
        conf = self._parse_config(filename)
        for entry in Config.REQUIRED:
            if entry not in conf:
                raise ConfigEntryError(entry, 'There is no %s in config file (%s)' % (entry, filename))
        for entry in Config.ALLOWED:
            value = conf.get(entry, Config.get_default(entry))
            conv_type = Config.get_type(entry)
            if conv_type:
                value = conv_type(value)
            setattr(self, entry, value)
        self.filename = filename
        if self.database:
            for i, db in enumerate(self.database):
                db = DB_RE.match(db)
                db = {'host': db.group(1),
                      'port': int(db.group(2) or 3306),
                      'user': db.group(3),
                      'passwd': db.group(4) or ''}
                self.database[i] = db
    
    def _parse_config(self, filename):
        with open(filename, 'r') as f:
            lines = f.readlines()
        config = {}
        for i, line in enumerate(lines, 1):
            line = line.split(Config.COMMENT_SYMBOL)[0]
            if not line:
                continue
            line = line.split('=')
            if len(line) != 2:
                raise ConfigSyntaxError('Invalid config syntax at line %i' % i)
            entry, value = [s.strip() for s in line]
            if not value:
                raise ConfigSyntaxError('Invalid config syntax at line %i' % i)
            Config.validate(entry, value)
            if Config.is_multiple(entry):
                if not entry in config:
                    config[entry] = []
                config[entry].append(value)
            else:
                config[entry] = value
        return config


class Status(object):
    OPTIONS = ('key',
               'is_registered',
               'schedules',
               'database',
               'backup',
               'amazon',
               'last_ver_check',
               'upload_dirs',
               'last_fs_upload')
    DEFAULT = {'schedules': [],
               'database': [],
               'upload_dirs': []}
    
    def __init__(self, path, **kwargs):
        self.path = path
        with open(self.path, 'r') as f:
            data = pickle.load(f)
            if 'key' not in data:
                data['key'] = kwargs.get('key', str(uuid1()))
                with open(self.path, 'w') as f:
                    pickle.dump(data, f)
        for option in Status.OPTIONS:
            setattr(self,
                    option,
                    data.get(option, kwargs.get(option) \
                                        or Status.DEFAULT.get(option)))
        self.backupdb = BackupData('/var/lib/bitcalm/backup.db')

    def get_files(self):
        files = []
        for s in self.schedules:
            if s.files:
                files.extend(s.files)
        return set(files)
    
    def has_files(self):
        for s in self.schedules:
            if s.files:
                return True
        return False

    def is_actual_version(self):
        if not self.last_ver_check:
            return False
        return self.last_ver_check + timedelta(minutes=10) > datetime.now()

    def save(self):
        data = {}
        for opt in Status.OPTIONS:
            data[opt] = getattr(self, opt, None)
        with open(self.path, 'w') as f:
            pickle.dump(data, f)


def connect(func):
    def inner(self, *args, **kwargs):
        conn, cur = self._connect()
        kwargs['conn'] = conn
        kwargs['cur'] = cur
        result = func(self, *args, **kwargs)
        cur.close()
        conn.close()
        return result
    return inner


class BackupData(object):

    class QUERY:
        _TABLE_NAME = 'backup'
        _COLUMNS = ('path TEXT PRIMARY KEY',
                    'hash_key INTEGER default 0',
                    'mtime FLOAT',
                    'size INTEGER',
                    'mode INTEGER',
                    'uid INTEGER',
                    'gid INTEGER',
                    'compress INTEGER default 1', # was compressed while performing backup
                    'backup_id INTEGER')
        _BACKUP_LIMIT = """ WHERE backup_id <= ?"""
        DROP = """DROP TABLE IF EXISTS %s""" % _TABLE_NAME
        CREATE = """CREATE TABLE %s (%s)""" % (_TABLE_NAME,
                                               ', '.join(_COLUMNS))
        GET_ROW = """SELECT mtime, size FROM %s WHERE path=?""" % _TABLE_NAME
        INSERT = """INSERT OR REPLACE INTO %s (%s) VALUES(%s)"""
        INSERT = INSERT % (_TABLE_NAME,
                           ', '.join([c.split(' ', 1)[0] for c in _COLUMNS]),
                           ','.join('?'*len(_COLUMNS)))
        COUNT = """SELECT COUNT(*) FROM %s""" % _TABLE_NAME
        COUNT_BACKUP = COUNT + _BACKUP_LIMIT
        FILES_ALL = """SELECT path, backup_id, hash_key, compress FROM backup"""
        FILES = FILES_ALL + _BACKUP_LIMIT

    def __init__(self, dbpath):
        self.db = dbpath
        if not os.path.exists(self.db):
            self.clean()
        else:
            conn, cur = self._connect()
            for n in (1, 7):
                query = """ALTER TABLE %s ADD COLUMN %s""" \
                            % (self.QUERY._TABLE_NAME, self.QUERY._COLUMNS[n])
                try:
                    cur.execute(query)
                except sqlite3.OperationalError:
                    pass
            cur.close()
            conn.close()

    def _connect(self):
        conn = sqlite3.connect(self.db)
        conn.text_factory = str
        return conn, conn.cursor()

    @connect
    def clean(self, conn, cur):
        cur.execute(self.QUERY.DROP)
        cur.execute(self.QUERY.CREATE)
        conn.commit()

    @connect
    def get(self, path, conn, cur):
        cur.execute(self.QUERY.GET_ROW, (path,))
        row = cur.fetchone()
        return row

    def get_mtime(self, path):
        row = self.get(path)
        return row[0] if row else 0

    def get_size(self, path):
        row = self.get(path)
        return row[1] if row else None

    @connect
    def add(self, rows, conn, cur):
        if len(rows) > 1:
            cur.executemany(self.QUERY.INSERT, rows)
        else:
            cur.execute(self.QUERY.INSERT, rows[0])
        conn.commit()

    def files(self, backup_id=None, iterator=False, **kwargs):
        args = (self.QUERY.FILES,
                (backup_id,)) if backup_id else (self.QUERY.FILES_ALL,)
        if iterator:
            if self.count(backup_id=backup_id):
                return self._iterfiles(args)
            return []
        return self._listfiles(args)

    @connect
    def _listfiles(self, args, **kwargs):
        cur = kwargs['cur']
        cur.execute(*args)
        return cur.fetchall()

    def _iterfiles(self, args, **kwargs):
        conn, cur = self._connect()
        for row in cur.execute(*args):
            yield row
        cur.close()
        conn.close()

    @connect
    def count(self, conn, cur, backup_id=None):
        if backup_id:
            args = (self.QUERY.COUNT_BACKUP, (backup_id,))
        else:
            args = (self.QUERY.COUNT,)
        cur.execute(*args)
        return cur.fetchone()[0]
