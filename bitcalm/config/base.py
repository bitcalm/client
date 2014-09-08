import re
import pickle
from uuid import uuid1

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
               'fshash',
               'schedules',
               'database',
               'backup',
               'amazon')
    DEFAULT = {'schedules': [],
               'database': []}
    
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

    def save(self):
        with open(self.path, 'w') as f:
            data = {opt: getattr(self, opt, None) for opt in Status.OPTIONS}
            pickle.dump(data, f)
