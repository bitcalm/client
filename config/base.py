import re

from .exceptions import ConfigEntryError, ConfigSyntaxError


class Config:
    DEFAULT_CONF = '/etc/bitcalm.conf'
    COMMENT_SYMBOL = '#'
    REQUIRED = ('uuid',)
    ALLOWED = ('uuid',)
    VALIDATOR = {'uuid': re.compile('^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$')}
    
    @staticmethod
    def validate(entry, value):
        if entry not in Config.ALLOWED:
            raise ConfigEntryError(entry, 'Disallowed entry: %s' % entry)
        if not Config.VALIDATOR[entry].match(value):
            raise ConfigEntryError(entry, 'Wrong %s: %s' % (entry, value))
    
    def __init__(self, filename=DEFAULT_CONF):
        conf = self._parse_config(filename)
        for entry in Config.REQUIRED:
            if entry not in conf:
                raise ConfigEntryError(entry, 'There is no %s in config file (%s)' % (entry, filename))
        self.uuid = self._parse_config(filename).get('uuid')
        self.filename = filename
    
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
            line[0] = line[0].strip()
            line[1] = line[1].strip()
            if not line[1]:
                raise ConfigSyntaxError('Invalid config syntax at line %i' % i)
            Config.validate(line[0], line[1])
            config[line[0]] = line[1]
        return config
