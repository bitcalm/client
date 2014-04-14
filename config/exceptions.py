class ConfigError(Exception):
    def __init__(self, msg):
        self.message = msg
    
    def __str__(self):
        return self.message


class ConfigSyntaxError(ConfigError):
    pass


class ConfigEntryError(ConfigError):
    def __init__(self, entry, msg):
        super(ConfigEntryError, self).__init__(msg)
        self.entry = entry
