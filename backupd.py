#!/usr/bin/python
import platform
from uuid import uuid1
from httplib import HTTPSConnection
from urllib import urlencode

from daemon.runner import DaemonRunner

from config.base import Config


class Api():
    BASE_URL = '/api/'
    HEADERS = {'Content-type': 'application/x-www-form-urlencoded',
               'Accept': 'text/plain'}
    
    def __init__(self, host, port, uuid, key):
        self.conn = HTTPSConnection(host, port)
        self.base_params = {'uuid': uuid, 'key': key}
    
    def encode_data(self, data):
        data.update(self.base_params)
        return urlencode(data)
    
    def hi(self, uname):
        self.conn.request('POST',
                          Api.BASE_URL + 'hi/',
                          self.encode_data({'host': uname[1],
                                            'uname': ' '.join(uname)}),
                          Api.HEADERS)
        res = self.conn.getresponse()
        self.conn.close()
        return res


class App():
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path = '/dev/tty'
        self.pidfile_path =  '/tmp/backup.pid'
        self.pidfile_timeout = 5
        self.config = Config('/etc/bitcalm/bitcalm.conf')
        self.key, self.is_new_client = App._get_key()
        self.api = Api('localhost', 8443, self.config.uuid, self.key)
    
    @staticmethod
    def _get_key():
        key_path = '/var/lib/bitcalm/key'
        with open(key_path) as f:
            key = f.read()
        if key:
            created = False
        else:
            key = str(uuid1())
            with open(key_path, 'w') as f:
                f.write(key)
            created = True
        return (key, created)
    
    def run(self):
        if self.is_new_client:
            res = self.api.hi(platform.uname())
            print 'Server says "%s"' % res.read()


daemon_runner = DaemonRunner(App())
daemon_runner.do_action()
