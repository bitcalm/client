#!/usr/bin/python
import pickle
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
    SETTINGS_PATH = '/var/lib/bitcalm/data'
    
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/null'
        self.stderr_path = '/dev/null'
        self.pidfile_path =  '/tmp/backup.pid'
        self.pidfile_timeout = 5
        self.config = Config('/etc/bitcalm/bitcalm.conf')
        self.load_settings()
        self.api = Api('localhost', 8443, self.config.uuid, self.key)

    def load_settings(self):
        with open(App.SETTINGS_PATH, 'r') as f:
            try:
                data = pickle.load(f)
            except EOFError:
                data = {'key': str(uuid1()), 'registered': False}
                with open(App.SETTINGS_PATH, 'w') as f:
                    pickle.dump(data, f)
        self.key = data['key']
        self.is_registered = data['registered']
    
    def save_settings(self):
        with open(App.SETTINGS_PATH, 'w') as f:
            pickle.dump({'key': self.key, 'registered': self.is_registered}, f)
    
    def run(self):
        pass

app = App()

if not app.is_registered:
    print 'Sending info about new client...'
    res = app.api.hi(platform.uname())
    print res.read()
    if res.status == 200:
        app.is_registered = True
        app.save_settings()
    else:
        exit('Aborted')

daemon_runner = DaemonRunner(App())
daemon_runner.do_action()
