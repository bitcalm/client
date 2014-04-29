#!/usr/bin/python
import json
import pickle
import platform
from uuid import uuid1
from hashlib import sha512 as sha

from daemon.runner import DaemonRunner

from config import Config
from filesystem import FSNode
from api import Api


class App(object):
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
        self.fshash = data.get('fshash', None)
    
    def save_settings(self):
        with open(App.SETTINGS_PATH, 'w') as f:
            pickle.dump({'key': self.key,
                         'registered': self.is_registered,
                         'fshash': self.fshash}, f)
    
    def run(self):
        root = FSNode('/', ignore=('sys', 'dev', 'root', 'cdrom',
                                   'boot', 'lost+found', 'proc', 'tmp',
                                   'sbin', 'bin'))
        root_d = root.as_dict()
        root_str = json.dumps(root_d)
        h = sha(root_str).hexdigest()
        if not self.fshash or self.fshash != h:
            status, content = self.api.set_fs(root_str)
            if status == 200:
                self.fshash = h
                self.save_settings()

app = App()

if not app.is_registered:
    print 'Sending info about new client...'
    status, content = app.api.hi(platform.uname())
    print content
    if status == 200:
        app.is_registered = True
        app.save_settings()
    else:
        exit('Aborted')

daemon_runner = DaemonRunner(App())
daemon_runner.do_action()
