#!/usr/bin/python
import os
import json
import pickle
import platform
from uuid import uuid1
from hashlib import sha512 as sha
from threading import Timer

from daemon.runner import DaemonRunner
from pyinotify import (WatchManager, ThreadedNotifier, 
                       IN_CREATE, IN_DELETE, IN_MOVED_FROM, IN_MOVED_TO)

from config import Config
from filesystem import FSNode
from filesystem.base import FSEvent
from api import Api


IGNORE_PATHS = ('sys', 'dev', 'root', 'cdrom', 'boot',
                'lost+found', 'proc', 'tmp', 'sbin', 'bin')
UPLOAD_PERIOD = 1800


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
        self.changelog = []
        self.loop = None

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
    
    def start_loop(self):
        if self.loop and self.loop.is_alive():
            self.loop.cancel()
        self.loop = Timer(UPLOAD_PERIOD, self.upload_fs)
        self.loop.start()
    
    def upload_fs(self):
        if self.changelog:
            current = list(self.changelog)
            status, content = self.api.update_fs(current)
            if status == 200:
                del self.changelog[:len(current)]
        self.start_loop()
            

    def run(self):
        basepath = '/'
        root = FSNode(basepath, ignore=IGNORE_PATHS)
        root_d = root.as_dict()
        root_str = json.dumps(root_d)
        h = sha(root_str).hexdigest()
        if not self.fshash or self.fshash != h:
            status, content = self.api.set_fs(root_str)
            if status == 200:
                self.fshash = h
                self.save_settings()

        wm = WatchManager()
        notifier = ThreadedNotifier(wm, FSEvent(changelog=self.changelog))
        notifier.start()
        mask = IN_CREATE|IN_DELETE|IN_MOVED_FROM|IN_MOVED_TO
        for item in os.listdir(basepath):
            path = os.path.join(basepath, item)
            if item in IGNORE_PATHS or os.path.islink(path):
                continue
            wm.add_watch(path, mask, rec=True)
        self.start_loop()


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

daemon_runner = DaemonRunner(app)
daemon_runner.do_action()
