#!/usr/bin/python
import os
import sys
import signal
import json
import time
import platform
from hashlib import sha512 as sha
from lockfile.pidlockfile import PIDLockFile
from datetime import datetime, timedelta, date

from daemon import DaemonContext
from pyinotify import (WatchManager, ThreadedNotifier, 
                       IN_CREATE, IN_DELETE, IN_MOVED_FROM, IN_MOVED_TO)

import backup
from config import status as client_status
from api import api
from filesystem.base import FSEvent, FSNode


IGNORE_PATHS = ('sys', 'dev', 'root', 'cdrom', 'boot',
                'lost+found', 'proc', 'tmp', 'sbin', 'bin')
UPLOAD_PERIOD = 1800
PIDFILE_PATH = '/tmp/bitcalm.pid'


class Action(object):
    def __init__(self, nexttime, func, *args, **kwargs):
        self.lastexectime = None
        self._func = func
        if callable(nexttime):
            self._period = 0
            self._next = nexttime
        else:
            self._period = nexttime
            self._next = self._default_next
        self.next()
        self._args = args
        self._kwargs = kwargs
    
    def __str__(self):
        return '%s at %s' % (self._func, self.time)
    
    def __call__(self):
        self.lastexectime = datetime.now()
        if self._func(*self._args, **self._kwargs):
            self.next()
    
    def __cmp__(self, other):
        if self.time > other.time:
            return 1
        if self.time < other.time:
            return -1
        return 0
    
    def _default_next(self):
        return (self.lastexectime or datetime.now()) \
            + timedelta(seconds=self._period)
    
    def next(self):
        self.time = self._next()
    
    def time_left(self):
        now = datetime.now()
        if self.time > now:
            return (self.time - now).seconds
        return 0


notifier = None

def on_stop(signum, frame):
    global notifier
    if notifier:
        notifier.stop()
    raise SystemExit('Terminated process with pid %i' % os.getpid())

def upload_fs(changelog):
    if not changelog:
        return True
    current = list(changelog)
    status, content = api.update_fs(current)
    if status == 200:
        del changelog[:len(current)]
        return True
    return False

def make_backup():
    status, content = api.set_backup_info('compress', time=time.time())
    if not status == 200:
        return False
    backup_id = content
    backup.compress()
    api.set_backup_info('upload', backup_id=backup_id)
    size = backup.upload()
    client_status.schedule['last'] = date.today().strftime('%Y.%m.%d')
    client_status.save()
    api.set_backup_info('complete',
                        backup_id=backup_id,
                        time=time.time(),
                        size=size)
    return True

def run():
    if not client_status.is_registered:
        print 'Sending info about new client...'
        status, content = api.hi(platform.uname())
        print content
        if status == 200:
            client_status.is_registered = True
            client_status.save()
        else:
            exit('Aborted')

    context = DaemonContext(pidfile=PIDLockFile(PIDFILE_PATH),
                            signal_map={signal.SIGTERM: on_stop})
    print 'Starting daemon'
    with context:
        status, content = api.get_settings()
        if status == 200:
            client_status.files = content.pop('files').split('\n')
            client_status.amazon = content.pop('amazon')
            content['time'] = (int(content['time'][:2]),
                               int(content['time'][2:]))
            client_status.schedule = content
            client_status.save()

        basepath = '/'
        root = FSNode(basepath, ignore=IGNORE_PATHS)
        root_d = root.as_dict()
        root_str = json.dumps(root_d)
        h = sha(root_str).hexdigest()
        if not client_status.fshash or client_status.fshash != h:
            status, content = api.set_fs(root_str)
            if status == 200:
                client_status.fshash = h
                client_status.save()

        wm = WatchManager()
        changelog = []
        global notifier
        notifier = ThreadedNotifier(wm, FSEvent(changelog=changelog))
        notifier.start()
        mask = IN_CREATE|IN_DELETE|IN_MOVED_FROM|IN_MOVED_TO
        for item in os.listdir(basepath):
            path = os.path.join(basepath, item)
            if item in IGNORE_PATHS or os.path.islink(path):
                continue
            wm.add_watch(path, mask, rec=True)

        actions = (Action(UPLOAD_PERIOD,
                          upload_fs,
                          changelog),
                   Action(backup.get_next(),
                          make_backup))
        while True:
            action = min(actions)
            time.sleep(action.time_left())
            action()

def stop():
    with open(PIDFILE_PATH, 'r') as f:
        pid = int(f.read().strip())
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError, e:
        print 'Failed to terminate %(pid)i: %(e)s' % vars()

def restart():
    stop()
    run()

def usage():
    exit('Usage: %s start|stop|restart' % sys.argv[0])

if __name__ == '__main__':
    if len(sys.argv) != 2:
        usage()
    actions = {'start': run,
               'stop': stop,
               'restart': restart}
    func = actions.get(sys.argv[1])
    if not func:
        usage()
    func()
