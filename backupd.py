#!/usr/bin/python
import os
import sys
import signal
import json
import time
import platform
from hashlib import sha256 as sha
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
SCHEDULE_UPDATE_PERIOD = 3600
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
        else:
            self.delay()
    
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
    
    def delay(self, period=600):
        self.time = datetime.now() + timedelta(seconds=period)
    
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
    status = api.update_fs(current)[0]
    if status == 200:
        del changelog[:len(current)]
        return True
    return False

def update_schedule(on_update=None, on_404=False):
    status, content = api.get_schedule()
    if status == 200:
        content['time'] = (int(content['time'][:2]),
                           int(content['time'][2:]))
        client_status.schedule = content
        client_status.save()
        if on_update:
            on_update()
        return True
    return {304: True, 404: on_404}.get(status, False)

def update_files():
    status, content = api.get_files()
    if status == 200:
        client_status.files_hash = sha(content).hexdigest()
        client_status.files = content.split('\n')
        client_status.save()
        return True
    elif status == 304:
        return True
    return False

def make_backup():
    if not update_files() or not client_status.files:
        return False
    status, backup_id = api.set_backup_info('compress', time=time.time())
    if not status == 200:
        return False
    backup.compress()
    kwargs = {'backup_id': backup_id}
    api.set_backup_info('upload', **kwargs)
    key, size = backup.upload()
    client_status.prev_backup = date.today().strftime('%Y.%m.%d')
    client_status.save()
    kwargs['time'] = time.time()
    kwargs['keyname'] = key
    kwargs['size'] = size
    api.set_backup_info('complete', **kwargs)
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
        
        status, content = api.get_s3_access()
        if status == 200:
            client_status.amazon = content
        
        actions = [Action(UPLOAD_PERIOD,
                          upload_fs,
                          changelog),]
        
        backup_action = lambda: Action(backup.get_next(), make_backup)
        
        def on_schedule_update(actions=actions):
            actions[-1] = backup_action()
        
        update_schedule_action = lambda: Action(SCHEDULE_UPDATE_PERIOD,
                                                update_schedule,
                                                on_update=on_schedule_update)

        if update_schedule() or client_status.schedule:
            actions.append(update_schedule_action())
            actions.append(backup_action())
        else:
            def on_schedule_download(actions=actions):
                actions[-1] = update_schedule_action()
                actions.append(backup_action())
            actions.append(Action(SCHEDULE_UPDATE_PERIOD,
                                  update_schedule,
                                  on_update=on_schedule_download,
                                  on_404=True))
        
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
