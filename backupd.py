#!/usr/bin/python
import os
import sys
import signal
import json
import time
import platform
from hashlib import sha512 as sha
from lockfile.pidlockfile import PIDLockFile

from daemon import DaemonContext
from pyinotify import (WatchManager, ThreadedNotifier, 
                       IN_CREATE, IN_DELETE, IN_MOVED_FROM, IN_MOVED_TO)

from config import status as client_status
from api import api
from filesystem.base import FSEvent, FSNode


IGNORE_PATHS = ('sys', 'dev', 'root', 'cdrom', 'boot',
                'lost+found', 'proc', 'tmp', 'sbin', 'bin')
UPLOAD_PERIOD = 1800
PIDFILE_PATH = '/tmp/bitcalm.pid'


notifier = None

def on_stop(signum, frame):
    global notifier
    if notifier:
        notifier.stop()
    raise SystemExit('Terminated process with pid %i' % os.getpid())

def upload_fs(changelog):
    if not changelog:
        return
    current = list(changelog)
    status, content = api.update_fs(current)
    if status == 200:
        del changelog[:len(current)]

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
            client_status.files = content.pop('files')
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
        while True:
            time.sleep(UPLOAD_PERIOD)
            upload_fs(changelog)

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
