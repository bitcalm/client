#!/usr/bin/python
import os
import signal
import lockfile
import json
import time
import platform
from hashlib import sha512 as sha

from daemon import DaemonContext
from pyinotify import (WatchManager, ThreadedNotifier, 
                       IN_CREATE, IN_DELETE, IN_MOVED_FROM, IN_MOVED_TO)

from config import config, status as client_status
from api import Api
from filesystem.base import FSEvent, FSNode


IGNORE_PATHS = ('sys', 'dev', 'root', 'cdrom', 'boot',
                'lost+found', 'proc', 'tmp', 'sbin', 'bin')
UPLOAD_PERIOD = 1800


api = Api('localhost', 8443, config.uuid, client_status.key)
notifier = None

def stop(signum, frame):
    global notifier
    if notifier:
        notifier.stop()
    raise SystemExit('Terminating')

def upload_fs(changelog):
    if not changelog:
        return
    current = list(changelog)
    status, content = api.update_fs(current)
    if status == 200:
        del changelog[:len(current)]


if __name__ == '__main__':
    if not client_status.is_registered:
        print 'Sending info about new client...'
        status, content = api.hi(platform.uname())
        print content
        if status == 200:
            client_status.is_registered = True
            client_status.save()
        else:
            exit('Aborted')

    context = DaemonContext(pidfile=lockfile.FileLock('/tmp/bitcalm.pid'),
                            signal_map={signal.SIGTERM: stop})
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
