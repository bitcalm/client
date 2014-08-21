#! /usr/bin/env python2.7
import os
import sys
import signal
import pickle
import time
import platform
from hashlib import sha256 as sha
from lockfile.pidlockfile import PIDLockFile
from datetime import datetime, date
from logging import FileHandler
from threading import Thread

from daemon import DaemonContext

import backup
import log
from config import status as client_status
from api import api
from filesystem.base import FSNode, Watcher
from actions import ActionPool, OneTimeAction, Action, ActionSeed


IGNORE_PATHS = ('sys', 'dev', 'root', 'cdrom', 'boot',
                'lost+found', 'proc', 'tmp', 'sbin', 'bin')

MIN = 60
HOUR = 60 * MIN

FS_UPLOAD_PERIOD = 30 * MIN
FS_SET_PERIOD = 24 * HOUR
LOG_UPLOAD_PERIOD = 5 * MIN
SCHEDULE_UPDATE_PERIOD = HOUR
RESTORE_CHECK_PERIOD = 10 * MIN
PIDFILE_PATH = '/var/run/bitcalmd.pid'
CRASH_PATH = '/var/log/bitcalm.crash'


fs_watcher = None
actions = ActionPool()


def on_stop(signum, frame):
    if fs_watcher:
        fs_watcher.stop()
    log.info('Terminated process with pid %i' % os.getpid())
    raise SystemExit()


def set_fs():
    log.info('Update filesystem image')
    basepath = '/'
    root = FSNode(basepath, ignore=IGNORE_PATHS)
    root_dict = root.as_dict()
    root_dump = pickle.dumps(root_dict)
    h = sha(root_dump).hexdigest()
    if not client_status.fshash or client_status.fshash != h:
        status = api.set_fs(root_dump)[0]
        if status == 200:
            client_status.fshash = h
            client_status.save()
            log.info('Filesystem image updated')
            return True
        else:
            log.error('Filesystem image update failed')
            return False
    log.info('Filesystem has not changed')
    return True


def upload_fs(changelog):
    if not changelog:
        return True
    current = list(changelog)
    status = api.update_fs(current)[0]
    if status == 200:
        del changelog[:len(current)]
        return True
    return False


def upload_log(entries=log.upload):
    if not entries:
        return True
    current = list(entries)
    status = api.upload_log(entries)[0]
    if status == 200:
        del entries[:len(current)]
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
    """ Returns:
            0 if update failed;
            1 if files are updated;
            2 if files not changed.
    """
    status, content = api.get_files()
    if status == 200:
        client_status.files_hash = sha(content).hexdigest()
        client_status.files = content.split('\n')
        client_status.save()
        fs_watcher.set_paths(client_status.files)
        if not fs_watcher.notifier.is_alive():
            log.info('Start watching filesystem')
            fs_watcher.start()
        return 1
    elif status == 304:
        return 2
    return 0


def restore():
    status, content = api.check_restore()
    if status == 200:
        if content:
            log.info('Start backup restore.')
            complete = []
            for item in content:
                error = backup.restore(item['key'], paths=item.get('items'))
                if error:
                    log.error(error)
                    break
                else:
                    complete.append(item['id'])
            else:
                log.info('All restore tasks are complete.')
            if complete:
                api.restore_complete(complete)
        return True
    else:
        return False


def compress_backup():
    try:
        backup.compress(client_status.backup['path'])
    except IOError:
        log.error('There is not enough free space on device')
        os.remove(client_status.backup['path'])
        space=backup.available_space()
        backup_action = actions.get(make_backup)
        actions.remove(backup_action)
        new = [OneTimeAction(nexttime=30*MIN,
                             func=lambda s=space: backup.available_space() > s,
                             tag='check_free_space',
                             followers=[backup_action],
                             cancel=['files_changed']),
               OneTimeAction(nexttime=30*MIN,
                             func=lambda: update_files() == 1,
                             tag='files_changed',
                             followers=[backup_action],
                             cancel=['check_free_space'])]
        actions.extend(new)
        return False
    client_status.backup['status'] = 'compressed'
    client_status.save()
    return True

def prepare_backup_upload():
    api.set_backup_info('upload', backup_id=client_status.backup['backup_id'])
    client_status.backup['status'] = 'upload'
    client_status.save()
    return True

def upload_backup():
    key, size = backup.upload(client_status.backup['path'])
    client_status.backup['status'] = 'uploaded'
    client_status.backup['time'] = time.time()
    client_status.backup['keyname'] = key
    client_status.backup['size'] = size
    client_status.save()
    return True

def complete_backup():
    del client_status.backup['status']
    del client_status.backup['path']
    api.set_backup_info('complete', **client_status.backup)
    client_status.backup = None
    client_status.prev_backup = date.today().strftime('%Y.%m.%d')
    client_status.save()
    return True

def make_backup():
    steps = [compress_backup,
             prepare_backup_upload,
             upload_backup,
             complete_backup]
    bstatus = client_status.backup and client_status.backup.get('status')
    if not bstatus:
        if not update_files() or not client_status.files:
            return False
        if not client_status.amazon:
            status, content = api.get_s3_access()
            if status == 200:
                client_status.amazon = content
                client_status.save()
            else:
                log.error('Getting S3 access failed')
                return False
    
        status, backup_id = api.set_backup_info('compress',
                                                time=time.time(),
                                                files='\n'.join(client_status.files))
        if not status == 200:
            return False
        tmp = '/tmp/backup_%s.tar.gz' % datetime.utcnow().strftime('%Y.%m.%d_%H%M')
        client_status.backup = {'backup_id': backup_id,
                                'path': tmp,
                                'status': 'compress'}
        client_status.save()
    else:
        status_map = {s: i for i, s in enumerate(('compress',
                                                  'compressed',
                                                  'upload',
                                                  'uploaded'))}
        steps = steps[status_map.get(bstatus):]
    for step in steps:
        if not step():
            return False
    return True


def immortal(func):
    def inner():
        def restart():
            t = Thread(target=func)
            t.setDaemon(True)
            t.start()
            return t
        t = restart()
        while True:
            t.join(2**31)
            if not t.is_alive():
                log.error('Unhandled exception, restarting')
                t = restart()
    return inner

@immortal
def work():
    if os.path.exists(CRASH_PATH):
        crash = os.stat(CRASH_PATH)
        if crash.st_size:
            with open(CRASH_PATH) as f:
                crash_info = f.read()
            status = api.report_crash(crash_info, crash.st_mtime)
            if status == 200:
                log.info('Crash reported')
                os.remove(CRASH_PATH)
    
    if client_status.backup:
        status = client_status.backup['status']
        if os.path.exists(client_status.backup['path']):
            if status == 'compress':
                os.remove(client_status.backup['path'])
                client_status.backup = None
                client_status.save()
            elif status == 'uploaded':
                os.remove(client_status.backup['path'])
        else:
            client_status.backup = None
            client_status.save()

    set_fs()

    global fs_watcher
    if not fs_watcher:
        log.info('Create watch manager')
        fs_watcher = Watcher()
    
    if update_files() == 2 and client_status.files:
        fs_watcher.set_paths(client_status.files)
        if not fs_watcher.notifier.is_alive():
            log.info('Start watching filesystem')
            fs_watcher.start()

    actions.extend([Action(FS_UPLOAD_PERIOD, upload_fs, fs_watcher.changelog),
                    Action(LOG_UPLOAD_PERIOD, upload_log),
                    Action(RESTORE_CHECK_PERIOD, restore),
                    Action(FS_SET_PERIOD, set_fs)])

    def on_schedule_update():
        b = actions.get(make_backup)
        if b:
            b.next()

    followers = [ActionSeed(backup.next_date, make_backup),
                 ActionSeed(SCHEDULE_UPDATE_PERIOD,
                            update_schedule,
                            on_update=on_schedule_update)]
    if update_schedule() or client_status.schedule:
        actions.extend([f.grow() for f in followers])
    else:
        actions.add(OneTimeAction(SCHEDULE_UPDATE_PERIOD,
                                  update_schedule,
                                  followers=followers))
    log.info('Start main loop')
    while True:
        action = actions.next()
        log.info('Next action is %s' % action)
        time.sleep(action.time_left())
        action()


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
                            signal_map={signal.SIGTERM: on_stop},
                            stderr=open(CRASH_PATH, 'w'))
    context.files_preserve = map(lambda h: h.stream,
                                 filter(lambda h: isinstance(h, FileHandler),
                                        log.logger.handlers))
    print 'Starting daemon'
    with context:
        log.info('Daemon started')
        work()


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
    exit('Usage: %s start|stop|restart' % os.path.basename(sys.argv[0]))


def main():
    if sys.version_info < (2, 7):
        exit('Please upgrade your python to 2.7 or newer')
    if len(sys.argv) != 2:
        usage()
    actions = {'start': run,
               'stop': stop,
               'restart': restart}
    func = actions.get(sys.argv[1], usage)
    func()


if __name__ == '__main__':
    main()
