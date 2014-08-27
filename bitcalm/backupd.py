#! /usr/bin/env python2.7
import os
import sys
import signal
import pickle
import time
import platform
import itertools
import subprocess
import gzip
from hashlib import sha256 as sha
from lockfile.pidlockfile import PIDLockFile
from datetime import datetime
from logging import FileHandler
from threading import Thread

import MySQLdb
from daemon import DaemonContext

import backup
import log
from config import config, status as client_status
from api import api
from filesystem.base import FSNode, Watcher
from actions import ActionPool, OneTimeAction, Action, ActionSeed
from schedule import DailySchedule, WeeklySchedule, MonthlySchedule
from _mysql_exceptions import OperationalError


IGNORE_PATHS = ('sys', 'dev', 'root', 'cdrom', 'boot',
                'lost+found', 'proc', 'tmp', 'sbin', 'bin')
DEFAULT_DB_PORT = 3306

MIN = 60
HOUR = 60 * MIN

FS_UPLOAD_PERIOD = 30 * MIN
FS_SET_PERIOD = 24 * HOUR
LOG_UPLOAD_PERIOD = 5 * MIN
CHANGES_CHECK_PERIOD = 10 * MIN
DB_CHECK_PERIOD = 24 * HOUR
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


def get_s3_access():
    status, content = api.get_s3_access()
    if status == 200 and content:
        client_status.amazon = content
        return True
    log.error('Getting S3 access failed')
    return False


def get_db_connection(db):
    try:
        return MySQLdb.connect(**db)
    except OperationalError, e:
        log.error("Access denied for user '%s'@'%s' (using password: YES)" % (db['user'], db['host']))
        return None


def check_db():
    if not (config.database or client_status.database):
        return True
    databases = {}
    for db in itertools.chain(config.database, client_status.database):
        conn = get_db_connection(db)
        if not conn:
            continue
        cur = conn.cursor()
        cur.execute('SHOW databases')
        db_names = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        databases['%s:%i' % (db['host'], db['port'])] = ';'.join(db_names)
    return api.set_databases(databases) == 200


def check_changes(on_schedule_update=None):
    status, content = api.get_changes()
    if status == 200:
        to_status = (('access', 'amazon'),
                     ('db', 'database'))
        for key, attr in to_status:
            value = content.get(key)
            if value:
                setattr(client_status, attr or key, value)
        schedules = content.get('schedules')
        if schedules:
            ids = []
            types = {'daily': DailySchedule,
                     'weekly': WeeklySchedule,
                     'monthly': MonthlySchedule}
            for i, s in enumerate(schedules):
                if 'db' in s:
                    s['db'] = pickle.loads(s['db'])
                ids.append(s['id'])
                schedules[i] = types[s.pop('type')](**s)
            client_status.schedules = filter(lambda s: s.id not in ids,
                                             client_status.schedules)
            client_status.schedules.extend(schedules)
            if on_schedule_update:
                on_schedule_update()
        client_status.save()
        tasks = content.get('restore')
        if tasks:
            actions.add(OneTimeAction(30, restore, tasks))
        return True
    elif status == 304:
        return True
    return False


def restore(tasks):
    log.info('Start backup restore.')
    complete = []
    for item in tasks:
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
    return len(tasks) == len(complete)


def make_backup():
    schedule = backup.next_schedule()
    if not client_status.backup:
        status, backup_id = api.set_backup_info('prepare',
                                                time=time.time(),
                                                schedule=schedule.id)
        if not status == 200:
            return False
        client_status.backup = {'backup_id': backup_id,
                                'status': 0,
                                'size': 0}
        client_status.save()
    else:
        backup_id = client_status.backup['backup_id']
    bstatus = client_status.backup
    if schedule.files and bstatus['status'] < 2:
        if bstatus['status'] == 0:
            api.set_backup_info('filesystem', backup_id=backup_id)
            bstatus['status'] = 1
            client_status.save()
        if bstatus.get('items') is None:
            bstatus['items'] = {'dirs': filter(os.path.isdir,
                                               schedule.files),
                                'files': filter(os.path.isfile,
                                                schedule.files)}
            client_status.save()
        key_prefix = 'backup_%i/filesystem' % backup_id
        make_key = lambda f, p=key_prefix: '%s%s.gz' % (p, f)
        while bstatus['items']['files']:
            filename = bstatus['items']['files'].pop()
            bstatus['size'] += backup.backup(make_key(filename), filename)
            client_status.save()
        while bstatus['items']['dirs']:
            for path, dirs, files in os.walk(bstatus['items']['dirs'].pop()):
                for fname in files:
                    fpath = os.path.join(path, fname)
                    bstatus['size'] += backup.backup(make_key(fpath), fpath)
            client_status.save()

    if schedule.databases and bstatus['status'] < 3:
        if bstatus['status'] != 2:
            api.set_backup_info('database', backup_id=backup_id)
            bstatus['status'] = 2
            client_status.save()
        if not bstatus.get('databases'):
            bstatus['databases'] = []
            for host, dbnames in schedule.databases.iteritems():
                if ':' in host:
                    host, port = host.split(':')
                    port = int(port)
                else:
                    port = DEFAULT_DB_PORT
                for name in dbnames:
                    client_status.backup['databases'].append((host, port, name))
                client_status.save()
        db_creds = {}
        make_key = lambda h, p: '%s:%i' % (h, p)
        for db in itertools.chain(config.database, client_status.database):
            key = make_key(db['host'], db.get('port', DEFAULT_DB_PORT))
            db_creds[key] = (db['user'], db['passwd'])
        key_prefix = 'backup_%i/databases/' % backup_id
        db_success = 0
        db_total = len(bstatus['databases'])
        while bstatus['databases']:
            host, port, name = bstatus['databases'].pop()
            try:
                user, passwd = db_creds[make_key(host, port)]
            except KeyError:
                log.error('There are no credentials for %s:%i' % (host, port))
                client_status.save()
                continue
            ts = datetime.utcnow().strftime('%Y.%m.%d_%H%M')
            filename = '%s_%i_%s_%s.sql.gz' % (host, port, name, ts)
            path = '/tmp/' + filename
            dump = subprocess.Popen(('mysqldump',
                                     '-u', user,
                                     '-p%s' % passwd,
                                     name),
                                    stdout=subprocess.PIPE)
            if dump.poll():
                log.error('Dump of %s from %s:%i failed' % (name, host, port))
                client_status.save()
                continue
            with gzip.open(path, 'wb') as f:
                f.write(dump.stdout.read())
            bstatus['size'] += backup.upload(os.path.join(key_prefix,
                                                          filename),
                                             path)
            client_status.save()
            db_success += 1
        if db_success != db_total:
            log.error('%i of %i databases was backuped' % (db_success, db_total))

    bstatus['status'] = 3
    client_status.save()
    api.set_backup_info('complete',
                        backup_id=backup_id,
                        time=time.time(),
                        size=bstatus['size'])
    client_status.backup = None
    backup.next_schedule().done()
    client_status.save()
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

    def update_watcher(files=None):
        if not files:
            files = client_status.get_files()
        global fs_watcher
        if not fs_watcher:
            log.info('Create watch manager')
            fs_watcher = Watcher()
            log.info('Start watching filesystem')
            fs_watcher.start()
        fs_watcher.set_paths(files)
        if not actions.get(upload_fs):
            actions.add(Action(FS_UPLOAD_PERIOD,
                               upload_fs,
                               fs_watcher.changelog))

    def on_schedule_update():
        files = client_status.get_files()
        if files:
            update_watcher(files)
        else:
            global fs_watcher
            if fs_watcher:
                fs_watcher.stop()
                fs_watcher = None
            if actions.get(upload_fs):
                actions.remove(upload_fs)
        b = actions.get(make_backup)
        if b:
            b.next()
        else:
            actions.add(Action(backup.next_date, make_backup))

    actions.extend([Action(LOG_UPLOAD_PERIOD, upload_log),
                    Action(FS_SET_PERIOD, set_fs),
                    Action(CHANGES_CHECK_PERIOD,
                           check_changes,
                           on_schedule_update=on_schedule_update)])

    if config.databases or status.databases:
        actions.add(OneTimeAction(7 * min, check_db,
                                  followers=[ActionSeed(DB_CHECK_PERIOD,
                                                        check_db)]))
    
    if client_status.amazon:
        actions.add(Action(backup.next_date, make_backup))
    else:
        actions.add(OneTimeAction(5*MIN,
                                  get_s3_access,
                                  followers=[ActionSeed(backup.next_date,
                                                        make_backup)]))
    
    if client_status.has_files():
        update_watcher()

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
