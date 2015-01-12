#! /usr/bin/env python2.7
import os
import re
import sys
import errno
import signal
import pickle
import time
import urllib2
import tarfile
import itertools
import subprocess
from lockfile.pidlockfile import PIDLockFile
from datetime import datetime, timedelta
from logging import FileHandler
from threading import Thread

from daemon import DaemonContext
from mysql.connector import errors as mysql_errors

import log
import backup
import bitcalm
from bitcalm.utils import total_seconds
from bitcalm.const import KB, MIN, HOUR, DAY
from config import config, status as client_status
from api import api
from filesystem.utils import levelwalk, iterfiles, modified
from actions import ActionPool, OneTimeAction, Action, StepAction, ActionSeed
from schedule import DailySchedule, WeeklySchedule, MonthlySchedule
from database import (EXCLUDE_DB,
                      DEFAULT_DB_PORT,
                      get_databases,
                      dump_db,
                      connection_error)

MAX_CRASH_SIZE = KB
FS_SET_PERIOD = DAY
LOG_UPLOAD_PERIOD = 5 * MIN
CHANGES_CHECK_PERIOD = 10 * MIN
DB_CHECK_PERIOD = DAY
PIDFILE_PATH = '/var/run/bitcalmd.pid'
CRASH_PATH = '/var/log/bitcalm.crash'


actions = ActionPool()


def on_stop(signum, frame):
    log.info('Terminated process with pid %i' % os.getpid())
    raise SystemExit()


def set_fs(depth=-1, step_time=2*MIN, top='/', action='start', start=None):
    till = datetime.utcnow() + timedelta(seconds=step_time)
    for level, has_next in levelwalk(depth=depth, top=top, start=start):
        status = api.update_fs([level], action, has_next=has_next)
        depth -= 1
        if status == 200:
            if has_next:
                client_status.upload_dirs = [[p[:2] for p in level if p[1]],
                                             depth]
            else:
                client_status.upload_dirs = []
                client_status.last_fs_upload = datetime.utcnow()
            client_status.save()
        else:
            return 0
        if datetime.utcnow() > till and has_next:
            return -1
        action = 'append'
    return 1

def update_fs(depth=-1, step_time=2*MIN):
    if client_status.upload_dirs:
        kwargs = {'action': 'append'}
        kwargs['start'], depth = client_status.upload_dirs
    else:
        kwargs = {}
    return set_fs(depth=depth, step_time=step_time, **kwargs)


def upload_log(entries=log.upload):
    if not entries:
        return True
    current = list(entries)
    status = api.upload_log(entries)[0]
    if status == 200:
        del entries[:len(current)]
        return True
    return False


def tail_log(last=10*KB, path=log.LOG_PATH):
    if not os.path.exists(path):
        return []
    log_size = os.stat(path).st_size
    if not log_size:
        return []
    with open(log.LOG_PATH) as f:
        if log_size > last:
            f.seek(log_size - last)
        data = f.read()
    data = data.split('\n')
    if len(data) > 1:
        data = data[1:]
    return data


def get_s3_access():
    status, content = api.get_s3_access()
    if status == 200 and content:
        client_status.amazon = content
        return True
    log.error('Getting S3 access failed')
    return False


def check_db():
    if not (config.database or client_status.database):
        return True
    databases = {}
    errors = []
    for db in itertools.chain(config.database, client_status.database):
        try:
            db_names = get_databases(**db)
        except mysql_errors.Error as err:
            errors.append((db['host'],
                           db.get('port', DEFAULT_DB_PORT),
                           err.errno))
            continue
        if db_names:
            databases['%s:%i' % (db['host'], db['port'])] = ';'.join(db_names)
    if errors:
        api.report_db_errors(errors)
    if databases:
        return api.set_databases(databases) == 200
    return True


def update(url):
    if url.startswith('/'):
        url = 'https://%s%s' % (config.host, url)
    filename = os.path.join('/tmp', os.path.basename(url))
    with open(filename, 'wb') as f:
        f.write(urllib2.urlopen(url).read())
    try:
        import pip
    except ImportError:
        dst = '/tmp/bitcalm'
        tar = tarfile.open(filename)
        tar.extractall(dst)
        tar.close()
        subprocess.check_call(('python', 'setup.py', 'install'), cwd=dst)
    else:
        pip.main(['uninstall', '-qy', 'bitcalm'])
        pip.main(['install', '-q', filename])
    subprocess.Popen(('bitcalm', 'restart'))
    return True


def uninstall(verbose=True):
    if verbose:
        msg = 'Uninstall BitCalm? (yes/no): '
        ans = raw_input(msg)
        while not re.match(r'^([Yy]es|[Nn]o)$', ans):
            ans = raw_input(msg)
        if ans.lower() == 'no':
            return
    subprocess.Popen(('uninstall_bitcalm', '--yes'))


def check_update():
    status, url = api.check_version()
    if status in (200, 304):
        client_status.last_ver_check = datetime.now()
        client_status.save()
        if status == 200:
            log.info('Start client update')
            update(url)
        return True
    return False


def check_changes():
    status, content = api.get_changes()
    if status == 200:
        if content.get('uninstall'):
            uninstall(verbose=False)
            stop()
        version = content.get('version')
        if version:
            ver, url = version
            if ver != bitcalm.__version__:
                actions.add(OneTimeAction(0, update, url))
                log.info('Planned update to %s' % ver)
                return True

        access = content.get('access')
        if access:
            client_status.amazon = access

        dbases = content.get('db')
        if dbases:
            client_status.database = dbases
            db_test = ((db, connection_error(**db)) for db in dbases)
            err_db = filter(lambda db: db[1], db_test)
            if err_db:
                for db in err_db:
                    dbases.remove(db[0])
                err_db = [(db['host'],
                           db.get('port', 3306),
                           err) for db, err in err_db]
                api.report_db_errors(err_db)
            if dbases and not actions.has(check_db):
                actions.add(Action(DB_CHECK_PERIOD, check_db, start=0))

        schedules = content.get('schedules')
        if schedules:
            types = {'daily': DailySchedule,
                     'weekly': WeeklySchedule,
                     'monthly': MonthlySchedule}
            curr = {}
            for s in client_status.schedules:
                curr[s.id] = s
            for s in schedules:
                if 'db' in s:
                    db = pickle.loads(s['db'])
                    user_db = lambda db: db not in EXCLUDE_DB
                    for dbases in db.itervalues():
                        dbases[:] = filter(user_db, dbases)
                    s['db'] = db
                cs = curr.get(s['id'])
                if cs:
                    if isinstance(cs, types[s['type']]):
                        cs.update(**s)
                    else:
                        ns = types[s.pop('type')](**s)
                        ns.prev_backup = cs.prev_backup
                        ns.exclude = cs.exclude
                        client_status.schedules.remove(cs)
                        client_status.schedules.append(ns)
                else:
                    ns = types[s.pop('type')](**s)
                    client_status.schedules.append(ns)
            b = actions.get(make_backup)
            if b:
                b.next()
            else:
                actions.add(Action(backup.next_date, make_backup))
        client_status.save()
        tasks = content.get('restore')
        if tasks:
            actions.add(OneTimeAction(30, restore, tasks))
        if content.get('log_tail', False):
            actions.add(OneTimeAction(0, upload_log, entries=tail_log()))
        if content.get('send_fs', False):
            fs_action = actions.get(update_fs)
            if fs_action:
                fs_action.delay(period=0)
            else:
                actions.add(StepAction(FS_SET_PERIOD, update_fs, start=0))
        return True
    elif status == 304:
        return True
    return False


def restore(tasks):
    log.info('Start backup restore.')
    complete = []
    for item in tasks:
        error = backup.restore(item['backup_id'])
        if error:
            log.error(error)
            break
        else:
            complete.append(item['id'])
    else:
        log.info('All restore tasks are complete.')
    if complete:
        api.restore_complete(complete)
        del tasks[:len(complete)]
    return not tasks


def make_backup():
    schedule = backup.next_schedule()
    if not client_status.backup:
        status, backup_id = api.set_backup_info('prepare',
                                                time=time.time(),
                                                schedule=schedule.id)
        if status != 200:
            return False
        client_status.backup = {'backup_id': backup_id, 'status': 0}
        client_status.save()
    else:
        backup_id = client_status.backup['backup_id']
    bstatus = client_status.backup
    if schedule.files and bstatus['status'] < 2:
        schedule.clean_files()
        if bstatus['status'] == 0:
            status, content = api.set_backup_info(
                                'filesystem',
                                backup_id=backup_id,
                                has_info=bool(client_status.backupdb.count()))
            if status == 200:
                bstatus['is_full'] = content['is_full']
                if bstatus['is_full']:
                    client_status.backupdb.clean()
                elif 'prev' in content:
                    backup.get_database(int(content['prev']))
            else:
                return False
            bstatus['status'] = 1
            client_status.save()
        if bstatus.get('items') is None:
            bstatus['items'] = {'dirs': filter(os.path.isdir,
                                               schedule.files),
                                'files': filter(os.path.isfile,
                                                schedule.files)}
            client_status.save()

        files = iterfiles(files=bstatus['items']['files'],
                          dirs=bstatus['items']['dirs'])
        if not bstatus['is_full']:
            files = modified(files, client_status.backupdb)

        with backup.BackupHandler(backup_id) as handler:
            for filename in files:
                try:
                    info = os.stat(filename)
                except OSError:
                    continue
                result = handler.upload_file(filename)
                row = (filename,
                       1,
                       info.st_mtime,
                       info.st_size,
                       info.st_mode,
                       info.st_uid,
                       info.st_gid,
                       int(result[1]),
                       backup_id)
                client_status.backupdb.add((row,))
                client_status.save()
                if handler.files_count >= 100:
                    handler.upload_stats()

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
        db_success = 0
        db_total = len(bstatus['databases'])
        with backup.BackupHandler(backup_id) as handler:
            while bstatus['databases']:
                host, port, name = bstatus['databases'].pop()
                try:
                    user, passwd = db_creds[make_key(host, port)]
                except KeyError:
                    log.error('There are no credentials for %s:%i' % (host, port))
                    client_status.save()
                    continue
                ts = datetime.utcnow().strftime('%Y.%m.%d_%H%M')
                path = '/tmp/%s_%i_%s_%s.sql.gz' % (host, port, name, ts)
                if not dump_db(name, host, user,
                               path=path, passwd=passwd, port=port):
                    log.error('Dump of %s from %s:%i failed' % (name, host, port))
                    client_status.save()
                    continue
                handler.upload_db(path)
                handler.upload_stats()
                client_status.save()
                db_success += 1
        if db_success != db_total:
            log.error('%i of %i databases was backuped' % (db_success, db_total))

    bstatus['status'] = 3
    client_status.save()
    api.set_backup_info('complete',
                        backup_id=backup_id,
                        time=time.time())
    client_status.backup = None
    backup.next_schedule().done()
    client_status.save()
    return True


def get_crash():
    if not os.path.exists(CRASH_PATH):
        return '', 0
    crash = os.stat(CRASH_PATH)
    if not crash.st_size:
        return '', 0
    with open(CRASH_PATH) as f:
        is_big = crash.st_size > MAX_CRASH_SIZE
        if is_big:
            f.seek(crash.st_size - MAX_CRASH_SIZE)
        data = f.read()
    mtime = crash.st_mtime
    if is_big:
        with open(CRASH_PATH, 'w') as f:
            f.write(data)
    return data, mtime


def report_crash():
    info, when = get_crash()
    if not info:
        return True
    try:
        status = api.report_crash(info, when)
    except Exception, e:
        log.error('Crash report failed: %s' % type(e).__name__)
        success = status = False
    else:
        success = status == 200
    if success:
        log.info('Crash reported')
        os.remove(CRASH_PATH)
    elif status:
        log.error('Crash report failed with status %i' % status)
    return success


class EmergencyWorker(object):
    """ Send last 10 KB of log and waits for commands.
        Possible commands are:
            - send log;
            - update/rollback;
            - run main thread immediately;
            - do not run main thread until command received;
            - try to run main thread hourly.
    """
    ITERPERIOD = 5*MIN

    def __call__(self):
        self.upload_log()
        brake = False
        work_till = datetime.utcnow() + timedelta(seconds=HOUR)
        while brake or datetime.utcnow() < work_till:
            iterstart = datetime.utcnow()
            status, commands = api.emergency()
            if status == 200:
                if 'update' in commands:
                    update(commands['update'])
                else:
                    worker = commands.get('worker', 0)
                    # possible worker values are:
                    #     -1 - do not run main thread
                    #     0 - try to run main thread (default)
                    #     1 - run main thread immediately
                    if not -1 <= worker <= 1:
                        self.log('wrong worker status (%i)' % worker)
                    elif worker == 1:
                        return
                    else:
                        brake = bool(worker)
                    if commands.get('log', False):
                        self.upload_log()
            else:
                self.log('commands receiving failed with status %i' % status)
            itertime = (datetime.utcnow() - iterstart).total_seconds()
            if itertime < self.ITERPERIOD:
                time.sleep(self.ITERPERIOD - itertime)

    def log(self, msg):
        log.info('Emergency: %s' % msg)

    def upload_log(self):
        self.log('trying to upload log')
        success = upload_log(entries=tail_log())
        self.log('log uploaded' if success else 'log upload failed')
        return success


class Observer(object):
    """ Runs observed function in a separate thread.
        If function works less than a minute:
            - after 3 crashes it restarting with a one minute interval;
            - after 10 crashes runs the emergency worker.
    """
    def __init__(self, func):
        self.worker = func
        self.errors = 0
        self.worker_started = None

    def __call__(self):
        t = self.start_worker()
        while True:
            t.join(2**31)
            if not t.is_alive():
                worktime = self.get_work_period()
                c = self.get_thread(func=report_crash)
                c.join(2**31)
                if worktime < MIN:
                    self.errors += 1
                    if self.errors >= 10:
                        log.info('Start emergency thread')
                        t = self.get_thread(EmergencyWorker())
                        t.join(2**31)
                        log.info('Emergency thread stopped')
                    elif self.errors >= 3:
                        time.sleep(MIN)
                else:
                    self.errors = 0
                t = self.start_worker()

    def get_thread(self, func, start=True):
        t = Thread(target=func)
        t.setDaemon(True)
        if start:
            t.start()
        return t

    def get_work_period(self):
        if not self.worker_started:
            return 0
        return (datetime.utcnow() - self.worker_started).total_seconds()

    def start_worker(self):
        self.worker_started = datetime.utcnow()
        log.info('Starting worker thread')
        return self.get_thread(func=self.worker)


def work():
    if not client_status.is_actual_version():
        check_update()

    actions.clear()
    if client_status.last_fs_upload:
        nxt = client_status.last_fs_upload + timedelta(seconds=FS_SET_PERIOD)
        till_next = max(0, total_seconds(nxt - datetime.utcnow()))
        del nxt
    else:
        till_next = 0
    actions.add(StepAction(FS_SET_PERIOD, update_fs, start=till_next))
    actions.extend([Action(LOG_UPLOAD_PERIOD, upload_log),
                    Action(CHANGES_CHECK_PERIOD, check_changes)])

    if config.database or client_status.database:
        actions.add(Action(DB_CHECK_PERIOD, check_db, start=7*MIN))
    
    if client_status.amazon:
        actions.add(Action(backup.next_date, make_backup))
    else:
        actions.add(OneTimeAction(5*MIN,
                                  get_s3_access,
                                  followers=[ActionSeed(backup.next_date,
                                                        make_backup)]))

    if os.path.exists(CRASH_PATH) and os.stat(CRASH_PATH).st_size > 0:
        actions.add(OneTimeAction(10*MIN, report_crash, start=0))

    log.info('Action pool is:\n%s' % '\n'.join(map(str, actions)))
    log.info('Start main loop')
    while True:
        action = actions.next()
        log.info('Next action is %s' % action)
        time.sleep(action.time_left())
        action()


def run():
    log.info('Checking updates')
    url = None
    if client_status.is_registered:
        status, url = api.check_version()
    else:
        status, content = api.get_version()
        if status == 200 and bitcalm.__version__ != content[0]:
            url = content[1]
    if status != 500:
        client_status.last_ver_check = datetime.now()
        client_status.save()
    if url:
        log.info('Start client update')
        update(url)
        exit()
        
    print 'Sending info about the client...'
    status, content = api.hi()
    print content
    if not client_status.is_registered:
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
        Observer(work)()


def get_pid():
    if os.path.exists(PIDFILE_PATH):
        with open(PIDFILE_PATH, 'r') as f:
            pid = f.read().strip()
        try:
            pid = int(pid)
        except ValueError:
            os.remove(PIDFILE_PATH)
            return None
        else:
            return pid
    else:
        return None


def start():
    pid = get_pid()
    if pid:
        try:
            os.kill(pid, 0)
        except OSError, e:
            running = e.errno == errno.EPERM
        else:
            running = True
        if running:
            exit('Bitcalm is running, pid %i' % pid)
        else:
            os.remove(PIDFILE_PATH)
    run()


def stop(verbose=True):
    pid = get_pid()
    if not pid:
        if verbose:
            print 'Bitcalm is not running'
        return True
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError, e:
        if verbose:
            print 'Failed to terminate %(pid)i: %(e)s' % vars()
        return False
    return True


def restart():
    if stop():
        run()


def usage(actions):
    return 'Usage: %s %s' % (os.path.basename(sys.argv[0]), '|'.join(actions))


def main():
    if sys.version_info < (2, 6):
        exit('Please upgrade your python to 2.6 or newer')
    actions = {'start': start,
               'stop': stop,
               'restart': restart,
               'uninstall': uninstall}
    if len(sys.argv) != 2:
        exit(usage(actions.keys()))
    func = actions.get(sys.argv[1])
    if not func:
        exit(usage(actions.keys()))
    func()


if __name__ == '__main__':
    main()
