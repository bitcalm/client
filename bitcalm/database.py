import os
import gzip
import subprocess
import itertools

import mysql.connector

from bitcalm.config import config, status


DEFAULT_DB_PORT = 3306


def connection_error(**kwargs):
    try:
        mysql.connector.connect(**kwargs)
    except mysql.connector.errors.Error as err:
        return err.errno
    return 0


def get_cursor(**kwargs):
    kwargs['password'] = kwargs.pop('passwd')
    return MySQLContextManager(**kwargs)


class MySQLContextManager(object):
    def __init__(self, **kwargs):
        self.conn = mysql.connector.connect(**kwargs)
        self.cur = self.conn.cursor()

    def __enter__(self):
        return self.cur

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()
        self.cur.close()


def _make_args(util='mysql', **kwargs):
    kwargs['password'] = kwargs.pop('passwd', '')
    name = kwargs.pop('name', '')
    args = [util]
    args.extend(('--%s=%s' % (k, v) for k, v in kwargs.iteritems()))
    if name:
        args.append(name)
    return args


def get_databases(user, passwd='', host='localhost', port=3306):
    with get_cursor(**vars()) as cur:
        cur.execute('show databases;')
        return [row[0] for row in cur.fetchall()]


def is_database_exists(name, host, user, passwd='', port=3306):
    return name in get_databases(host, user, passwd, port)


def get_credentials(host, port):
    for db in itertools.chain(config.database, status.database):
        if db['host'] == host:
            return db['user'], db['passwd']
    return None


def import_db(dump, user, host='', passwd='', port=None, name=''):
    if not (host and port and name):
        dhost, dport, dname = os.path.basename(dump).split('_', 3)[:3]
        host = host or dhost
        port = port or dport
        name = name or dname
    if not is_database_exists(name, host, user, passwd, port):
        return False
    try:
        subprocess.check_call(('mysql',
                               '-u', user,
                               '-p%s' % passwd,
                               name),
                              stdin=open(dump))
    except subprocess.CalledProcessError:
        return False
    return True


def dump_db(name, host, user, path, passwd='', port=3306):
    kwargs = vars()
    del kwargs['path']
    dump = subprocess.Popen(_make_args(util='mysqldump', **kwargs),
                            stdout=subprocess.PIPE)
    if dump.poll():
        return False
    gz = gzip.open(path, 'wb')
    gz.write(dump.stdout.read())
    gz.close()
    return True
