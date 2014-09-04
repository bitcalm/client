import os
import gzip
import subprocess
import itertools

from bitcalm.config import config, status


DEFAULT_DB_PORT = 3306


def _make_args(util='mysql', **kwargs):
    kwargs['password'] = kwargs.pop('passwd', '')
    name = kwargs.pop('name', '')
    args = [util]
    args.extend(('--%s=%s' % (k, v) for k, v in kwargs.iteritems()))
    if name:
        args.append(name)
    return args


def get_databases(host, user, passwd='', port=3306):
    kwargs = vars()
    kwargs['execute'] = 'show databases;'
    mysql = subprocess.Popen(_make_args(**kwargs),
                             stdout=subprocess.PIPE)
    return mysql.communicate()[0].split('\n')[1:-1]


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
    with gzip.open(path, 'wb') as f:
        f.write(dump.stdout.read())
    return True
