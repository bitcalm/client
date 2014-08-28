import os
import subprocess
import itertools

import MySQLdb
from _mysql_exceptions import OperationalError

from bitcalm import log
from bitcalm.config import config, status


DEFAULT_DB_PORT = 3306


def get_connection(host, user, passwd='', port=3306):
    try:
        return MySQLdb.connect(host=host, port=port, user=user, passwd=passwd)
    except OperationalError, e:
        log.error(e[1])
        return None


def get_databases(host, user, passwd='', port=3306):
    conn = get_connection(host, user, passwd, port)
    if not conn:
        return None
    cur = conn.cursor()
    cur.execute('SHOW databases')
    db_names = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return db_names


def is_database_exists(name, host, user, passwd='', port=3306):
    return name in get_databases(host, user, passwd, port)


def create_database(name, host, user, passwd='', port=3306):
    conn = get_connection(host, user, passwd, port)
    if not conn:
        return False
    cur = conn.cursor()
    cur.execute('CREATE DATABASE %s' % name)
    cur.close()
    conn.close()
    return True


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
