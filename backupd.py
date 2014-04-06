#!/usr/bin/python
from httplib import HTTPSConnection
from daemon.runner import DaemonRunner


class Api():
    BASE_URL = '/api/'
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
    
    def hi(self):
        conn = HTTPSConnection(self.host, self.port)
        conn.request('GET', Api.BASE_URL + 'hi/')
        res = conn.getresponse()
        conn.close()
        return res.read()


class App():
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path = '/dev/tty'
        self.pidfile_path =  '/tmp/backup.pid'
        self.pidfile_timeout = 5
        self.api = Api('localhost', 8443)
    
    def run(self):
        print 'Server says "%s"' % self.api.hi()


daemon_runner = DaemonRunner(App())
daemon_runner.do_action()
