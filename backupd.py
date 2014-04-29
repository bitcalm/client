#!/usr/bin/python
import json
import pickle
import platform
from random import random
from uuid import uuid1
from httplib import HTTPSConnection
from urllib import urlencode
from hashlib import sha512 as sha

from daemon.runner import DaemonRunner

from config import Config
from filesystem import FSNode


class Api(object):
    BOUNDARY = '-' * 20 + sha(str(random())).hexdigest()[:20]
    
    def __init__(self, host, port, uuid, key):
        self.conn = HTTPSConnection(host, port)
        self.base_params = {'uuid': uuid, 'key': key}
    
    def _send(self, path, data={}, files={}, method='POST'):
        data.update(self.base_params)
        headers = {'Accept': 'text/plain'}
        if files:
            body = self.encode_multipart_data(data, files)
            headers['Content-type'] = 'multipart/form-data; boundary=%s' % Api.BOUNDARY
        else:
            body = urlencode(data)
            headers['Content-type'] = 'application/x-www-form-urlencoded'
        self.conn.request(method,
                          '/api/%s/' % path,
                          body,
                          headers)
        response = self.conn.getresponse()
        result = (response.status, response.read())
        self.conn.close()
        return result
    
    def encode_multipart_data(self, data={}, files={}):
        """ Returns multipart/form-data encoded data
        """
        data.update(self.base_params)
        boundary = '--' + Api.BOUNDARY
        crlf = '\r\n'
        
        data_tpl = crlf.join((boundary,
                                'Content-Disposition: form-data; name="%(name)s"',
                                '',
                                '%(value)s'))

        file_tpl = crlf.join((boundary,
                                'Content-Disposition: form-data; name="%(name)s"; filename="%(name)s"',
                                'Content-Type: application/octet-stream',
                                '',
                                '%(value)s'))
        
        def render(tpl, data):
            return [tpl % {'name': key,
                           'value': value} for key, value in data.iteritems()]
        
        result = render(data_tpl, data)
        if files:
            result.extend(render(file_tpl, files))
        result.append('%s--\r\n' % boundary)
        return crlf.join(result)
    
    def hi(self, uname):
        return self._send('hi', {'host': uname[1], 'uname': ' '.join(uname)})
    
    def set_fs(self, fs):
        return self._send('set_fs', files={'fs': fs})


class App(object):
    SETTINGS_PATH = '/var/lib/bitcalm/data'
    
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/null'
        self.stderr_path = '/dev/null'
        self.pidfile_path =  '/tmp/backup.pid'
        self.pidfile_timeout = 5
        self.config = Config('/etc/bitcalm/bitcalm.conf')
        self.load_settings()
        self.api = Api('localhost', 8443, self.config.uuid, self.key)

    def load_settings(self):
        with open(App.SETTINGS_PATH, 'r') as f:
            try:
                data = pickle.load(f)
            except EOFError:
                data = {'key': str(uuid1()), 'registered': False}
                with open(App.SETTINGS_PATH, 'w') as f:
                    pickle.dump(data, f)
        self.key = data['key']
        self.is_registered = data['registered']
        self.fshash = data.get('fshash', None)
    
    def save_settings(self):
        with open(App.SETTINGS_PATH, 'w') as f:
            pickle.dump({'key': self.key,
                         'registered': self.is_registered,
                         'fshash': self.fshash}, f)
    
    def run(self):
        root = FSNode('/', ignore=('sys', 'dev', 'root', 'cdrom',
                                   'boot', 'lost+found', 'proc', 'tmp',
                                   'sbin', 'bin'))
        root_d = root.as_dict()
        root_str = json.dumps(root_d)
        h = sha(root_str).hexdigest()
        if not self.fshash or self.fshash != h:
            status, content = self.api.set_fs(root_str)
            if status == 200:
                self.fshash = h
                self.save_settings()

app = App()

if not app.is_registered:
    print 'Sending info about new client...'
    status, content = app.api.hi(platform.uname())
    print content
    if status == 200:
        app.is_registered = True
        app.save_settings()
    else:
        exit('Aborted')

daemon_runner = DaemonRunner(App())
daemon_runner.do_action()
