import json
import zlib
import pickle
import platform
from random import random
from httplib import HTTPSConnection, HTTPConnection
from hashlib import sha512 as sha
from urllib import urlencode

from config import config, status as client_status
from bitcalm import __version__


def returns_json(func):
    def inner(self, *args, **kwargs):
        status, content = func(self, *args, **kwargs)
        if status == 200:
            content = json.loads(content)
        return status, content
    return inner


class Api(object):
    BOUNDARY = '-' * 20 + sha(str(random())).hexdigest()[:20]

    def __init__(self, host, port, uuid, key):
        conn_cls = HTTPSConnection if config.https else HTTPConnection
        self.conn = conn_cls(host, port)
        self.base_params = {'uuid': uuid, 'key': key}
    
    def _send(self, path, data={}, files={}, method='POST'):
        data.update(self.base_params)
        headers = {'Accept': 'text/plain'}
        url = '/api/%s/' % path
        if files:
            body = self.encode_multipart_data(data, files)
            headers['Content-type'] = 'multipart/form-data; boundary=%s' % Api.BOUNDARY
            method = 'POST'
        else:
            body = urlencode(data)
            headers['Content-type'] = 'application/x-www-form-urlencoded'
        if method == 'GET':
            url = '%s?%s' % (url, body)
            body = None
        self.conn.request(method, url, body, headers)
        response = self.conn.getresponse()
        result = (response.status, response.read())
        self.conn.close()
        return result
    
    def encode_multipart_data(self, data={}, files={}):
        """ Returns multipart/form-data encoded data
        """
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
    
    def hi(self):
        uname = platform.uname()
        return self._send('hi', {'host': uname[1],
                                 'uname': ' '.join(uname),
                                 'v': __version__})
    
    def set_fs(self, fs):
        return self._send('fs/set', files={'fs': zlib.compress(fs, 9)})

    def update_fs(self, levels, action, has_next):
        allowed = ('start', 'append')
        if action not in allowed:
            msg = 'Wrong action: %s. Allowed actions are: %s.' % (action, ', '.join(allowed))
            raise ValueError(msg)
        levels = zlib.compress(pickle.dumps(levels), 9)
        return self._send('fs/%s' % action,
                          data={'wait_more': int(has_next)},
                          files={'levels': levels})[0]
    
    def upload_log(self, entries):
        if len(entries) > 1:
            kwargs = {'files': {'entries': zlib.compress(';'.join(entries), 9)}}
        else:
            kwargs = {'data': {'entries': entries[0]}}
        return self._send('log', **kwargs)

    @returns_json
    def get_schedules(self):
        return self._send('get/schedules', method='GET')

    @returns_json
    def get_changes(self):
        return self._send('changes', method='GET')

    @returns_json
    def get_s3_access(self):
        return self._send('get/access', method='GET')
    
    def set_backup_info(self, status, **kwargs):
        backup_id = kwargs.pop('backup_id', None)
        allowed = ('time', 'size', 'schedule', 'has_info')
        data = {k: v for k, v in kwargs.iteritems() if k in allowed}
        if backup_id:
            data['id'] = backup_id
        s, c = self._send('backup/%s' % status, data)
        if s == 200:
            if not backup_id:
                c = int(c)
            elif status == 'filesystem':
                c = json.loads(c)
        return s, c

    def upload_files_info(self, backup_id, files):
        files = json.dumps(files)
        return self._send('backup/%i/files' % backup_id,
                          data={'files': files})[0]

    @returns_json
    def get_files_info(self, backup_id):
        return self._send('backup/%i/files' % backup_id, method='GET')
    
    def set_databases(self, databases):
        return self._send('databases', data={'db': json.dumps(databases)})[0]

    @returns_json
    def get_db_credentials(self):
        return self._send('get/db', method='GET')

    def report_crash(self, info, when):
        return self._send('crash',
                          data={'time': when},
                          files={'info': zlib.compress(info, 9)})[0]

    @returns_json
    def check_restore(self):
        return self._send('get/restore', method='GET')

    def restore_complete(self, tasks):
        return self._send('backup/restore_complete',
                          data={'tasks': ','.join(map(str, tasks))})[0]

    def check_version(self):
        return self._send('version', data={'v': __version__})

    @returns_json
    def get_version(self):
        return self._send('version/current', method='GET')


api = Api(config.host, config.port, config.uuid, client_status.key)
