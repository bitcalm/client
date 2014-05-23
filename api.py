import json
from random import random
from httplib import HTTPSConnection
from hashlib import sha512 as sha
from urllib import urlencode

from config import config, status


class Api(object):
    BOUNDARY = '-' * 20 + sha(str(random())).hexdigest()[:20]
    
    def __init__(self, host, port, uuid, key):
        self.conn = HTTPSConnection(host, port)
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
        return self._send('fs/set', files={'fs': fs})
    
    def update_fs(self, changes):
        return self._send('fs/update', files={'changes': json.dumps(changes)})
    
    def get_settings(self):
        status, content = self._send('settings', method='GET')
        if status == 200:
            content = json.loads(content)
        return status, content
    
    def set_backup_info(self, status, backup_id=None, **kwargs):
        data = {k: v for k, v in kwargs.iteritems() if k in ('time', 'size')}
        if backup_id:
            data['id'] = backup_id
        s, c = self._send('backup/%s' % status, data)
        if not backup_id and s == 200:
            c = int(c)
        return s, c


api = Api('localhost', 8443, config.uuid, status.key)
