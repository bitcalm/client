import unittest

from bitcalm.config.base import DB_RE


class DBConfigTest(unittest.TestCase):
    def setUp(self):
        self.valid = []
        for h in ('localhost', '127.0.0.1', 'example.com'):
            for port in ('3306', ''):
                if port:
                    h = ':'.join((h,port))
                for passwd in ('passw0rd', ''):
                    self.valid.append(';'.join(filter(None,
                                                      (h, 'user', passwd))))
        self.invalid = ('localhost:user;password',
                        'localhost:3306:password',
                        'localhost;user;')

    def runTest(self):
        for item in self.valid:
            self.assertTrue(DB_RE.match(item), '%s did not match' % item)
        for item in self.invalid:
            self.assertFalse(DB_RE.match(item))


if __name__ == '__main__':
    unittest.main()
