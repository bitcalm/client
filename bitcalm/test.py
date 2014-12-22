import unittest

from bitcalm.utils import COMPRESSED, is_file_compressed


class CompressedTest(unittest.TestCase):
    def setUp(self):
        self.items = {True: COMPRESSED + ('7z.001', '7z.100',
                                          'r01', 'r10',
                                          'z01', 'z10'),
                      False: ('bmp', 'txt', 'ini', 'cnf')}
        for key, value in self.items.iteritems():
            self.items[key] = ['/tmp/test.' + item for item in value]

    def runTest(self):
        mapping = {True: (self.assertTrue, '%s is compressed but do not match'),
                   False: (self.assertFalse, '%s is not compressed but match')}
        for compressed, items in self.items.iteritems():
            for item in items:
                func, msg = mapping[compressed]
                func(is_file_compressed(item), msg % item)

if __name__ == '__main__':
    unittest.main()
