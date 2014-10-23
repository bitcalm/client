import logging


class ListHandler(logging.Handler):
    def __init__(self, upload, *args, **kwargs):
        logging.Handler.__init__(self, *args, **kwargs)
        self.log = upload
    
    def emit(self, record):
        self.log.append(self.format(record))


logger = logging.getLogger('bitcalm')
logger.setLevel(logging.INFO)

fh = logging.FileHandler('/var/log/bitcalm.log')
fmt = logging.Formatter('%(asctime)s %(levelname)s %(message)s',
                        '%Y-%m-%d %H:%M:%S')
fh.setFormatter(fmt)
fh.setLevel(logging.INFO)
logger.addHandler(fh)
del fh

upload = []

lh = ListHandler(upload)
lh.setFormatter(fmt)
lh.setLevel(logging.ERROR)
logger.addHandler(lh)
del lh
del fmt

info = logger.info
error = logger.error
