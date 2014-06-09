import logging

logger = logging.getLogger('bitcalm')
logger.setLevel(logging.INFO)

fh = logging.FileHandler('/var/log/bitcalm.log')
fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s',
                                  '%Y-%m-%d %H:%M:%S'))
fh.setLevel(logging.INFO)

logger.addHandler(fh)

del fh

info = logger.info
error = logger.error
