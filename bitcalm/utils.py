from bitcalm.const import DAY, MICROSEC


def total_seconds(td):
    return td.days * DAY + td.seconds + td.microseconds * MICROSEC
