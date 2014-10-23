DAY = 60 * 60 * 24


def total_seconds(td):
    return td.days * DAY + td.seconds + td.microseconds / 10.0 ** 6
