import os


IGNORE_DIRS = ('sys', 'dev', 'root', 'cdrom', 'boot',
               'lost+found', 'proc', 'tmp', 'sbin', 'bin')


def ls(path):
    if not os.path.isdir(path):
        return (), ()
    try:
        children = os.listdir(path)
    except OSError:
        return (), ()
    dirs = []
    others = []
    for c in children:
        (dirs if os.path.isdir(os.path.join(path, c)) else others).append(c)
    return dirs, others


def islink(parent, name):
    return os.path.islink(os.path.join(parent, name))


def exclude_links(parent, items):
    return [item for item in items if not islink(parent, item)]


def count_links(parent, items):
    links = 0
    for item in items:
        if islink(parent, item):
            links += 1
    return links


def levelwalk(top='/', depth=-1, start=None):
    if not depth:
        raise ValueError('Wrong depth')
    if start:
        items = start
    elif top == '/':
        cdirs, cfiles = ls(top)
        cdirs = [p for p in cdirs if p not in IGNORE_DIRS]
        depth -= 1
        yield ([(top, cdirs, cfiles)],
               bool(count_links(top, cdirs) != len(cdirs) and depth))
        items = [(top, cdirs)]
    else:
        items = [(os.path.dirname(top), [os.path.basename(top)])]
    while items and depth:
        next_items = []
        level = []
        while items:
            parent, dirs = items.pop()
            dirs = exclude_links(parent, dirs)
            for d in dirs:
                path = os.path.join(parent, d)
                cdirs, cfiles = ls(path)
                if not (cdirs or cfiles):
                    continue
                if count_links(parent, cdirs) != len(cdirs):
                    next_items.append((path, cdirs))
                level.append((path, cdirs, cfiles))
        depth -= 1
        yield level, bool(next_items and depth)
        items = next_items
