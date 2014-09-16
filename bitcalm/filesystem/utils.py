import os


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


def levelwalk(top='/', depth=-1, start=None):
    items = start or [(os.path.dirname(top), [os.path.basename(top)])]
    while items and depth:
        next_items = []
        level = []
        for parent, dirs in items:
            for d in dirs:
                path = os.path.join(parent, d)
                cdirs, cfiles = ls(path)
                if cdirs:
                    next_items.append((path, cdirs))
                level.append((path, cdirs, cfiles))
        depth -= 1
        yield level, bool(next_items and depth)
        items = next_items
