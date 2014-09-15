import os


def ls(path):
    if not os.path.isdir(path):
        return []
    try:
        c = os.listdir(path)
    except OSError:
        return []
    else:
        return [os.path.join(path, item) for item in c]


def children(paths):
    result = []
    for path in paths:
        result.extend(ls(path))
    return result


def levelwalk(top='/', depth=-1):
    if not isinstance(top, list):
        top = [top]
    if len(top) > 1:
        level = lambda p: len(filter(None, p.split('/')))
        lvl = level(top[0])
        for p in top[1:]:
            if level(p) != lvl:
                raise ValueError('Paths must be of one level depth')
    paths = top
    while paths and depth:
        c = children(paths)
        paths = filter(os.path.isdir, c)
        depth -= 1
        yield c, bool(paths and depth)
