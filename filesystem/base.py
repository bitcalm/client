import os


class FSNode(object):
    def __init__(self, path, parent=None, ignore=()):
        path = os.path.abspath(path)
        if not parent:
            self._rootdir = os.path.dirname(path)
        self.name = os.path.basename(path) or path
        self.ignore = ignore
        self.is_dir = os.path.isdir(path)
        self.is_file = not self.is_dir
        self._parent = parent
        if self.is_dir:
            self._children = []
    
    def __str__(self):
        return self.name if self.is_file else self.name.rstrip('/') + '/'
    
    def __repr__(self):
        return "FSNode('%s')" % self.abspath()
    
    @property
    def parent(self):
        return self._parent
    
    @property
    def children(self):
        if self.is_file:
            return []
        if not self._children:
            node_path = self.abspath()
            try:
                ls = os.listdir(node_path)
            except OSError:
                pass
            else:
                for f in ls:
                    if f in self.ignore:
                        continue
                    path = os.path.join(node_path, f)
                    if not os.path.islink(path):
                        self._children.append(FSNode(path, parent=self))
        return self._children
    
    @children.deleter
    def children(self):
        self._children = []
    
    def iterdirs(self):
        for c in self.children:
            if c.is_dir:
                yield c

    def dirs(self):
        return tuple(self.iterdirs())
    
    def iterfiles(self):
        for c in self.children:
            if c.is_file:
                yield c
    
    def files(self):
        return tuple(self.iterfiles())
    
    def iterparents(self):
        curr = self
        while (curr._parent):
            yield curr._parent
            curr = curr._parent
    
    def parents(self):
        return tuple(self.iterparents())

    def get_root(self):
        parents = self.parents()
        return parents[-1] if parents else self
    
    def abspath(self):
        parents = (p.name for p in reversed(self.parents()))
        dirname = os.path.join(self.get_root()._rootdir, *parents)
        return os.path.join(dirname, self.name)
    
    def as_dict(self):
        data = {'n': self.name}
        if self.is_dir:
            for key, items in (('f', self.files()), ('d', self.dirs())):
                if items:
                    data[key] = list(item.as_dict() for item in items)
            del self.children
        return data
