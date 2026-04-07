class Digest(dict):

    def __init__(self, *source, **kwargs):
        super(Digest).__init__()
        for arg in source:
            if arg and isinstance(arg, dict):
                for key in arg.keys():
                    self[key] = arg[key]
        for kw in kwargs:
            self[kw] = kwargs[kw]

    def __getattr__(self, attr):
        if attr not in self:
            raise AttributeError("No attribute '{}' in digest".format(attr))
        return self[attr]

    def __setattr__(self, key, value):
        self[key] = value

    def __add__(self, other):
        for k, v in other.items():
            self[k] = v
        return self

    @classmethod
    def from_locals(cls, locals: dict, exclude: list[str] = None, ignore_falsies=True):
        exclude = set(exclude) if exclude else set()
        exclude.add("self")
        d = Digest()
        for k, v in locals.items():
            if k in exclude or (ignore_falsies and not v):
                continue
            d[k] = v
        return d

    @classmethod
    def list_of(cls, items: list):
        return list([Digest(i) for i in items])
