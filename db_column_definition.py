class DbColumnDefinition(object):
    RESERVED = {"user", "object"}

    def __init__(self, name: str,
                 data_type: str,
                 nullable: bool = False,
                 is_primary_key: bool = False,
                 is_unique: bool = False,
                 default=None,
                 length=0
                 ):
        self.name = name
        if name in DbColumnDefinition.RESERVED:
            self.name = f"\"{name}\""
        self.data_type = data_type.upper()
        self.data_type = self.data_type
        self.nullable = nullable
        self.is_primary_key = is_primary_key
        self.is_unique = is_unique
        self.default = default
        self.length = length

    def copy(self):
        return DbColumnDefinition(name=self.name,
                                  data_type=self.data_type,
                                  nullable=self.nullable,
                                  is_primary_key=self.is_primary_key,
                                  is_unique=self.is_unique,
                                  default=self.default, length=self.length)

    def __str__(self):
        null = "NULL" if self.nullable else "NOT NULL"
        default = f"default {self.default}" if self.default else ""
        data_type = self.data_type
        if self.length:
            data_type = f"{data_type}({self.length})"
        return f"{self.name} {data_type} {default} {null}"

    def __eq__(self, other):
        return str(self).__eq__(str(other))

    def __le__(self, other):
        return str(self).__le__(str(other))

    def __gt__(self, other):
        return str(self).__gt__(str(other))
