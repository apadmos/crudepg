class DbColumnDefinition(object):
    RESERVED = {"user", "object"}

    def __init__(self, name: str,
                 data_type: str,
                 nullable: bool = False,
                 is_primary_key: bool = False,
                 is_unique: bool = False,
                 default=None
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

    def copy(self):
        return DbColumnDefinition(name=self.name,
                                  data_type=self.data_type,
                                  nullable=self.nullable,
                                  is_primary_key=self.is_primary_key,
                                  is_unique=self.is_unique,
                                  default=self.default)

    def __str__(self):
        null = "NULL" if self.nullable else "NOT NULL"
        default = f"default {self.default}" if self.default else ""
        return f"{self.name} {self.data_type} {default} {null}"

    def __eq__(self, other):
        return str(self).__eq__(str(other))

    def __le__(self, other):
        return str(self).__le__(str(other))

    def __gt__(self, other):
        return str(self).__gt__(str(other))


class DbTableDefinition(object):

    def __init__(self, name: str,
                 column_definitions: list[DbColumnDefinition],
                 fk_columns: list = None,
                 schema=None, scripts: list = None, unique_constraints: list = None):
        self.name = name
        self.column_definitions = sorted(column_definitions)
        self.schema = schema
        self.scripts = scripts
        self.fk_columns = fk_columns
        self.unique_constraints = unique_constraints

    def get_primary_key_columns(self):
        return filter(lambda f: f.is_primary_key, self.column_definitions)

    def get_unique_columns(self):
        return filter(lambda f: f.is_unique, self.column_definitions)

    def __str__(self):
        if self.schema:
            return f"{self.schema}.{self.name}"
        return self.name

    def get_column(self, column: str):
        for c in self.column_definitions:
            if c.name == column or c == column or f"\"{c.name}\"" == column:
                return c
        return None
