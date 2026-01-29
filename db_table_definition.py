class DbColumnDefinition(object):
    TYPE_MAPPING = {
        "INT4": "INT",
        "INTEGER": "INT",
        "BOOLEAN": "BOOL",
    }

    RESERVED = {"user", "object"}

    def __init__(self, name: str,
                 data_type: str,
                 nullable: bool = False,
                 is_primary_key: bool = False,
                 is_unique: bool = False,
                 length: int = 0):
        self.name = name
        if name in DbColumnDefinition.RESERVED:
            self.name = f"\"{name}\""
        self.data_type = data_type.upper()
        self.data_type = self.TYPE_MAPPING.get(self.data_type) or self.data_type
        self.nullable = nullable
        self.is_primary_key = is_primary_key
        self.is_unique = is_unique
        self.length = length

    def __str__(self):
        null = "NULL" if self.nullable else "NOT NULL"
        type = f"{self.data_type}({self.length})" if self.length else self.data_type
        return f"{self.name} {type} {null}"

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
                 schema=None, scripts: list = None):
        self.name = name
        self.column_definitions = sorted(column_definitions)
        self.schema = schema
        self.scripts = scripts
        self.fk_columns = fk_columns

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
