from .db_column_definition import DbColumnDefinition


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
