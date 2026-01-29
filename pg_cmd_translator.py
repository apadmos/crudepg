import json
import re

from .db_table_definition import DbTableDefinition
from .db_cmd import DbCmd


class PgCmdTranslator(object):

    def cmd_str(self, sql_cmd: str, vals: dict = None):

        def replacement(match):
            hit = match[0]
            return f"%({hit[1:]})s"

        sql_cmd = re.sub(r"@\w+", replacement, sql_cmd)

        if vals:
            flattened = json.dumps(vals, default=str)
            parsed = json.loads(flattened)
            return DbCmd(sql_cmd, params=flattened, params_parsed=parsed)

        return DbCmd(sql_cmd)

    def drop_schema(self, schema):
        return self.cmd_str(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")

    def drop_table(self, table, if_exists=True):
        if if_exists:
            return self.cmd_str(f"DROP TABLE IF EXISTS {table};")
        else:
            return self.cmd_str(f"DROP TABLE {table};")

    def pull_column_names(self, columns, join=', '):
        """
		Given a single string of column definitions strip out just the names for use in queries.
		day int NOT NULL, week int NOT NULL, turn into [day, week]
		:param columns:
		:param join:
		:return:
		"""
        names = [c.split(' ')[0].strip() for c in columns.split(',')]
        if join:
            return join.join(names)
        return names

    def create_schema(self, schema):
        return self.cmd_str(f'CREATE SCHEMA {schema};')

    def create_table(self, table: DbTableDefinition):

        columns = ', '.join([str(c) for c in table.column_definitions])

        foreign_keys = map(lambda t: f"""CONSTRAINT fk_{t[0]}_{t[1].name}_{t[2]} FOREIGN KEY({t[0]})
        REFERENCES {t[1]}({t[2]})""", table.fk_columns or [])

        constraints = list(foreign_keys)
        primary_key_columns = list(table.get_primary_key_columns())
        if primary_key_columns:
            pk = ", ".join([c.name for c in primary_key_columns])
            constraints.append(f"CONSTRAINT {table.name}_pk PRIMARY KEY ({pk})")

        unique_columns = list(table.get_unique_columns())
        if unique_columns:
            unique = f"CONSTRAINT {table.name}_unique UNIQUE({','.join([c.name for c in unique_columns])})"
            constraints.append(unique)

        constraints = ", " + ", ".join(constraints) if constraints else ""
        return self.cmd_str(f"CREATE TABLE {table} ({columns}{constraints});")

    def insert(self, table: str, data: dict):
        keys = sorted(data.keys())
        params = ", ".join([f"to_tsvector(%({k})s)" if str(k).startswith('searchable') else f"%({k})s" for k in keys])
        keys = ", ".join(keys)
        return self.cmd_str(f"INSERT INTO {table} ({keys}) VALUES ({params})", data)

    def delete(self, table: str, where_equals: dict):
        keys = [f'{k} = %({k})s' for k in where_equals.keys()]
        keys = " AND ".join(keys)
        return self.cmd_str(f'DELETE FROM {table} WHERE {keys}', where_equals)

    def update(self, table: str, updates: dict, where_equals: dict):
        params = {}
        wheres = []
        ups = []

        self._to_param_dict(updates, 'set', ups, '=', params)
        self._to_param_dict(where_equals, 'where', wheres, '=', params)

        return self.cmd_str(f'UPDATE {table} '
                            f'SET {", ".join(ups)}'
                            f' WHERE {"AND ".join(wheres)}', params)

    def _to_param_dict(self, params: dict, prefix: str, statement_collector: list, separator: str,
                       params_collector: dict):
        if params:
            for k in params.keys():
                if " " in k:
                    parts = k.split()
                    lk = parts[0]
                    modifier = parts[1]
                    if modifier == "tsvector":
                        statement_collector.append(f"{lk} {separator} to_tsvector('english', %({prefix}_{lk})s)")
                    else:
                        raise Exception(f"Unhandled type hint {modifier}")
                    params_collector[f"{prefix}_{lk}"] = params[k]
                else:
                    statement_collector.append(f"{k} {separator} %({prefix}_{k})s")
                    params_collector[f"{prefix}_{k}"] = params[k]

    def read(self, table, equals: dict = None,
             less_than: dict = None, greater_than: dict = None, take: int = None,
             order_by=None, desc=False, column_string="*", skip: int = 0):
        skip = f" OFFSET {skip}" if skip else ""
        take = f" LIMIT {take}" if take else ""
        if order_by:
            """https://stackoverflow.com/questions/1055360/how-to-tell-a-variable-is-iterable-but-not-a-string"""
            if not isinstance(order_by, str):
                order_by = ",".join(order_by)
        order_by = f" ORDER BY {order_by}" if order_by else ""
        desc = " DESC" if desc else " ASC" if order_by else ""

        params = {}
        conditions = []

        self._to_param_dict(equals, 'where', conditions, '=', params)
        self._to_param_dict(greater_than, 'gt', conditions, '>', params)
        self._to_param_dict(less_than, 'lt', conditions, '<', params)

        if conditions:
            j = " AND ".join(conditions)
            where = f" WHERE {j}"
        else:
            where = ""

        cmd = f"SELECT {column_string} FROM {table}{where}{order_by}{desc}{skip}{take};"

        return self.cmd_str(cmd, params)

    def get_table_schema(self, catalog: str, schema_name, table_name):
        sql = """SELECT * From information_schema.columns
                WHERE table_schema = %(schema_name)s
                AND table_catalog =  %(table_catalog)s
                AND table_name = %(table_name)s
                ORDER BY table_name, column_name;"""
        return self.cmd_str(sql, {'schema_name': schema_name,
                                  "table_name": table_name,
                                  "table_catalog": catalog})
