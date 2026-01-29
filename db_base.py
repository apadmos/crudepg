import psycopg2

from .db_stored_procedure_script import DbStoredProcedureScript
from .db_table_definition import DbTableDefinition, DbColumnDefinition
from .pg_cmd_executor import PgCmdExecutor
from .pg_cmd_translator import PgCmdTranslator


class PostgresDB(object):

    def __init__(self, host, user, password, database):
        self.executor = PgCmdExecutor(host=host, user=user, password=password, database=database)
        self.translator = PgCmdTranslator()
        self.registered_tables = []
        self.registered_scripts = []

    def __enter__(self):
        self.executor.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.executor.dispose()

    def drop_schema(self, schema):
        cmd = self.translator.drop_schema(schema)
        return self.executor.execute_void(cmd)

    def drop_table(self, table: str, if_exists=True):
        cmd = self.translator.drop_table(table, if_exists)
        return self.executor.execute_void(cmd)

    def recreate_schema(self, schema):
        cmd = self.translator.create_schema(schema)
        sc = self.executor.execute_void(cmd)
        self.executor.execute_void(self.translator.cmd_str(f"drop extension if exists pg_trgm;"))
        self.executor.execute_void(self.translator.cmd_str(f"create extension pg_trgm with schema {schema};"))
        return sc

    def create_table(self, table: DbTableDefinition):
        cmd = self.translator.create_table(table=table)
        tr = self.executor.execute_void(cmd)
        for script in table.scripts or []:
            self.executor.execute_void(self.translator.cmd_str(script))
        return tr

    def insert(self, table: str, data: dict):
        cmd = self.translator.insert(table=table, data=data)
        return self.executor.execute_void(cmd)

    def delete(self, table: str, where: dict):
        cmd = self.translator.delete(table=table, where_equals=where)
        return self.executor.execute_void(cmd)

    def update(self, table: str, updates:dict, where: dict):
        cmd = self.translator.update(table=table, updates=updates, where_equals=where)
        return self.executor.execute_void(cmd)

    def select(self, table, where: dict = None,
               less_then: dict = None,
               greater_than: dict = None,
               take=None, order_by=None, desc=False, column_string="*", skip:int=0):
        cmd = self.translator.read(table=table, equals=where,
                                   greater_than=greater_than,
                                   less_than=less_then,
                                   take=take,
                                   skip=skip,
                                   order_by=order_by,
                                   desc=desc,
                                   column_string=column_string)
        return self.executor.execute_reader(cmd)

    def count(self, table, where: dict = None):
        r = self.select(table=table, where=where, column_string="count(*) as count")
        if r:
            val = r[0]["count"]
            return int(val)
        return 0

    def read(self, raw_sql, raw_params:dict = None):
        cmd = self.translator.cmd_str(sql_cmd=raw_sql, vals=raw_params or {})
        return self.executor.execute_reader(cmd)

    def read_first(self, raw_sql, raw_params:dict = None):
        cmd = self.translator.cmd_str(sql_cmd=raw_sql, vals=raw_params or {})
        r = self.executor.execute_reader(cmd)
        return r[0] if r else None

    def void(self, raw_sql, raw_params:dict = None):
        cmd = self.translator.cmd_str(sql_cmd=raw_sql, vals=raw_params or {})
        return self.executor.execute_void(cmd)

    def first(self, table, where: dict = None):
        cmd = self.translator.read(table=table, equals=where, take=1)
        possibles = self.executor.execute_reader(cmd)
        if possibles:
            return possibles[0]
        return None

    def add_column(self, table, column:DbColumnDefinition):
        return self.void(f"""ALTER TABLE {table} ADD {column}""")

    def drop_column(self, table, column:DbColumnDefinition):
        return self.void(f"""ALTER TABLE {table} DROP {column.name}""")

    def alter_column_type(self, table, column:DbColumnDefinition):
        return self.void(f"""ALTER TABLE {table} ALTER {column.name} TYPE {column.data_type} USING {column.name}::{column.data_type}""")

    def alter_column_nullable(self, table, column:DbColumnDefinition):
        if column.nullable:
            return self.void(f"""ALTER TABLE {table} ALTER {column.name} DROP NOT NULL""")
        else:
            return self.void(f"""ALTER TABLE {table} ALTER {column.name} SET NOT NULL""")

    def remove_column(self, table, column:DbColumnDefinition):
        return self.void(f"""ALTER TABLE {table} DROP {column}""")



    def register_table(self, table: DbTableDefinition):
        self.registered_tables.append(table)
        return table

    def register_procedure_script(self, script:DbStoredProcedureScript):
        self.registered_scripts.append(script)
        return script

    def ensure_tables_and_scripts(self, mute=True):
        for t in self.registered_tables:
            try:
                print(self.create_table(t).cmd)
            except psycopg2.errors.DuplicateTable:
                if not mute:
                    print(f"table {t.name} already exists")
        for s in self.registered_scripts:
            self.void(s.script)

    def query_columns_schemas(self, table:DbTableDefinition):
        """Go to the database and get the schema info for the version of this table
        that is already created in the database"""
        cmd = self.translator.get_table_schema(
            catalog=self.executor.database,
            schema_name=table.schema, table_name=table.name)
        columns = []
        for info in self.executor.execute_reader(cmd):
            type = info["udt_name"]
            nulls = info['is_nullable'] == "NO"
            name = info["column_name"]
            length = int(info.get('character_octet_length') or 0)
            length = 0 if length == 1073741824 else length
            columns.append(DbColumnDefinition(
                data_type=type,
                nullable=not nulls,
                name=name,
                length=length
            ))
        return columns

    def resolve_table_differences(self, table:DbTableDefinition, interactive=True):
        database_columns = self.query_columns_schemas(table)
        database_table = DbTableDefinition(name=table.name, column_definitions=database_columns, schema=table.schema)
        differences = []

        def bool_answer(prompt:str):
            if not interactive:
                return True
            a = input(prompt)
            return a and str(a).strip().lower().startswith("y")

        if not database_columns:
            if bool_answer(f"Table {table} not found in DB, attempt to add? (y/n)"):
                self.create_table(table)
        else:
            all_columns = set([x.name for x in database_columns]).union(set([x.name for x in table.column_definitions]))
            for column_name in all_columns:
                local_column = table.get_column(column_name)
                remote_column = database_table.get_column(column_name)

                if local_column and not remote_column:
                    print(f"MISSING {local_column} in database.")
                    if bool_answer(f"Attempt to add column? (y/n)"):
                        self.add_column(table, local_column)

                elif remote_column and not local_column:
                    print(f"Database has extra column {remote_column} in database")
                    if bool_answer(f"Attempt to drop column? (y/n)"):
                        self.drop_column(table, remote_column)
                elif remote_column != local_column:
                    print(f"SCHEMA FOR {local_column} is different from {remote_column}")
                    if bool_answer(f"Attempt to alter column? (y/n)"):
                        if local_column.data_type != remote_column.data_type or local_column.length != remote_column.length:
                            self.alter_column_type(table, local_column)
                        if local_column.nullable != remote_column.nullable:
                            self.alter_column_nullable(table, local_column)

        return differences




