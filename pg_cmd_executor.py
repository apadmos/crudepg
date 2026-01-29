import psycopg2

from util import Digest
from .db_cmd import DbCmd


class PgCmdExecutor(object):

    def __init__(self, host='127.0.0.1', user='postgres', password=None, database='postgres'):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.con = None
        self.cur = None

    def connect(self):
        self.dispose()
        self.con = psycopg2.connect(dbname=self.database,
                                    host=self.host,
                                    user=self.user,
                                    password=self.password)
        self.cur = self.con.cursor()

    def dispose(self):
        if self.cur:
            self.cur.close()
            self.cur = None
        if self.con:
            self.con.close()
            self.con = None

    def execute_reader(self, cmd: DbCmd):
        was_con = bool(self.con)
        try:
            if not was_con:
                self.connect()
            if cmd.params_dict:
                self.cur.execute(cmd.cmd, cmd.params_dict)
            else:
                self.cur.execute(cmd.cmd)
            rows = []
            for r in self.cur.fetchall():
                d = Digest()
                for i in range(len(self.cur.description)):
                    col_name = self.cur.description[i].name
                    d[col_name] = r[i]
                rows.append(d)
            return rows
        except Exception as ex:
            print(f"{cmd.cmd} {cmd.params_str}")
            raise ex
        finally:
            if not was_con:
                self.dispose()

    def execute_void(self, cmd: DbCmd):
        was_con = bool(self.con)
        try:
            if not was_con:
                self.connect()
            if cmd.params_dict:
                self.cur.execute(cmd.cmd, cmd.params_dict)
            else:
                self.cur.execute(cmd.cmd)
            self.con.commit()
        except Exception:
            if cmd:
                print(f"!!! Error {cmd.cmd} {cmd.params_str} !!!")
            raise
        finally:
            if not was_con:
                self.dispose()
        return cmd
