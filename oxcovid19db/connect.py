import pandas as pd
from typing import Tuple, List
import psycopg2.extras
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

MAX_ATTEMPT_FAIL = 10

class Connect:
    ''' Provides an object with an execute method which queries the database and returns a DataFrame
    The returned DataFrame provides the gid as a tuple, not a list, for hashability'''

    def __init__(self, host: str = 'covid19db.org',
                 port: str = '5432',
                 dbname: str = 'covid19',
                 user: str = 'covid19',
                 password: str = 'covid19'):

        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.dbname = dbname

        self.conn = None
        self.cur = None
        self.open_connection()
        self.cursor()

    def reset_connection(self):
        self.close_connection()
        self.open_connection()
        self.cursor()

    def open_connection(self, attempt: int = MAX_ATTEMPT_FAIL):
        if not self.conn:
            try:
                self.conn = psycopg2.connect(user=self.user, password=self.password, host=self.host,
                                             port=self.port, database=self.dbname, connect_timeout=5)
                self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            except psycopg2.OperationalError as error:
                if attempt > 0:
                    time.sleep(5)
                    self.open_connection(attempt - 1)
                else:
                    raise error
            except (Exception, psycopg2.Error) as error:
                raise error

    def cursor(self):
        if not self.cur or self.cur.closed:
            if not self.conn:
                self.open_connection()
            self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            return self.cur

    def execute(self, query: str, data: str = None, attempt: int = MAX_ATTEMPT_FAIL, return_pandas: bool = True):
        try:
            self.cur.execute(query, data)
            self.conn.commit()
        except (psycopg2.DatabaseError, psycopg2.OperationalError) as error:
            if attempt > 0:
                time.sleep(1)
                self.reset_connection()
                self.execute(query, data, attempt - 1, return_pandas)
            else:
                raise error
        except (Exception, psycopg2.Error) as error:
            raise error
        results = self.cur.fetchall()
        column_list = [desc[0] for desc in self.cur.description]

        if return_pandas:
            df = pd.DataFrame(results, columns=column_list)
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            if 'gid' in df.columns:
                df['gid'] = df['gid'].map(tuple)
            return df
        else:
            return results, column_list

    def close_connection(self):
        if self.conn:
            if self.cur:
                self.cur.close()
            self.conn.close()
        self.conn = None
        self.cur = None
