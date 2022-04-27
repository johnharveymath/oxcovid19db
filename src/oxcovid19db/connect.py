'''
   Copyright 2021 John Harvey

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
'''

import pandas as pd
from typing import Tuple, List
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time

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

        # Open the connection on initialisation
        self.open_connection()
        self.cursor()

    def reset_connection(self):
        """ if execute method fails due to a connection issue, this method is called to reset the connection """
        self.close_connection()
        self.open_connection()
        self.cursor()

    def open_connection(self, attempt: int = MAX_ATTEMPT_FAIL):
        """ Opens the connection and assigns it to self.conn """
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
        """ Create a cursor and assign it to self.cur """
        if not self.cur or self.cur.closed:
            if not self.conn:
                self.open_connection()
            self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            return self.cur

    def execute(self, query: str, data=None, attempt: int = MAX_ATTEMPT_FAIL, return_pandas: bool = True):
        """
        Uses self.cur to execute a query
        data is inserted into the query as with the psycopg2 method Cursor.execute(query, data)
        Documentation is here: https://www.psycopg.org/docs/cursor.html#cursor.execute
        attempt: a non-negative integer which is the number of times the query will be re-attempted
        return_pandas: if True, returns DataFrame, if False, returns the results of the query
        The returned DataFrame provides the gid as a tuple, not a list, for hashability
        """

        # check attempt is a positive integer
        try:
            attempt = int(attempt)
            if attempt < 0:
                raise ValueError('Variable attempt must be a non-negative integer')
        except ValueError:
            raise ValueError('Variable attempt must be a non-negative integer')

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
