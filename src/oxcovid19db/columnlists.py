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

from .connect import Connect


class ColumnLists:
    """
    The class variable __cache_state is a dict holding the list of columns for each table
    Initialising an object with a table_list will use this variable to assign attributes to the object
    Then any extra tables will be collected from the database
    Then the cache is updated
    """
    __cache_state = {}

    def __init__(self, table_list):
        # Assign any column lists already collected
        self.__dict__ = ColumnLists.__cache_state

        # Check if more columns needed
        drops = ['source', 'date', 'country', 'countrycode', 'adm_area_1', 'adm_area_2', 'adm_area_3', 'gid']
        db_conn = Connect()
        for table in table_list:
            if not getattr(self, table, None):
                results, columns = db_conn.execute(f"SELECT * FROM {table} WHERE FALSE;", return_pandas=False)
                columns = [col for col in columns if col not in drops]
                setattr(self, table, columns)

        # Update the state
        ColumnLists.__cache_state = self.__dict__
