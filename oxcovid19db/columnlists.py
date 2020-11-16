from .connect import Connect

'''
class ColumnLists:
    class __ColumnLists:
        def __init__(self, table_list):
            column_lists = dict()
            drops = ['source', 'date', 'country', 'countrycode', 'adm_area_1', 'adm_area_2', 'adm_area_3', 'gid']
            db_conn = Connect()
            for table in table_list:
                results, columns = db_conn.execute(f"SELECT * FROM {table} WHERE FALSE;", return_pandas=False)
                columns = [col for col in columns if col not in drops]
                setattr(self, table, columns)
    instance = None
    def __init__(self, table_list):
        if not ColumnLists.instance:
            ColumnLists.instance = ColumnLists.__ColumnLists(table_list)
    def __getattr__(self, name):
        return getattr(self.instance, name)
'''

class ColumnLists:
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
