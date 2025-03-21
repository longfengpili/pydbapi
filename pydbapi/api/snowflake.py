# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-11-13 10:27:53
# @github: https://github.com/longfengpili


# import threading
# import snowflake.connector

# from pydbapi.db import DBMixin, DBFileExec
# from pydbapi.sql import SqlCompile
# from pydbapi.conf import AUTO_RULES


# class SqlSnowflakeCompile(SqlCompile):
#     '''[summary]

#     [description]
#         构造mysql sql
#     Extends:
#         SqlCompile
#     '''

#     def __init__(self, tablename):
#         super(SqlSnowflakeCompile, self).__init__(tablename)

#     def create(self, columns, indexes):
#         sql = self.create_nonindex(columns)
#         if indexes and not isinstance(indexes, list):
#             raise TypeError(f"indexes must be a list !")
#         if indexes:
#             indexes = ','.join(indexes)
#             sql = f"{sql.replace(';', '')}\ncluster by ({indexes});"
#         return sql


# class SnowflakeDB(DBMixin, DBFileExec):
#     _instance_lock = threading.Lock()

#     def __init__(self, user, password, account, warehouse, database, schema, safe_rule=True):
#         self.user = user
#         self.password = password
#         self.account = account
#         self.warehouse = warehouse
#         self.database = database
#         self.schema = schema
#         super(SnowflakeDB, self).__init__()
#         self.auto_rules = AUTO_RULES if safe_rule else None
#         self.dbtype = 'snowflake'

#     @classmethod
#     def get_instance(cls, *args, **kwargs):
#         if not hasattr(SnowflakeDB, '_instance'):
#             with SnowflakeDB._instance_lock:
#                 if not hasattr(SnowflakeDB, '_instance'):
#                     SnowflakeDB._instance = SnowflakeDB(*args, **kwargs)

#         return SnowflakeDB._instance

#     def get_conn(self):
#         conn = snowflake.connector.connect(database=self.database, user=self.user, password=self.password,
#                                            account=self.account, warehouse=self.warehouse, schema=self.schema)
#         if not conn:
#             self.get_conn()
#         return conn

#     def create(self, tablename, columns, indexes=None, verbose=0):
#         # tablename = f"{self.database}.{tablename}"
#         sqlcompile = SqlSnowflakeCompile(tablename)
#         sql_for_create = sqlcompile.create(columns, indexes)
#         cursor, action, result = self.execute(sql_for_create, verbose=verbose)
#         return cursor, action, result
