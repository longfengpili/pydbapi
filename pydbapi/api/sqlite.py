# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2023-12-04 12:00:45
# @github: https://github.com/longfengpili


import os
import threading
import sqlite3

from pydbapi.db import DBMixin, DBFileExec
from pydbapi.sql import SqlCompile

import logging
sqlitelogger = logging.getLogger(__name__)


class SqliteCompile(SqlCompile):
    '''[summary]

    [description]
        构造redshift sql
    Extends:
        SqlCompile
    '''
    def __init__(self, tablename):
        super(SqliteCompile, self).__init__(tablename)

    def create(self, columns, indexes):
        'sqlite 暂不考虑索引'
        sql = self.create_nonindex(columns)
        # if indexes and not isinstance(indexes, list):
        #     raise TypeError(f"indexes must be a list !")
        # if indexes:
        #     indexes = ','.join(indexes)
        #     sql = f"{sql.replace(';', '')}interleaved sortkey({indexes});"
        return sql


class SqliteDB(DBMixin, DBFileExec):
    _instance_lock = threading.Lock()

    def __init__(self, database=None):
        self.database = database if database else os.path.join(os.path.expanduser('~'), 'sqlite3_test.db')
        super(SqliteDB, self).__init__()
        self.dbtype = 'sqlite'

    # def __new__(cls, *args, **kwargs):
    #     if not hasattr(SqliteDB, '_instance'):
    #         with SqliteDB._instance_lock:
    #             if not hasattr(SqliteDB, '_instance'):
    #                 SqliteDB._instance = super().__new__(cls)

    #     return SqliteDB._instance

    @classmethod
    def get_instance(cls, *args, **kwargs):
        if not hasattr(SqliteDB, '_instance'):
            with SqliteDB._instance_lock:
                if not hasattr(SqliteDB, '_instance'):
                    SqliteDB._instance = cls(*args, **kwargs)

        return SqliteDB._instance

    def get_conn(self):
        conn = sqlite3.connect(database=self.database)
        sqlitelogger.info(f'connect {self.__class__.__name__}({self.database})')
        if not conn:
            self.get_conn()
        return conn

    def create(self, tablename, columns, indexes=None, verbose=0):
        # tablename = f"{self.database}.{tablename}"
        sqlcompile = SqliteCompile(tablename)
        sql_for_create = sqlcompile.create(columns, indexes)
        rows, action, result = self.execute(sql_for_create, verbose=verbose)
        return rows, action, result
