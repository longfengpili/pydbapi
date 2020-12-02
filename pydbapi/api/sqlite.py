# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-03 15:25:44
# @Last Modified time: 2020-12-02 14:52:18
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import os
import re
import sqlite3

from pydbapi.db import DBCommon, DBFileExec
from pydbapi.sql import SqlCompile
from pydbapi.conf import LOG_BASE_PATH

import logging
import logging.config
from pydbapi.conf import LOGGING_CONFIG
logging.config.dictConfig(LOGGING_CONFIG)
sqlitelogger = logging.getLogger('sqlite')


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


class SqliteDB(DBCommon, DBFileExec):

    def __init__(self, database=None):
        self.database = database
        super(SqliteDB, self).__init__()

    def get_conn(self):
        if not self.database:
            self.database = os.path.join(LOG_BASE_PATH, 'sqlite3_test.db')
        conn = sqlite3.connect(database=self.database)
        if not conn:
            self.get_conn()
        return conn

    def create(self, tablename, columns, indexes=None):
        # tablename = f"{self.database}.{tablename}"
        sqlcompile = SqliteCompile(tablename)
        sql_for_create = sqlcompile.create(columns, indexes)
        rows, action, result = self.execute(sql_for_create)
        return rows, action, result
