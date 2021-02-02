# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-10 14:40:50
# @Last Modified time: 2021-02-02 12:26:14
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-


import pymysql

from pydbapi.db import DBCommon, DBFileExec
from pydbapi.sql import SqlCompile
from pydbapi.conf import AUTO_RULES


import logging
import logging.config
from pydbapi.conf import LOGGING_CONFIG
logging.config.dictConfig(LOGGING_CONFIG)
mysqllogger = logging.getLogger('mysql')


class SqlMysqlCompile(SqlCompile):
    '''[summary]

    [description]
        构造mysql sql
    Extends:
        SqlCompile
    '''

    def __init__(self, tablename):
        super(SqlMysqlCompile, self).__init__(tablename)

    def create(self, columns, indexes):
        'mysql 暂不考虑索引'
        sql = self.create_nonindex(columns)
        # if indexes and not isinstance(indexes, list):
        #     raise TypeError(f"indexes must be a list !")
        # if indexes:
        #     indexes = ','.join(indexes)
        #     sql = f"{sql.replace(';', '')}interleaved sortkey({indexes});"
        return sql


class MysqlDB(DBCommon, DBFileExec):

    def __init__(self, host, user, password, database, port=3306, charset="utf8", safe_rule=True):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset
        super(MysqlDB, self).__init__()
        self.auto_rules = AUTO_RULES if safe_rule else None

    def get_conn(self):
        conn = pymysql.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port, charset=self.charset)
        if not conn:
            self.get_conn()
        return conn

    def create(self, tablename, columns, indexes=None):
        # tablename = f"{self.database}.{tablename}"
        sqlcompile = SqlMysqlCompile(tablename)
        sql_for_create = sqlcompile.create(columns, indexes)
        rows, action, result = self.execute(sql_for_create)
        return rows, action, result
