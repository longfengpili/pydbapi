# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-10 14:40:50
# @Last Modified time: 2021-02-02 12:26:46
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-


import psycopg2

from pydbapi.db import DBCommon, DBFileExec
from pydbapi.sql import SqlCompile
from pydbapi.conf import REDSHIFT_AUTO_RULES


import logging
import logging.config
from pydbapi.conf import LOGGING_CONFIG
logging.config.dictConfig(LOGGING_CONFIG)
redlogger = logging.getLogger('redshift')


class SqlRedshiftCompile(SqlCompile):
    '''[summary]

    [description]
        构造redshift sql
    Extends:
        SqlCompile
    '''

    def __init__(self, tablename):
        super(SqlRedshiftCompile, self).__init__(tablename)

    def create(self, columns, indexes):
        sql = self.create_nonindex(columns)
        if indexes and not isinstance(indexes, list):
            raise TypeError(f"indexes must be a list !")
        if indexes:
            indexes = ','.join(indexes)
            sql = f"{sql.replace(';', '')}\ninterleaved sortkey({indexes});"
        return sql


class RedshiftDB(DBCommon, DBFileExec):

    def __init__(self, host, user, password, database, port='5439', safe_rule=True):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        super(RedshiftDB, self).__init__()
        self.auto_rules = REDSHIFT_AUTO_RULES if safe_rule else None

    def get_conn(self):
        conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
        if not conn:
            self.get_conn()
        return conn

    def create(self, tablename, columns, indexes=None):
        # tablename = f"{self.database}.{tablename}"
        sqlcompile = SqlRedshiftCompile(tablename)
        sql_for_create = sqlcompile.create(columns, indexes)
        rows, action, result = self.execute(sql_for_create)
        return rows, action, result
