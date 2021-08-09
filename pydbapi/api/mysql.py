# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-10 14:40:50
# @Last Modified time: 2021-08-09 15:45:21
# @github: https://github.com/longfengpili

# !/usr/bin/env python3
# -*- coding:utf-8 -*-


import threading
import pymysql

from pydbapi.db import DBCommon, DBFileExec
from pydbapi.sql import SqlCompile
from pydbapi.conf import AUTO_RULES


import logging
mysqllogger = logging.getLogger(__name__)


class SqlMysqlCompile(SqlCompile):
    '''[summary]

    [description]
        构造mysql sql
    Extends:
        SqlCompile
    '''

    def __init__(self, tablename):
        super(SqlMysqlCompile, self).__init__(tablename)

    def create(self, columns, indexes, index_part=128):
        'mysql 暂不考虑索引'
        sql = self.create_nonindex(columns)

        if indexes and not isinstance(indexes, list):
            raise TypeError(f"indexes must be a list, but got {indexes} !")

        if indexes:
            for index in indexes:
                index_sqlexpr = f"index {index}_index ({index}({index_part}))"
                sql = sql.replace(');', f",\n{index_sqlexpr});")
                continue

        return sql

    def dumpsql(self, columns, dumpfile, fromtable=None, condition=None):
        selectsql = self.select_base(columns, fromtable=fromtable, condition=condition)
        intosql = f'into outfile "{dumpfile}" fields terminated by ",";'
        dumpsql = selectsql.replace(";", intosql)
        return dumpsql

    def loadsql(self, columns, loadfile, intotable=None, fieldterminated=','):
        intotable = intotable or self.tablename
        loadsql = f'''load data infile "{loadfile}" into table {intotable}
                      fields terminated by "{fieldterminated}" ({columns.select_cols});'''
        return loadsql


class MysqlDB(DBCommon, DBFileExec):
    _instance_lock = threading.Lock()

    def __init__(self, host, user, password, database, port=3306, charset="utf8", safe_rule=True):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset
        super(MysqlDB, self).__init__()
        self.auto_rules = AUTO_RULES if safe_rule else None

    @classmethod
    def get_instance(cls, *args, **kwargs):
        if not hasattr(MysqlDB, '_instance'):
            with MysqlDB._instance_lock:
                if not hasattr(MysqlDB, '_instance'):
                    MysqlDB._instance = MysqlDB(*args, **kwargs)

        return MysqlDB._instance

    def get_conn(self):
        conn = pymysql.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port, charset=self.charset)
        if not conn:
            self.get_conn()
        return conn

    def create(self, tablename, columns, indexes=None, verbose=0):
        # tablename = f"{self.database}.{tablename}"
        sqlcompile = SqlMysqlCompile(tablename)
        sql_for_create = sqlcompile.create(columns, indexes)
        rows, action, result = self.execute(sql_for_create, verbose=verbose)
        return rows, action, result

    def dumpdata(self, tablename, columns, dumpfile, condition=None, verbose=0):
        sqlcompile = SqlMysqlCompile(tablename)
        sql_for_dump = sqlcompile.dumpsql(columns, dumpfile, condition=condition)
        rows, action, result = self.execute(sql_for_dump, verbose=verbose)
        mysqllogger.info(f"【{action}】{tablename} dumpdata {rows} rows succeed, outfile: {dumpfile} !")
        return rows, action, result

    def loaddata(self, tablename, columns, loadfile, fieldterminated=',', verbose=0):
        sqlcompile = SqlMysqlCompile(tablename)
        sql_for_load = sqlcompile.loadsql(columns, loadfile, fieldterminated=fieldterminated)
        rows, action, result = self.execute(sql_for_load, verbose=verbose)
        mysqllogger.info(f"【{action}】{tablename} loaddata {rows} rows succeed, loadfile: {loadfile} !")
        return rows, action, result
