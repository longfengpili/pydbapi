# -*- coding: utf-8 -*-
# @Author: chunyang.xu
# @Date:   2021-04-13 20:12:04
# @Last Modified by:   chunyang.xu
# @Last Modified time: 2021-04-13 20:22:29


import threading
import pymysql

from pydbapi.db import DBCommon, DBFileExec
from pydbapi.sql import SqlCompile, SqlFileParse
from pydbapi.conf import AUTO_RULES


import logging
dorislogger = logging.getLogger(__name__)


class DorisDBFileExec(DBFileExec):

    def __init__(self):
        super(DorisDBFileExec, self).__init__()

    def get_filesqls(self, filepath, **kw):
        sqlfileparser = SqlFileParse(filepath)
        arguments, sqls = sqlfileparser.get_filesqls(**kw)
        return arguments, sqls


class SqlDorisCompile(SqlCompile):
    '''[summary]

    [description]
        构造mysql sql
    Extends:
        SqlCompile
    '''

    def __init__(self, tablename):
        super(SqlDorisCompile, self).__init__(tablename)

    def create(self, columns, indexes):
        'mysql 暂不考虑索引'
        sql = self.create_nonindex(columns)
        # if indexes and not isinstance(indexes, list):
        #     raise TypeError(f"indexes must be a list !")
        # if indexes:
        #     indexes = ','.join(indexes)
        #     sql = f"{sql.replace(';', '')}interleaved sortkey({indexes});"
        return sql


class DorisDB(DBCommon, DorisDBFileExec):
    _instance_lock = threading.Lock()

    def __init__(self, host, user, password, database, port=3306, charset="utf8", safe_rule=True):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset
        super(DorisDB, self).__init__()
        self.auto_rules = AUTO_RULES if safe_rule else None

    @classmethod
    def get_instance(cls, *args, **kwargs):
        if not hasattr(DorisDB, '_instance'):
            with DorisDB._instance_lock:
                if not hasattr(DorisDB, '_instance'):
                    DorisDB._instance = DorisDB(*args, **kwargs)

        return DorisDB._instance

    def get_conn(self):
        conn = pymysql.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port, charset=self.charset)
        if not conn:
            self.get_conn()
        return conn

    def create(self, tablename, columns, indexes=None, verbose=0):
        # tablename = f"{self.database}.{tablename}"
        sqlcompile = SqlDorisCompile(tablename)
        sql_for_create = sqlcompile.create(columns, indexes)
        rows, action, result = self.execute(sql_for_create, verbose=verbose)
        return rows, action, result

    def dumpdata(self, tablename, columns, dumpfile, condition=None, verbose=0):
        sqlcompile = SqlDorisCompile(tablename)
        sql_for_dump = sqlcompile.dumpsql(columns, dumpfile, condition=condition)
        rows, action, result = self.execute(sql_for_dump, verbose=verbose)
        dorislogger.info(f"【{action}】{tablename} dumpdata {rows} rows succeed, outfile: {dumpfile} !")
        return rows, action, result

    def loaddata(self, tablename, columns, loadfile, fieldterminated=',', verbose=0):
        sqlcompile = SqlDorisCompile(tablename)
        sql_for_load = sqlcompile.loadsql(columns, loadfile, fieldterminated=fieldterminated)
        rows, action, result = self.execute(sql_for_load, verbose=verbose)
        dorislogger.info(f"【{action}】{tablename} loaddata {rows} rows succeed, loadfile: {loadfile} !")
        return rows, action, result
