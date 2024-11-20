# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-11-20 18:42:44
# @github: https://github.com/longfengpili


import threading
import psycopg2

from pydbapi.db import DBMixin, DBFileExec
from pydbapi.sql import SqlCompile
from pydbapi.conf import AUTO_RULES


import logging
redlogger = logging.getLogger(__name__)


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
            index = f"interleaved sortkey({indexes})"
            sql += index

        return sql


class RedshiftDB(DBMixin, DBFileExec):
    _instance_lock = threading.Lock()

    def __init__(self, host, user, password, database, port='5439', safe_rule=True):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        super(RedshiftDB, self).__init__()
        self.auto_rules = AUTO_RULES if safe_rule else None
        self.dbtype = 'redshift'

    # def __new__(cls, *args, **kwargs):
    #     if not hasattr(RedshiftDB, '_instance'):
    #         with RedshiftDB._instance_lock:
    #             if not hasattr(RedshiftDB, '_instance'):
    #                 RedshiftDB._instance = super().__new__(cls)

    #     return RedshiftDB._instance

    @classmethod
    def get_instance(cls, *args, **kwargs):
        if not hasattr(RedshiftDB, '_instance'):
            with RedshiftDB._instance_lock:
                if not hasattr(RedshiftDB, '_instance'):
                    RedshiftDB._instance = cls(*args, **kwargs)

        return RedshiftDB._instance

    def get_conn(self):
        if not hasattr(RedshiftDB, '_conn'):
            with RedshiftDB._instance_lock:
                if not hasattr(RedshiftDB, '_conn'):
                    conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
                    redlogger.info(f'connect {self.__class__.__name__}({self.user}@{self.host}:{self.port}/{self.database})')
                    RedshiftDB._conn = conn
        return RedshiftDB._conn

    def create(self, tablename, columns, indexes=None, verbose=0):
        # tablename = f"{self.database}.{tablename}"
        sqlcompile = SqlRedshiftCompile(tablename)
        sql_for_create = sqlcompile.create(columns, indexes)
        cursor, action, result = self.execute(sql_for_create, verbose=verbose)
        return cursor, action, result
