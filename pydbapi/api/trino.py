# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2023-12-04 11:46:32
# @github: https://github.com/longfengpili


import threading
from trino.dbapi import connect
from trino.auth import BasicAuthentication

from pydbapi.db import DBMixin, DBFileExec
from pydbapi.sql import SqlCompile
from pydbapi.conf import AUTO_RULES


import logging
mytrinologger = logging.getLogger(__name__)


class SqlTrinoCompile(SqlCompile):
    '''[summary]

    [description]
        构造mysql sql
    Extends:
        SqlCompile
    '''

    def __init__(self, tablename):
        super(SqlTrinoCompile, self).__init__(tablename)

    def create_partition(self, partition):
        coltype = partition.coltype
        if not coltype.startswith('varchar'):
            raise TypeError(f"{partition} only support varchar !")
        partition = f"with (partitioned_by = ARRAY['{partition.newname}'])"
        return partition

    def create(self, columns, partition=None):
        partition_sql = None
        if partition:
            partition_key = columns.get_column_by_name(partition)
            columns.remove(partition)
            columns.append(partition_key)
            partition_sql = self.create_partition(partition_key)

        sql = self.create_nonindex(columns)

        if partition_sql:
            sql = sql.replace(';', f'\n{partition_sql};')

        return sql


class TrinoDB(DBMixin, DBFileExec):
    _instance_lock = threading.Lock()

    def __init__(self, host, user, password, database, catalog='hive', port=8443, safe_rule=True):
        '''[summary]
        
        [init]
        
        Args:
            host ([str]): [host]
            user ([str]): [username]
            password ([str]): [password]
            database ([str]): [database]
            isolation_level (number): [isolation_level] (default: `0`)
                AUTOCOMMIT = 0  # 每个事务单独执行
                READ_UNCOMMITTED = 1  # 脏读（dirty read），一个事务可以读取到另一个事务未提交的事务记录
                READ_COMMITTED = 2 # 不可重复读（non-repeatable read），一个事务只能读取到已经提交的记录，不能读取到未提交的记录
                REPEATABLE_READ = 3 # 幻读（phantom read），一个事务可以多次从数据库读取某条记录，而且多次读取的那条记录都是一致的，相同的
                SERIALIZABLE = 4 # 事务执行时，会在所有级别上加锁，比如read和write时都会加锁，仿佛事务是以串行的方式进行的，而不是一起发生的。这会防止脏读、不可重复读和幻读的出现，但是，会带来性能的下降
                数据库默认的隔离级别：mysql为可重复读，oracle为提交后读
                trino不支持多个事务组合操作
            catalog (str): [cataglog] (default: `'hive'`)
            port (number): [port] (default: `8443`)
            safe_rule (bool): [safe rule] (default: `True`)
        '''

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.catalog = catalog
        self.database = database
        super(TrinoDB, self).__init__()
        self.auto_rules = AUTO_RULES if safe_rule else None
        self.dbtype = 'trino'

    # def __new__(cls, *args, **kwargs):
    #     if not hasattr(TrinoDB, '_instance'):
    #         with TrinoDB._instance_lock:
    #             if not hasattr(TrinoDB, '_instance'):
    #                 TrinoDB._instance = super().__new__(cls)

    #     return TrinoDB._instance

    @classmethod
    def get_instance(cls, *args, **kwargs):
        # mytrinologger.info(TrinoDB._instance_lock)
        if not hasattr(TrinoDB, '_instance'):
            # mytrinologger.info(TrinoDB._instance_lock)
            with TrinoDB._instance_lock:
                if not hasattr(TrinoDB, '_instance'):
                    TrinoDB._instance = cls(*args, **kwargs)

        return TrinoDB._instance

    def get_conn(self):
        auth = BasicAuthentication(self.user, self.password)
        conn = connect(host=self.host, user=self.user, auth=auth, 
                       catalog=self.catalog, schema=self.database,
                       port=self.port, http_scheme="https"
                       )
        mytrinologger.info(f'connect {self.__class__.__name__}({self.user}@{self.host}:{self.port}/{self.catalog}.{self.database})')  # noqa: E501
        if not conn:
            self.get_conn()
        return conn

    def create(self, tablename, columns, partition=None, verbose=0):
        # tablename = f"{self.database}.{tablename}"
        sqlcompile = SqlTrinoCompile(tablename)
        sql_for_create = sqlcompile.create(columns, partition=partition)
        rows, action, result = self.execute(sql_for_create, verbose=verbose)
        return rows, action, result

    def insert(self, tablename, columns, inserttype='value', values=None, chunksize=1000, 
               fromtable=None, condition=None, verbose=0):
        if values:
            vlength = len(values)

        if self._check_isauto(tablename):
            sqlcompile = SqlCompile(tablename)
            sql_for_insert = sqlcompile.insert(columns, inserttype=inserttype, values=values,
                                               chunksize=chunksize, fromtable=fromtable, condition=condition)
            rows, action, result = self.execute(sql_for_insert, verbose=verbose)

            rows = vlength if values else rows
            mytrinologger.info(f'【{action}】{tablename} insert succeed !')
            return rows, action, result
