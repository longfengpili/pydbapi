# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-11-20 18:40:50
# @github: https://github.com/longfengpili


import threading
from datetime import date

from trino.dbapi import connect
from trino.auth import BasicAuthentication

from pydbapi.db import DBMixin, DBFileExec
from pydbapi.model import ColumnModel, ColumnsModel
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
        if not (coltype.startswith('varchar') or coltype == 'date'):
            raise TypeError(f"{partition} only support varchar, date !")
        partition = f"with (partitioned_by = ARRAY['{partition.newname}'])"
        return partition

    def table_properties(self, partition: str, format: str = 'ORC', transactional: str = 'true'):
        '''
        查看支持的属性
        SELECT * FROM system.metadata.table_properties; 
        '''
        coltype = partition.coltype
        if not (coltype.startswith('varchar') or coltype == 'date'):
            raise TypeError(f"{partition} only support varchar, date !")

        base_properties = f"format = '{format}',\n        transactional = {transactional}"

        if partition:
            partition_property = f"partitioned_by = ARRAY['{partition.newname}'],"
            table_properties = f'''
            with (
                {partition_property}
                {base_properties}
            )
            '''
        else:
            table_properties = f'''
            with (
                {base_properties}
            )
            '''

        return table_properties.strip()

    def create(self, columns, partition: str, transactional: str = 'true'):
        partition_col = columns.get_column_by_name(partition)
        if partition_col:
            columns.remove(partition)
            columns.append(partition_col)
        else:
            raise ValueError(f"<{partition}> not in {columns}")

        sql = self.create_nonindex(columns)
        table_properties = self.table_properties(partition=partition_col, transactional=transactional)
        sql += table_properties

        return sql


class TrinoDB(DBMixin, DBFileExec):
    _instance_lock = threading.Lock()

    def __init__(self, host: str, user: str, password: str, database: str, 
                 catalog: str = 'hive', port: int = 8443, safe_rule: bool = True, 
                 **kwargs: dict):
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
            kwargs (dict): [其他trino支持参数，可以询问开发同学，例如source、timezone等]
        '''

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.catalog = catalog
        self.database = database
        self.kwargs = kwargs
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
        if not hasattr(TrinoDB, '_conn'):
            with TrinoDB._instance_lock:
                if not hasattr(TrinoDB, '_conn'):
                    auth = BasicAuthentication(self.user, self.password)
                    conn = connect(host=self.host, user=self.user, auth=auth, 
                                   catalog=self.catalog, schema=self.database,
                                   port=self.port, http_scheme="https",
                                   **self.kwargs)
                    mytrinologger.info(f'connect {self.__class__.__name__}({self.user}@{self.host}:{self.port}/{self.catalog}.{self.database})')  # noqa: E501
                    TrinoDB._conn = conn
        return TrinoDB._conn

    def cur_columns(self, cursor):
        desc = cursor.description
        columns = ColumnsModel(*tuple(map(lambda x: ColumnModel(x.name, x.type_code), desc))) if desc else None
        return columns

    def create(self, tablename, columns, partition=None, verbose=0):
        # tablename = f"{self.database}.{tablename}"
        sqlcompile = SqlTrinoCompile(tablename)
        sql_for_create = sqlcompile.create(columns, partition=partition)
        cursor, action, result = self.execute(sql_for_create, verbose=verbose)
        return cursor, action, result

    def insert(self, tablename, columns, inserttype='value', values=None, chunksize=1000, 
               fromtable=None, condition=None, verbose=0):
        if values:
            vlength = len(values)

        if self._check_isauto(tablename):
            sqlcompile = SqlCompile(tablename)
            sql_for_insert = sqlcompile.insert(columns, inserttype=inserttype, values=values,
                                               chunksize=chunksize, fromtable=fromtable, condition=condition)
            cursor, action, result = self.execute(sql_for_insert, verbose=verbose)

            rows = cursor.rowcount
            rows = vlength if values else rows
            mytrinologger.info(f'【{action}】{tablename} insert succeed !')
            return cursor, action, result

    def alter_tablecol(self, tablename: str, colname: str, newname: str = None, newtype: str = None, 
                       sqlexpr: str = None, partition: str = 'part_date', conditions: list[str] = None, verbose: int = 0):
        alter_columns = self.alter_column(tablename, colname, newname, newtype, sqlexpr)

        if alter_columns:
            # create tmp table
            mtablename = f"{tablename}_tmp"
            self.create(mtablename, alter_columns, partition=partition, verbose=verbose)

            # alter table
            self.alter_tablecol_base(tablename, mtablename, alter_columns, conditions=conditions, verbose=verbose)
