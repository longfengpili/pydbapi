# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2022-11-14 14:17:02
# @Last Modified by:   longfengpili
# @Last Modified time: 2022-11-14 19:31:14


import re
import tqdm
import pandas as pd
import threading

from pydbapi.col import ColumnModel
from pydbapi.db import DBMixin, DBFileExec
from pydbapi.sql import SqlParse, SqlCompile
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
        if coltype != 'date':
            raise TypeError(f"{partition} only support date type !")
        partition = f"with (partitioned_by = ARRAY['{partition.newname}'])"
        return partition

    def create(self, columns, partition=None):
        if partition:
            dt = ColumnModel('dt', 'date')
            columns = columns.append(dt)

        sql = self.create_nonindex(columns)

        if dt:
            partition = self.create_partition(dt)
            sql = sql.replace(';', f'\n{partition};')

        return sql


class TrinoDB(DBMixin, DBFileExec):
    _instance_lock = threading.Lock()

    def __init__(self, host, user, password, database, catalog='hive', port=9090, safe_rule=True):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.catalog = catalog
        self.database = database
        super(TrinoDB, self).__init__()
        self.auto_rules = AUTO_RULES if safe_rule else None

    # def __new__(cls, *args, **kwargs):
    #     if not hasattr(TrinoDB, '_instance'):
    #         with TrinoDB._instance_lock:
    #             if not hasattr(TrinoDB, '_instance'):
    #                 TrinoDB._instance = super().__new__(cls)

    #     return TrinoDB._instance

    @classmethod
    def get_instance(cls, *args, **kwargs):
        mytrinologger.info(TrinoDB._instance_lock)
        if not hasattr(TrinoDB, '_instance'):
            mytrinologger.info(TrinoDB._instance_lock)
            with TrinoDB._instance_lock:
                if not hasattr(TrinoDB, '_instance'):
                    TrinoDB._instance = cls(*args, **kwargs)

        return TrinoDB._instance

    def get_conn(self):
        try:
            from trino.dbapi import connect
        except Exception as e:
            mytrinologger.error(f"please add [trino] path in sys.path, error: {e}")
            raise
        conn = connect(schema=self.database, user=self.user, password=self.password,
                       host=self.host, port=self.port, catalog=self.catalog)
        if not conn:
            self.get_conn()
        return conn

    def execute(self, sql, count=None, ehandling='raise', verbose=0):
        '''[summary]

        [description]
            执行sql
        Arguments:
            sql {[str]} -- [sql]

        Keyword Arguments:
            count {[int]} -- [返回的结果数量] (default: {None})
            ehandling {[str]} -- [错误处理] （raise: 错误弹出异常）
            verbose {[int]} -- [打印进程状态] （0：不打印， 1：文字进度， 2：进度条）

        Returns:
            rows {[int]} -- [影响的行数]
            results {[list]} -- [返回的结果]
        '''
        # def cur_getresults(cur, count):
        #     results = cur.fetchmany(count) if count else cur.fetchall()
        #     results = list(results) if results else []
        #     columns = tuple(map(lambda x: x[0].lower(), cur.description)) if cur.description  # 列名
        #     return columns, results

        rows = 0
        idx = 0
        conn = self.get_conn()
        # mytrinologger.info(conn)
        cur = conn.cursor()
        sqls = SqlParse.split_sqls(sql)
        # print(sqls)
        sqls = [sql.strip() for sql in sqls if sql]
        sqls_length = len(sqls)
        bar_format = '{l_bar}{bar:30}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix[0]}'
        sqls = sqls if verbose <= 1 else tqdm(sqls, ncols=100, postfix=['START'], bar_format=bar_format)  # 如果verbose>=2则显示进度条
        for _sql in sqls:
            results = None
            idx += 1
            parser = SqlParse(_sql)
            comment, sql, action, tablename = parser.comment, parser.sql, parser.action, parser.tablename
            if not sql:
                # mytrinologger.info(f'【{idx:0>2d}_PROGRESS】 no run !!!\n{_sql}')
                continue

            step = f"【{idx:0>2d}_PROGRESS】({action}){tablename}::{comment}"

            if verbose == 1:
                mytrinologger.info(f"{step}")
                mytrinologger.debug(sql)
            elif verbose >= 2:
                sqls.postfix[0] = f"{step}"
                sqls.update()
            else:
                pass
                
            self._execute_step(cur, sql, ehandling=ehandling)
            results = self.cur_results(cur, count)

            if (action == 'SELECT' and (verbose or idx == sqls_length)) \
                    or (action == 'WITH' and idx == sqls_length):
                # columns, results = cur_getresults(cur, count)
                desc, columns = self.cur_columns(cur)
                if verbose and columns:
                    mytrinologger.info(f"\n{pd.DataFrame(results, columns=columns)}")
                elif not columns:
                    mytrinologger.warning(f"Not Columns, cursor description is {desc}")
                else:
                    pass

                if columns:
                    results.insert(0, columns)

        try:
            conn.commit()
        except Exception as e:
            mytrinologger.error(e)
            conn.rollback()

        rows = len(results)
        conn.close()
        return rows, action, results

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
