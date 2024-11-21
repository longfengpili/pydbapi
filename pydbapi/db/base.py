# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-11-21 11:35:50
# @github: https://github.com/longfengpili


import re
import sys
import time
from datetime import date

from abc import ABC, abstractmethod

from tqdm.contrib.logging import logging_redirect_tqdm

from pydbapi.sql import SqlStatement, SqlStatements, SqlCompile
from pydbapi.model import ColumnModel, ColumnsModel, ResModel

from pydbapi.conf import AUTO_RULES

import logging
dblogger = logging.getLogger(__name__)


class DBbase(ABC):

    def __init__(self, *args, **kwargs):
        self.dbtype = None

    @abstractmethod
    def get_conn(self):
        pass

    def prepare_sql_statements(self, sqlstmts, verbose):
        if any("jupyter" in arg for arg in sys.argv):
            from tqdm.notebook import tqdm
        else:
            from tqdm import tqdm

        if isinstance(sqlstmts, str):
            sqlstmts = SqlStatements(sqlstmts)
        elif isinstance(sqlstmts, SqlStatements):
            sqlstmts = sqlstmts
        else:
            raise TypeError("sqlstmts must be a string or an instance of SqlStatements")

        bar_format = '{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix[0]}'
        sqlstmts = sqlstmts if verbose <= 1 else tqdm(sqlstmts, postfix=['START'], bar_format=bar_format)  # 如果verbose>=2则显示进度条
        return sqlstmts

    def _execute_step(self, cursor, sql, ehandling='raise'):
        '''[summary]

        [description]
            在conn中执行单步sql
        Arguments:
            cursor {[cursor]} -- [游标]
            sql {[str]} -- [sql]

        Raises:
            ValueError -- [sql执行错误原因及SQL]
        '''
        sql = re.sub(r'\s{2,}', '\n', sql)
        try:
            cursor.execute(sql)
        except Exception as e:
            if ehandling == 'raise':
                raise ValueError(f"【Error】:{e}【Sql】:{sql}")

    def cur_results(self, cursor, count):
        results = cursor.fetchmany(count) if count else cursor.fetchall()
        results = list(results) if results else []
        return results

    @abstractmethod
    def cur_columns(self, cursor):
        desc = cursor.description
        columns = ColumnsModel(*tuple(map(lambda x: ColumnModel(x[0], 'varchar'), desc))) if desc else None

        return columns

    def fetch_query_results(self, action, cursor, count, verbose):
        columns = self.cur_columns(cursor)
        results = self.cur_results(cursor, count)
        results = ResModel(columns, results)

        if verbose and not columns and action != 'insert':
            dblogger.warning(f"【{action}】No results")
        elif (verbose == 1 or verbose >= 3) and results:
            dblogger.info(f"\n{results.to_dataframe()}")

        return results

    def handle_progress_logging(self, step, verbose, sqlstmts):
        if verbose == 1:
            dblogger.info(step)
        elif verbose >= 2:
            sqlstmts.postfix[0] = step
            if verbose >= 3:
                dblogger.info(step)

    def execute(self, sqlstmts: any([str, SqlStatements]), count: int = None, ehandling: str = 'raise', verbose: int = 0):
        '''[summary]

        [description]
            执行sql
        Arguments:
            sqlstmt {[str, SqlStatements]} -- [sql]

        Keyword Arguments:
            count {[int]} -- [返回的结果数量] (default: {None})
            ehandling {[str]} -- [错误处理] （raise: 错误弹出异常）
            verbose {[int]} -- [打印进程状态] （0：不打印， 1：文字进度， 2：进度条）

        Returns:
            cursor {[cursor]} -- [游标，可以返回游标的各种信息]
            action {[str]} -- [方法]
            results {[ResModel]} -- [返回的结果]
        '''
        
        results = None
        conn = self.get_conn()
        cursor = conn.cursor()
        sqlstmts = self.prepare_sql_statements(sqlstmts, verbose)
        try: 
            with logging_redirect_tqdm():
                for idx, stmt in enumerate(sqlstmts):
                    comment, sql, action, tablename = stmt.comment, stmt.sql, stmt.action, stmt.tablename
                    if not sql:
                        # dblogger.info(f'【{idx:0>2d}_PROGRESS】 no run !!!\n{_sql}')
                        continue

                    step = f"【{idx:0>2d}_PROGRESS】({action}){tablename}::{comment}"
                    self.handle_progress_logging(step, verbose, sqlstmts)
                    self._execute_step(cursor, sql, ehandling=ehandling)

                    if idx + 1 == len(sqlstmts) or action in ['SELECT', 'WITH']:
                        results = self.fetch_query_results(action, cursor, count, verbose)

            conn.commit()
        except Exception:
            if self.dbtype not in ('trino',):
                conn.rollback()
            raise
        finally:
            if self.dbtype not in ('trino',):
                cursor.close()
            # conn.close()  # 注释掉conn

        return cursor, action, results


class DBMixin(DBbase):

    def __init__(self):
        self.auto_rules = AUTO_RULES
        super(DBMixin, self).__init__()

    def _check_isauto(self, tablename):
        '''[summary]

        [description]
            通过tablename控制是否可以通过python代码处理
        Arguments:
            tablename {[str]} -- [表名]
        '''
        if not self.auto_rules:
            return True
        for rule in self.auto_rules:
            if rule in tablename:
                return True
        else:
            raise Exception(f"【drop】 please drop [{tablename}] on workbench! Or add rule into auto_rules !")
        return False

    def drop(self, tablename, verbose=0):
        self._check_isauto(tablename)
        sqlcompile = SqlCompile(tablename)
        sql_for_drop = sqlcompile.drop()
        cursor, action, result = self.execute(sql_for_drop, verbose=verbose)
        dblogger.info(f'【{action}】{tablename} drop succeed !')
        return cursor, action, result

    def delete(self, tablename, condition, verbose=0):
        self._check_isauto(tablename)
        sqlcompile = SqlCompile(tablename)
        sql_for_delete = sqlcompile.delete(condition)
        cursor, action, result = self.execute(sql_for_delete, verbose=verbose)
        dblogger.info(f'【{action}】{tablename} delete {cursor.rowcount} rows succeed !')
        return cursor, action, result

    def insert(self, tablename, columns, inserttype='value', values=None, chunksize=1000, 
               fromtable=None, condition=None, verbose=0):
        if values:
            vlength = len(values)

        self._check_isauto(tablename)
        
        sqlcompile = SqlCompile(tablename)
        sql_for_insert = sqlcompile.insert(columns, inserttype=inserttype, values=values,
                                           chunksize=chunksize, fromtable=fromtable, condition=condition)
        cursor, action, result = self.execute(sql_for_insert, verbose=verbose)

        rows = cursor.rowcount
        if values and rows != (vlength % chunksize or chunksize):
            raise Exception('Insert Error !!!')

        rows = vlength if values else rows
        dblogger.info(f'【{action}】{tablename} insert {rows} rows succeed !')
        return cursor, action, result

    def get_columns(self, tablename, verbose=0):
        sql = f"pragma table_info('{tablename}');" if self.dbtype == 'sqlite' else f"show columns from {tablename};"
        cursor, action, results = self.execute(sql, verbose=verbose)
        cols = results.values
        nameidx = 1 if self.dbtype == 'sqlite' else 0
        typeidx = 2 if self.dbtype == 'sqlite' else 1
        columns = ColumnsModel(*[ColumnModel(col[nameidx], col[typeidx]) for col in cols])
        
        return columns

    def select(self, tablename, columns, condition=None, verbose=0):
        '''[summary]

        [description]
            执行select 
        Arguments:
            tablename {[str]} -- [表名]
            columns {[dict]} -- [列的信息]

        Keyword Arguments:
            condition {[str]} -- [where中的表达式] (default: {None})

        Returns:
            rows[int] -- [影响的数量]
            action[str] -- [sql表达式DML]
            result[list] -- [结果, 第一个元素是列名]
        '''
        sqlcompile = SqlCompile(tablename)
        sql_for_select = sqlcompile.select_base(columns, condition=condition)
        cursor, action, result = self.execute(sql_for_select, verbose=verbose)
        return cursor, action, result

    def add_columns(self, tablename, columns, verbose=0):
        old_columns = self.get_columns(tablename)
        old_columns = old_columns.all_cols
        old_columns = set(old_columns)
        new_columns = columns.all_cols
        new_columns = set(new_columns)
        dblogger.info(f'{old_columns}, {new_columns}')

        if old_columns == new_columns:
            dblogger.info(f'【{tablename}】columns not changed !')
        if old_columns - new_columns:
            raise Exception(f"【{tablename}】columns【{old_columns - new_columns}】 not set, should exists !")
        if new_columns - old_columns:
            sqlcompile = SqlCompile(tablename)
            add_columns = new_columns - old_columns
            for col_name in add_columns:
                column = columns.get_column_by_name(col_name)
                sql = sqlcompile.add_column(column.newname, column.coltype)
                self.execute(sql, verbose=0)
            dblogger.info(f'【{tablename}】add columns succeeded !【{new_columns - old_columns}】')

    def alter_tablename(self, ftablename: str, ttablename: str, retries: int = 3, verbose: int = 0):
        altersql = f'alter table {ftablename} rename to {ttablename};'
        attempt = 0

        while attempt < retries:
            try:
                self.execute(altersql, verbose=verbose)
            except Exception as e:
                dblogger.error(f"Attempt {attempt + 1} failed: {e}")
                time.sleep(5)  # 在重试之前等待
                attempt += 1

            try:
                self.get_columns(ttablename)
                dblogger.info(f"alter table {ftablename} to {ttablename} succeeded ~")
                break
            except Exception:
                pass

        if attempt == retries:
            dblogger.error(f"All {retries} attempts to rename table {ftablename} to {ttablename} failed.")

    def alter_column(self, tablename: str, colname: str, newname: str = None, newtype: str = None, sqlexpr: str = None):
        old_columns = self.get_columns(tablename)
        alter_col = old_columns.get_column_by_name(colname)

        if not alter_col:
            dblogger.error(f"{colname} not in {tablename} !!!")
            return

        newname = newname or alter_col.newname
        newtype = newtype or alter_col.coltype
        sqlexpr = sqlexpr or f"cast({colname} as {newtype})" if newtype != alter_col.coltype \
                             else f"{alter_col.newname}" if newname != alter_col.newname else None
        newcol = ColumnModel(newname, newtype, sqlexpr=sqlexpr)
        if newcol == alter_col:
            dblogger.info(f"{newcol} same, not need to change ~")
            return

        alter_columns = old_columns.alter(colname, newcol)

        return alter_columns

    def alter_tablecol_base(self, ftablename: str, mtablename: str, alter_columns: ColumnsModel, 
                            conditions: list[str] = None, verbose: int = 0):
        # tablename
        today = date.today()
        today_str = today.strftime('%Y%m%d')
        time_str = time.time_ns()
        tablename_backup = f"{ftablename}_backup_{today_str}_{time_str}_{self.user}"

        # alter ftablename to backup
        self.alter_tablename(ftablename, tablename_backup, verbose=verbose)

        # move data to mtablename
        conditions = conditions or [None]
        for condition in conditions:
            self.insert(mtablename, alter_columns, fromtable=tablename_backup, inserttype='select', 
                        condition=condition, verbose=verbose)

        # alter mtablename to ftablename
        self.alter_tablename(mtablename, ftablename, verbose=verbose)
