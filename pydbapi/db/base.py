# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-10-12 11:05:06
# @github: https://github.com/longfengpili


import re
import sys
import time
import pandas as pd
from datetime import date

from abc import ABC, abstractmethod

from tqdm.contrib.logging import logging_redirect_tqdm

from pydbapi.sql import SqlParse, SqlCompile
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

    def prepare_sql_statements(self, sql, verbose):
        if any("jupyter" in arg for arg in sys.argv):
            from tqdm.notebook import tqdm
        else:
            from tqdm import tqdm

        sqlparse = SqlParse(sql)
        sqls = [stmt.value.strip(' ;') for stmt in sqlparse.statements]
        bar_format = '{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix[0]}'
        sqls = sqls if verbose <= 1 else tqdm(sqls, postfix=['START'], bar_format=bar_format)  # 如果verbose>=2则显示进度条
        return sqls

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

    def fetch_query_results(self, action, cur, count, verbose):
        columns = self.cur_columns(cur)
        results = self.cur_results(cur, count)
        results = ResModel(columns, results)

        if verbose and not columns:
            dblogger.warning(f"【{action}】No results")
        elif (verbose == 1 or verbose >= 3) and results:
            dblogger.info(f"\n{results.to_dataframe()}")

        return results

    def handle_progress_logging(self, step, verbose, sqls):
        if verbose == 1:
            dblogger.info(step)
        elif verbose >= 2:
            sqls.postfix[0] = step
            if verbose >= 3:
                dblogger.info(step)

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
        
        results = None
        conn = self.get_conn()
        cur = conn.cursor()
        sqls = self.prepare_sql_statements(sql, verbose)
        try: 
            with logging_redirect_tqdm():
                for idx, _sql in enumerate(sqls):
                    parser = SqlParse(_sql)
                    comment, sql, action, tablename = parser.comment, parser.sql, parser.action, parser.tablename
                    if not sql:
                        # dblogger.info(f'【{idx:0>2d}_PROGRESS】 no run !!!\n{_sql}')
                        continue

                    step = f"【{idx:0>2d}_PROGRESS】({action}){tablename}::{comment}"
                    self.handle_progress_logging(step, verbose, sqls)
                    self._execute_step(cur, sql, ehandling=ehandling)

                    if idx + 1 == len(sqls) or action in ['SELECT', 'WITH']:
                        results = self.fetch_query_results(action, cur, count, verbose)

            conn.commit()
        except Exception:
            if self.dbtype not in ('trino',):
                conn.rollback()
            raise
        finally:
            if self.dbtype not in ('trino',):
                cur.close()
            # conn.close()  # 注释掉conn

        rows = cur.rowcount
        rows = len(results) if rows == -1 and results else rows
        return rows, action, results


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
        rows, action, result = self.execute(sql_for_drop, verbose=verbose)
        dblogger.info(f'【{action}】{tablename} drop succeed !')
        return rows, action, result

    def delete(self, tablename, condition, verbose=0):
        self._check_isauto(tablename)
        sqlcompile = SqlCompile(tablename)
        sql_for_delete = sqlcompile.delete(condition)
        rows, action, result = self.execute(sql_for_delete, verbose=verbose)
        dblogger.info(f'【{action}】{tablename} delete {rows} rows succeed !')
        return rows, action, result

    def insert(self, tablename, columns, inserttype='value', values=None, chunksize=1000, 
               fromtable=None, condition=None, verbose=0):
        if values:
            vlength = len(values)

        self._check_isauto(tablename)
        
        sqlcompile = SqlCompile(tablename)
        sql_for_insert = sqlcompile.insert(columns, inserttype=inserttype, values=values,
                                           chunksize=chunksize, fromtable=fromtable, condition=condition)
        rows, action, result = self.execute(sql_for_insert, verbose=verbose)

        if values and rows != (vlength % chunksize or chunksize):
            raise Exception('Insert Error !!!')

        rows = vlength if values else rows
        dblogger.info(f'【{action}】{tablename} insert {rows} rows succeed !')
        return rows, action, result

    def get_columns(self, tablename, verbose=0):
        sql = f"pragma table_info('{tablename}');" if self.dbtype == 'sqlite' else f"show columns from {tablename};"
        rows, action, results = self.execute(sql, verbose=verbose)
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
        rows, action, result = self.execute(sql_for_select, verbose=verbose)
        return rows, action, result

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
