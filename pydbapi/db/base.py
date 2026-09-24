# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-12-26 17:02:17
# @github: https://github.com/longfengpili


import inspect
import sys
import threading
import time
from datetime import date
from typing import Union

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
        self._conn = None
        self._conn_lock = threading.Lock()

    @classmethod
    def get_instance(cls, *args, **kwargs):
        """Reuse one instance per class; never silently ignore new configuration."""
        with cls._instance_lock:
            instance = cls.__dict__.get('_instance')
            if instance is not None and not args and not kwargs:
                return instance
            config = inspect.signature(cls).bind(*args, **kwargs)
            config.apply_defaults()
            if instance is None:
                instance = cls(*args, **kwargs)
                instance._instance_config = config.arguments
                cls._instance = instance
            elif config.arguments != instance._instance_config:
                raise ValueError('get_instance configuration differs from the existing instance; '
                                 'construct a separate database instance instead.')
            return instance

    @abstractmethod
    def get_conn(self):
        pass

    def close(self):
        """Release this instance's connection; a later get_conn() can reconnect."""
        with self._conn_lock:
            conn, self._conn = self._conn, None
            if conn is not None:
                conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.close()
        except Exception:
            if exc_type is None:
                raise
            dblogger.exception('Failed to close connection while handling an error')
        return False

    def _begin_transaction(self, conn, cursor, statements):
        """Drivers other than SQLite start transactions themselves."""

    def _validate_statements(self, statements):
        controls = {'begin', 'commit', 'rollback', 'savepoint', 'release', 'end', 'start', 'start transaction'}
        if any(stmt.action in controls for stmt in statements):
            raise ValueError('execute owns the transaction; use the driver connection for explicit transaction SQL')

    def _rollback_connection(self, conn):
        conn.rollback()

    def _max_bind_params(self):
        return None

    @staticmethod
    def validate_ehandling(ehandling):
        if ehandling not in ('raise', 'pass'):
            raise ValueError("ehandling must be 'raise' or 'pass'")

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

        if not len(sqlstmts):
            raise ValueError('SQL must contain at least one executable statement')
        self._validate_statements(sqlstmts)

        bar_format = '{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix[0]}'
        sqlstmts = sqlstmts if verbose <= 1 else tqdm(sqlstmts, postfix=['START'], bar_format=bar_format)  # 如果verbose>=2则显示进度条
        return sqlstmts

    def _execute_step(self, cursor, sql, params=None):
        '''[summary]

        [description]
            在conn中执行单步sql
        Arguments:
            cursor {[cursor]} -- [游标]
            sql {[str]} -- [sql]

        Raises:
            ValueError -- [sql执行错误原因及SQL]
        '''
        try:
            if params is None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, params)
        except Exception as e:
            error = f"【Error】:{e}【Sql】:{sql}"
            raise ValueError(error) from e

    def cur_results(self, cursor, count):
        if count is None:
            return list(cursor.fetchall() or [])
        results = list(cursor.fetchmany(count) or []) if count else []
        # Finish streamed queries (including RETURNING) before commit/reuse.
        # count limits retained rows, not database work or network transfer.
        while cursor.fetchmany(1000):
            pass
        return results

    @abstractmethod
    def cur_columns(self, cursor):
        desc = cursor.description
        columns = ColumnsModel(*tuple(map(lambda x: ColumnModel(x[0], 'varchar'), desc))) if desc else None

        return columns

    def fetch_query_results(self, action, cursor, count, verbose):
        columns = self.cur_columns(cursor) if cursor.description is not None else None
        # Trino must consume its stream even for statements without columns.
        values = self.cur_results(cursor, count) if columns is not None or self.dbtype == 'trino' else []
        results = ResModel(columns, values, action=action, rowcount=cursor.rowcount)

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

    def execute(self, sqlstmts: Union[str, SqlStatements], count: int = None,
                ehandling: str = 'raise', verbose: int = 0) -> tuple:
        """Execute SQL, returning a closed cursor, action and durable result metadata.

        Each call commits on success; pass mode aborts and returns result.error.
        """
        return self._execute(sqlstmts, count, ehandling, verbose)

    def _execute(self, sqlstmts, count=None, ehandling='raise', verbose=0, *, parameter_sets=None):
        '''执行 SQL 语句并返回结果clear

        Arguments:
            sqlstmts (Union[str, SqlStatements]): 要执行的 SQL 语句
            count (int, optional): 返回的结果数量 (default: None)
            ehandling (str, optional): 错误处理方式 ('raise': 抛出异常) (default: 'raise')
            verbose (int, optional): 进程状态打印级别 (0: 不打印, 1: 打印进度信息, 2: 显示进度条)

        Returns:
            tuple: (cursor, action, results) 
                cursor: 游标对象, 可以获取游标的各种信息
                action: 执行的操作类型
                results: 查询返回的结果
        '''
        
        self.validate_ehandling(ehandling)
        if count is not None and (isinstance(count, bool) or not isinstance(count, int) or count < 0):
            raise ValueError('count must be None or a non-negative integer')
        if isinstance(verbose, bool) or not isinstance(verbose, int) or verbose < 0:
            raise ValueError('verbose must be a non-negative integer')
        statements = SqlStatements(sqlstmts) if isinstance(sqlstmts, str) else sqlstmts
        sqlstmts = self.prepare_sql_statements(statements, verbose)
        conn = cursor = None
        action = None
        cursor_closed = False
        try:
            conn = self.get_conn()
            cursor = conn.cursor()
            self._begin_transaction(conn, cursor, statements)
            with logging_redirect_tqdm():
                for idx, stmt in enumerate(sqlstmts):
                    action = stmt.action
                    sql = stmt.sql_without_terminator if self.dbtype == 'trino' else stmt.sql
                    step = f"【{idx:0>2d}_PROGRESS】({action}){stmt.tablename}::{stmt.comment}"
                    self.handle_progress_logging(step, verbose, sqlstmts)
                    params = parameter_sets[idx] if parameter_sets is not None else None
                    self._execute_step(cursor, sql, params)
                    # Intermediate streams are drained but their rows are discarded.
                    result_count = count if idx + 1 == len(sqlstmts) else 0
                    results = self.fetch_query_results(action, cursor, result_count, verbose)
            cursor_closed = True
            cursor.close()
            conn.commit()
            return cursor, action, results
        except BaseException as error:
            if cursor is not None and not cursor_closed:
                cursor_closed = True
                try:
                    cursor.close()
                except Exception:
                    dblogger.exception('Failed to close cursor while handling an error')
            if conn is not None:
                try:
                    self._rollback_connection(conn)
                except Exception:
                    # An uncertain transaction must never be reused or reported as
                    # a successfully handled error, even in pass mode.
                    try:
                        self.close()
                    except Exception:
                        dblogger.exception('Failed to discard connection after rollback failure')
                    raise
            if ehandling == 'pass' and cursor is not None and isinstance(error, Exception):
                dblogger.error('%s', error)
                return cursor, action, ResModel(None, [], action=action, error=error)
            raise
        finally:
            if verbose >= 2:
                sqlstmts.close()


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
        dblogger.info(f'【{action}】{tablename} delete {result.rowcount} rows succeed !')
        return cursor, action, result

    def insert(self, tablename, columns, inserttype: str = 'value', values: list = None, chunksize: int = 1000, 
               fromtable: str = None, condition: str = None, ehandling: str = 'raise', verbose: int = 0):
        self.validate_ehandling(ehandling)
        self._check_isauto(tablename)
        if inserttype == 'value':
            if not isinstance(columns, ColumnsModel) or not len(columns):
                raise ValueError('columns must be a non-empty ColumnsModel')
            if isinstance(chunksize, bool) or not isinstance(chunksize, int) or chunksize <= 0:
                raise ValueError('chunksize must be a positive integer')
            if not isinstance(values, list) or not values:
                raise ValueError('values must be a non-empty list')
            if any(not isinstance(row, (list, tuple)) or len(row) != len(columns) for row in values):
                raise ValueError('Each row must have exactly as many values as columns')
            limit = self._max_bind_params()
            if limit is not None:
                if len(columns) > limit:
                    raise ValueError('Number of columns exceeds the driver bind parameter limit')
                chunksize = min(chunksize, limit // len(columns))
            placeholder = '?' if self.dbtype in ('sqlite', 'trino') else '%s'
            column_names = ', '.join(col.newname for col in columns)
            row_sql = '(' + ', '.join([placeholder] * len(columns)) + ')'
            sqls, parameter_sets = [], []
            for offset in range(0, len(values), chunksize):
                chunk = values[offset:offset + chunksize]
                sqls.append(f'insert into {tablename} ({column_names}) values '
                            + ', '.join([row_sql] * len(chunk)) + ';')
                parameter_sets.append(tuple(value for row in chunk for value in row))
            cursor, action, result = self._execute(SqlStatements('\n'.join(sqls)), ehandling=ehandling,
                                                    verbose=verbose, parameter_sets=parameter_sets)
        else:
            sql = SqlCompile(tablename).insert(columns, inserttype=inserttype,
                                               fromtable=fromtable, condition=condition)
            cursor, action, result = self.execute(sql, ehandling=ehandling, verbose=verbose)
        if result.error is None:
            rows = len(values) if inserttype == 'value' else result.rowcount
            dblogger.info('%s insert %s rows succeeded', tablename, rows)
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
        if isinstance(retries, bool) or not isinstance(retries, int) or retries <= 0:
            raise ValueError('retries must be a positive integer')
        altersql = f'alter table {ftablename} rename to {ttablename};'
        renamed = False
        for attempt in range(retries):
            try:
                if not renamed:
                    self.execute(altersql, verbose=verbose)
                    renamed = True
                if not self.get_columns(ttablename):
                    raise RuntimeError('Renamed table has no visible columns')
                dblogger.info('Renamed %s to %s', ftablename, ttablename)
                return
            except Exception as error:
                if attempt + 1 == retries:
                    phase = 'metadata verification' if renamed else 'SQL execution'
                    raise RuntimeError(f'Failed to rename {ftablename} to {ttablename} '
                                       f'after {retries} attempts ({phase})') from error
                time.sleep(5)

    def alter_column(self, tablename: str, colname: str, newname: str = None, newtype: str = None, sqlexpr: str = None):
        old_columns = self.get_columns(tablename)
        alter_col = old_columns.get_column_by_name(colname)

        if not alter_col:
            raise ValueError(f'{colname} not in {tablename}')

        newname = newname or alter_col.newname
        newtype = newtype or alter_col.coltype
        if sqlexpr is None:
            sqlexpr = f'cast({colname} as {newtype})' if newtype != alter_col.coltype else colname
        newcol = ColumnModel(newname, newtype, sqlexpr=sqlexpr)
        if newcol == alter_col:
            dblogger.info(f"{newcol} same, not need to change ~")
            return

        alter_columns = old_columns.alter(colname, newcol)

        return alter_columns

    def alter_tablecol_base(self, ftablename: str, mtablename: str, alter_columns: ColumnsModel, 
                            conditions: list[str] = None, verbose: int = 0):
        if ftablename == mtablename:
            raise ValueError('Source and staging tables must differ')
        self._check_isauto(ftablename)
        self._check_isauto(mtablename)
        source_columns = self.get_columns(ftablename)
        staging_columns = self.get_columns(mtablename)
        if not source_columns or not staging_columns:
            raise ValueError('Both source and staging tables must exist')
        if set(staging_columns.all_cols) != set(alter_columns.all_cols):
            raise ValueError('Staging columns do not match the migration columns')
        count = self.execute(f'select count(*) from {mtablename}')[2].values[0][0]
        if count:
            raise ValueError('Staging table must be empty before migration')
        today_str = date.today().strftime('%Y%m%d')
        tablename_backup = f'{ftablename}_backup_{today_str}_{time.time_ns()}'
        stage = 'backup rename'
        try:
            self.alter_tablename(ftablename, tablename_backup, verbose=verbose)
            stage = 'copy'
            for condition in conditions or [None]:
                self.insert(mtablename, alter_columns, fromtable=tablename_backup, inserttype='select',
                            condition=condition, verbose=verbose)
            stage = 'final rename'
            self.alter_tablename(mtablename, ftablename, verbose=verbose)
        except Exception as error:
            raise RuntimeError(f'Migration failed during {stage}: source={ftablename}, '
                               f'backup={tablename_backup}, staging={mtablename}. '
                               'Existing tables are retained for recovery.') from error
        dblogger.info('Migration complete; backup retained at %s', tablename_backup)
        return tablename_backup
