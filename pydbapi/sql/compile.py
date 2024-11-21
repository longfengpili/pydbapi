# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-11-21 10:40:19
# @github: https://github.com/longfengpili


import re
from pydbapi.model import ColumnsModel
from pydbapi.sql import SqlStatements


class SqlCompile(object):

    def __init__(self, tablename):
        self.tablename = tablename

    def select_base(self, columns, fromtable=None, condition=None):
        '''[summary]

        [description]
            生成select sql (未考虑join，所以暂时用base)
        Arguments:
            columns {[ColumnsModel]} -- [列信息需要按照排列顺序处理]

        Keyword Arguments:
            condition {[条件]} -- [where中的条件] (default: {None})

        Returns:
            [str] -- [返回sql]

        Raises:
            TypeError -- [检查columns的情况]
        '''
        tablename = fromtable or self.tablename
        if not isinstance(columns, ColumnsModel):
            raise TypeError("colums must be a ColumnsModel !")

        sql = f'select {columns.select_cols}\nfrom {tablename}'
        condition = f"where {condition}" if condition else ''
        group = f'group by {columns.group_cols}' if columns.group_cols else ''
        order = f'order by {columns.order_cols}' if columns.order_cols else ''

        if condition:
            sql = sql + '\n' + condition
        if group:
            sql = sql + '\n' + group
        if order:
            sql = sql + '\n' + order
        sql = sql + '\n;'
        return SqlStatements(sql)

    def create_nonindex(self, columns):
        '''[summary]

        [description]
            create sql
        Arguments:
            self.tablename {[str]} -- [表名]
            columns {[ColumnsModel]} -- [列信息]

        Returns:
            [str] -- [sql]

        Raises:
            TypeError -- [类别错误]
        '''

        if not isinstance(columns, ColumnsModel):
            raise TypeError("colums must be a ColumnsModel !")

        sql = f'create table if not exists {self.tablename}\n{columns.create_cols};'
        return SqlStatements(sql)

    def drop(self):
        sql = f'drop table if exists {self.tablename};'
        return SqlStatements(sql)

    def _insert_value(self, columns, values):
        '''[summary]

        [description]
            插入数据
        Arguments:
            self.tablename {[str]} -- [表名]
            columns {[ColumnsModel]} -- [列信息]
            values {[list]} -- [插入的数据]
        '''
        def deal_value(dtypes: list, value: list):
            _value = []
            for d, v in zip(dtypes, value):
                if v is None:
                    v = 'Null'
                elif d.startswith('varchar') or d.startswith('str'):
                    v = v.replace("'", "''") if isinstance(v, str) else v
                    v = f"'{v}'"
                elif d in ('date', 'datetime', 'timestamp') and re.match(r'\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?', v):
                    v = f"'{v}'"
                else:
                    v = f'{v}'

                _value.append(v)
            _value = '(' + ','.join(_value) + ')'
            return _value

        def deal_values(columns: ColumnsModel, values: list):
            j_values = []
            if not values:
                raise ValueError(f"{values} is empty !!!")
            if not isinstance(values, list):
                raise TypeError('values must be a list !')

            dtypes = [col.coltype for col in columns]
            j_values = [deal_value(dtypes, value) for value in values]
            j_values = ',\n'.join(j_values)
            return j_values

        values = deal_values(columns, values)

        if not isinstance(columns, ColumnsModel):
            raise TypeError("colums must be a ColumnsModel !")

        sql = f'insert into {self.tablename}\n({columns.new_cols})\nvalues\n{values};'
        return sql

    def _insert_by_value(self, columns, values, chunksize=1000):
        sql = []
        vlength = len(values)
        step = 0
        for i in range(0, vlength, chunksize):
            step += 1
            maxi = i+chunksize
            maxi = maxi if maxi < vlength else vlength
            chvalues = values[i: maxi]
            _sql = self._insert_value(columns, chvalues)
            _sql = f"\n--[NO.{step:>03d}(total {vlength} rows)]insert {maxi-i} rows, From [{i+1}] to [{maxi}]\n{_sql}"
            sql.append(_sql)

        sql = ''.join(sql)
        return SqlStatements(sql)

    def _insert_by_select(self, fromtable, columns, condition=None):
        selectsql = self.select_base(columns, fromtable, condition=condition)

        sql = f'''insert into {self.tablename}\n({columns.new_cols})\n{selectsql.sql}'''
        return SqlStatements(sql)

    def insert(self, columns, inserttype='value', values=None, chunksize=1000, fromtable=None, condition=None):
        if inserttype == 'value':
            if not values:
                raise Exception(f"InsertType is {inserttype}, values must be not None")
            stmts = self._insert_by_value(columns, values, chunksize=chunksize)
        elif inserttype == 'select':
            if not fromtable:
                raise Exception(f"InsertType is {inserttype}, fromtable must be not None")
            stmts = self._insert_by_select(fromtable, columns, condition=condition)
        else:
            raise Exception(f"Not supported {inserttype}, insertType must be value or select")
        return stmts

    def delete(self, condition):
        sql = f'''delete from {self.tablename} where {condition};'''
        return SqlStatements(sql)

    def add_column(self, colname, coltype):
        coltype = f"{coltype}(32)" if coltype == 'varchar' else coltype
        sql = f'alter table {self.tablename} add column {colname} {coltype};'
        return SqlStatements(sql)
