# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-03 14:04:33
# @Last Modified time: 2020-07-01 16:09:47
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-


import re

class SqlCompile(object):

    def __init__(self, tablename):
        self.tablename = tablename
        self.aggfunc = ['min', 'max', 'sum', 'count']

    def select_base(self, columns, condition=None):
        '''[summary]
        
        [description]
            生成select sql (未考虑join，所以暂时用base)
        Arguments:
            columns {[dict]} -- [列的信息，需要按照排列顺序处理]
            {'id_rename': {'sqlexpr':'id', 'func': 'min', 'order': 1}, ……}
            # sqlexpr : sql表达式
            # order: 用于排序
            # func: 后续处理的函数
        
        Keyword Arguments:
            condition {[条件]} -- [where中的条件] (default: {None})
        
        Returns:
            [str] -- [返回sql]
        
        Raises:
            TypeError -- [检查columns的情况]
        '''
        def deal_columns(columns):
            agg_cols = []
            ori_cols = []
            group_cols = []

            order_cols = list(filter((lambda x: x[1].get('order', 10000) < 10000), columns.items()))
            order_cols = sorted(order_cols, key=lambda x: x[1].get('order'))
            order_cols = [col for col, info in order_cols]
            order_cols = ', '.join(order_cols)

            for col, info in columns.items():
                sqlexpr = info.get('sqlexpr', col)
                func = info.get('func')

                if func and func in self.aggfunc:
                    aggcol = f"{func}({sqlexpr}) as {col}"
                    agg_cols.append(aggcol)
                else:
                    group_cols.append(col)
                    col = col if sqlexpr == col else f"{sqlexpr} as {col}"
                    ori_cols.append(col)

            cols = ori_cols + agg_cols
            cols = ', '.join(cols)
            group_cols = ', '.join(group_cols) if agg_cols else None
            return cols, group_cols, order_cols

        if not isinstance(columns, dict):
            raise TypeError("colums must be a dict ! example:{'id': {'source':'id', 'func': 'min'}, ……}")

        columns, group_columns, order_columns = deal_columns(columns)
        sql = f'select {columns} from {self.tablename}'
        condition = f"where {condition}" if condition else ''
        group = f'group by {group_columns}' if group_columns else ''
        order = f'order by {order_columns}' if order_columns else ''
        sql = ' \n'.join([sql, condition, group, order]) + ';'
        return sql

    def create_nonindex(self, columns):
        '''[summary]
        
        [description]
            create sql
        Arguments:
            self.tablename {[str]} -- [表名]
            columns {[dict]} -- [列名及属性]
        
        Returns:
            [str] -- [sql]
        
        Raises:
            TypeError -- [类别错误]
        '''
        
        if not isinstance(columns, dict):
            raise TypeError('colums must be a dict ! example:{"column_name":"column_type"}')

        columns = ',\n'.join([k.lower() + ' '+ f"{'varchar(128)' if v == 'varchar' else v}" for k, v in columns.items()])
        sql = f'''
            create table if not exists {self.tablename}
            ({columns});
        '''
        sql = re.sub(r'\s{2,}', '\n', sql)
        return sql

    def drop(self):
        sql = f'drop table if exists {self.tablename};'
        return sql

    def insert(self, columns, values):
        '''[summary]
        
        [description]
            插入数据
        Arguments:
            self.tablename {[str]} -- [表名]
            columns {[dict]} -- [列名及属性]
            values {[list]} -- [插入的数据]
        '''
        def deal_values(values):
            j_values = []
            if not values:
                raise ValueError(f"{values} is empty !!!")
            if not isinstance(values, list):
                raise TypeError('values must be a list !')
            j_values = [str(tuple(value)) for value in values]
            return ','.join(j_values)

        columns = ', '.join(columns)
        values = deal_values(values)

        sql = f'''insert into {self.tablename}
                ({columns})
                values {values}
                ;
            '''
        return sql

    def delete(self, condition):
        sql = f'''delete from {self.tablename} where {condition};'''
        return sql
