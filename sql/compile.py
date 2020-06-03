# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-03 14:04:33
# @Last Modified time: 2020-06-03 15:05:16
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

class SqlCompile(object):

    def __init__(self):
        pass

    def select(self, tablename, columns, condition=None):
        columns = ', '.join(columns)

        if condition:
            sql = f'select {columns} from {tablename} where {condition};'
        else:
            sql = f'select {columns} from {tablename};'

        return sql


    def create_nonindex(self, tablename, columns):
        '''[summary]
        
        [description]
            create sql
        Arguments:
            tablename {[str]} -- [表名]
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
            create table if not exists {tablename}
            ({columns});
        '''
        sql = re.sub(r'\s{2,}', '\n', sql)
        return sql

    def drop(self, tablename):
        sql = f'drop table if exists {tablename};'
        return sql

    def insert(self, tablename, columns, values):
        '''[summary]
        
        [description]
            插入数据
        Arguments:
            tablename {[str]} -- [表名]
            columns {[dict]} -- [列名及属性]
            values {[list]} -- [插入的数据]
        '''
        def deal_values(values):
            j_values = []
            if not isinstance(values, list):
                raise TypeError('values must be a list !')
            j_values = [str(tuple(value)) for v in values]
            return ','.join(j_values)

        columns = ', '.join(columns)
        values = deal_values(values)

        sql = f'''insert into {tablename}
                ({columns})
                values ({values})
                ;
            '''
        return sql

    def delete(self, tablename, condition):
        sql = f'''delete from {tablename} where {condition};'''
        return sql



