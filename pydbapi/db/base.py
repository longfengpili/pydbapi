# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-02 18:46:58
# @Last Modified time: 2020-06-09 16:28:25
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import re
import os

from dbapi.sql import SqlParse, SqlCompile

import logging
from logging import config

config = config.fileConfig('./dbapi/mylogging/dblog.conf')
dblog = logging.getLogger('db')

class DBbase(object):

    def __init__(self):
        pass

    def get_conn(self):
        pass

    def __execute_step(self, cursor, sql):
        '''[summary]
        
        [description]
            在conn中执行单步sql
        Arguments:
            cursor {[cursor]} -- [游标]
            sql {[str]} -- [sql]
        
        Raises:
            ValueError -- [sql执行错误原因及SQL]
        '''
        sql = re.sub('\s{2,}', '\n', sql)
        try:
            cursor.execute(sql)
        except Exception as e:
            dblog.error(f"{e}{sql}")
            raise ValueError(f"【Error】:{e}【Sql】:{sql};")

    def execute(self, sql, count=None, progress=False):
        '''[summary]
        
        [description]
            执行sql
        Arguments:
            sql {[str]} -- [sql]
        
        Keyword Arguments:
            count {[int]} -- [返回的结果数量] (default: {None})
        
        Returns:
            rows {[int]} -- [影响的行数]
            result {[list]} -- [返回的结果]
        '''
        rows = 0
        result = None
        conn = self.get_conn()
        # dblog.info(conn)
        cur = conn.cursor()
        sql = sql if sql.strip().endswith(';') else sql.strip() + ';'
        sqls = sql.split(";")[:-1]
        sqls = [sql.strip() for sql in sqls if sql]
        sqls_length = len(sqls)
        for idx, sql in enumerate(sqls):
            # dblog.info(sql)
            parser = SqlParse(sql)
            comment, action, tablename = parser.comment, parser.action, parser.tablename
            if progress:
                dblog.info(f"【{idx}】({action}){tablename}::{comment}")
            self.__execute_step(cur, sql)
            rows = cur.rowcount
            if idx == sqls_length - 1 and action == 'SELECT':
                result = cur.fetchmany(count) if count else cur.fetchall()
                columns = tuple(map(lambda x: x[0], cur.description)) #列名
                result.insert(0, columns)
        try:
            conn.commit()
        except Exception as e:
            dblog.error(e)
            conn.rollback()
        conn.close()
        
        return rows, action, result


class DBCommon(DBbase):

    def __init__(self):
        self.auto_rules = ['test_xu']
        super(DBCommon, self).__init__()

    def __check_isauto(self, tablename):
        '''[summary]
        
        [description]
            通过tablename控制是否可以通过python代码处理
        Arguments:
            tablename {[str]} -- [表名]
        '''
        for rule in self.auto_rules:
            if rule in tablename:
                return True
            return False

    def drop(self, tablename):
        if self.__check_isauto(tablename):
            sqlcompile = SqlCompile(tablename)
            sql_for_drop = sqlcompile.drop()
            rows, action, result = self.execute(sql_for_drop)
            dblog.info(f'【{action}】{tablename} drop succeed !')
            return rows, action, result
        else:
            raise Exception(f"【drop】 please drop [{tablename}] on workbench! Or add rule into auto_rules !")

    def delete(self, tablename, condition):
        if self.__check_isauto(tablename):
            sqlcompile = SqlCompile(tablename)
            sql_for_delete = sqlcompile.delete(condition)
            rows, action, result = self.execute(sql_for_delete)
            dblog.info(f'【{action}】{tablename} delete succeed !')
            return rows, action, result
        else:
            raise Exception(f"【delete】 please delete [{tablename}] on workbench! Or add rule into auto_rules !")

    def insert(self, tablename, columns, values):
        if self.__check_isauto(tablename):
            sqlcompile = SqlCompile(tablename)
            sql_for_insert = sqlcompile.insert(columns, values)
            rows, action, result = self.execute(sql_for_insert)
            return rows, action, result
        else:
            raise Exception(f"【insert】 please insert [{tablename}] on workbench! Or add rule into auto_rules !")

    def get_columns(self, tablename):
        sql = f"select * from {tablename} limit 1;"
        rows, action, result = self.execute(sql)
        columns = result[0]
        return columns









