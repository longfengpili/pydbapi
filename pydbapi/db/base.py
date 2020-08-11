# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-02 18:46:58
# @Last Modified time: 2020-08-11 19:16:58
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import re
import os
import pandas as pd
from tqdm import tqdm

from pydbapi.sql import SqlParse, SqlCompile
from pydbapi.conf import AUTO_RULES

import logging
import logging.config
from pydbapi.conf import LOGGING_CONFIG
logging.config.dictConfig(LOGGING_CONFIG)
dblogger = logging.getLogger('db')

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
        sql = re.sub(r'\s{2,}', '\n', sql)
        try:
            cursor.execute(sql)
        except Exception as e:
            dblogger.error(f"{e}{sql}")
            raise ValueError(f"【Error】:{e}【Sql】:{sql};")

    def execute(self, sql, count=None, verbose=False):
        '''[summary]

        [description]
            执行sql
        Arguments:
            sql {[str]} -- [sql]

        Keyword Arguments:
            count {[int]} -- [返回的结果数量] (default: {None})

        Returns:
            rows {[int]} -- [影响的行数]
            results {[list]} -- [返回的结果]
        '''
        def cur_getresults(cur, count):
            results = cur.fetchmany(count) if count else cur.fetchall()
            results = list(results) if results else []
            columns = tuple(map(lambda x: x[0], cur.description)) #列名
            return columns, results

        rows = 0
        idx = 0
        conn = self.get_conn()
        # dblogger.info(conn)
        cur = conn.cursor()
        sql = sql if sql.strip().endswith(';') else sql.strip() + ';'
        sqls = sql.split(";")[:-1]
        sqls = [sql.strip() for sql in sqls if sql]
        sqls_length = len(sqls)
        bar_format='{l_bar}{bar:30}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix[0]}'
        sqls = sqls if verbose else tqdm(sqls, ncols=100, postfix=['start'], bar_format=bar_format) # 如果没有verbose显示进度条
        for sql in sqls:
            results = None
            idx += 1
            # dblogger.info(sql)
            parser = SqlParse(sql)
            comment, sql, action, tablename = parser.comment, parser.sql, parser.action, parser.tablename
            step = f"【{idx}】({action}){tablename}::{comment}"
            if verbose:
                dblogger.info(f"Start {step}")
            else:
                sqls.postfix[0] = step
                sqls.update()

            self.__execute_step(cur, sql)

            if action == 'SELECT' and (verbose or idx == sqls_length):
                columns, results = cur_getresults(cur, count)
                if verbose:
                    dblogger.info(f"\n{pd.DataFrame(results, columns=columns)}")
                results.insert(0, columns)
        try:
            conn.commit()
        except Exception as e:
            dblogger.error(e)
            conn.rollback()
        rows = cur.rowcount
        conn.close()
        return rows, action, results


class DBCommon(DBbase):

    def __init__(self):
        self.auto_rules = AUTO_RULES
        super(DBCommon, self).__init__()

    def __check_isauto(self, tablename):
        '''[summary]

        [description]
            通过tablename控制是否可以通过python代码处理
        Arguments:
            tablename {[str]} -- [表名]
        '''
        if not self.auto_rules: return True
        for rule in self.auto_rules:
            if rule in tablename:
                return True
        return False

    def drop(self, tablename):
        if self.__check_isauto(tablename):
            sqlcompile = SqlCompile(tablename)
            sql_for_drop = sqlcompile.drop()
            rows, action, result = self.execute(sql_for_drop)
            dblogger.info(f'【{action}】{tablename} drop succeed !')
            return rows, action, result
        else:
            raise Exception(f"【drop】 please drop [{tablename}] on workbench! Or add rule into auto_rules !")

    def delete(self, tablename, condition):
        if self.__check_isauto(tablename):
            sqlcompile = SqlCompile(tablename)
            sql_for_delete = sqlcompile.delete(condition)
            rows, action, result = self.execute(sql_for_delete)
            dblogger.info(f'【{action}】{tablename} delete succeed !')
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

    def select(self, tablename, columns, condition=None):
        '''[summary]
        
        [description]
            执行select 
        Arguments:
            tablename {[str]} -- [表名]
            columns {[dict]} -- [列的信息]
            {'id_rename': {'sqlexpr':'id', 'func': 'min', 'order': 1}, ……}
                # sqlexpr : sql表达式， 如果为空则默认获取key值。 可以是任何sql表达式。
                # order: 用于排序
                # func: 后续处理的函数
        
        Keyword Arguments:
            condition {[str]} -- [where中的表达式] (default: {None})
        
        Returns:
            rows[int] -- [影响的数量]
            action[str] -- [sql表达式DML]
            result[list] -- [结果, 第一个元素是列名]
        '''
        sqlcompile = SqlCompile(tablename)
        sql_for_select = sqlcompile.select_base(columns, condition)
        rows, action, result = self.execute(sql_for_select)
        return rows, action, result







