# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-02 18:46:58
# @Last Modified time: 2020-06-03 19:01:07
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import re

from sql import SqlParse

import logging
from logging import config

config = config.fileConfig('dblog.conf')
dblog = logging.getLogger('db')

class DBbase(object):

    def __init__(self):
        pass

    def get_conn(self):
        pass

    def execute_step(self, cursor, sql):
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
            raise ValueError(f"{e}{sql}")

    def execute(self, sql, count=None):
        '''[summary]
        
        [description]
            执行sql
        Arguments:
            sql {[str]} -- [sql]
        
        Keyword Arguments:
            count {[int]} -- [返回的结果数量] (default: {None})
        
        Returns:
            count {[int]} -- [影响的行数]
            result {[list]} -- [返回的结果]
        '''
        result = None
        conn = self.get_conn()
        # dblog.info(conn)
        cur = conn.cursor()
        sqls = sql.split(";")[:-1]
        sqls = [sql.strip() for sql in sqls if sql]
        sqls_length = len(sqls)
        for idx, sql in enumerate(sqls):
            parser = SqlParse(sql)
            comment, action, tablename = parser.comment, parser.action, parser.tablename
            dblog.info(f"【{idx}】({action}){tablename}")
            self.execute_step(cur, sql)
            count = cur.rowcount
            if idx == sqls_length - 1 and action == 'select':
                result = cur.fetmany(count) if count else cur.fetchall()
                columns = tuple(map(lambda x: x[0], cur.description)) #列名
                result.insert(0, columns)
        try:
            conn.commit()
        except Exception as e:
            dblog.error(e)
            conn.rollback()
        conn.close()
        
        return count, result
