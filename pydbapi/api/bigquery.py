# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-12 12:00:28
# @Last Modified time: 2021-04-13 20:06:33
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import os

from google.cloud import bigquery
from google.cloud.bigquery import dbapi

from pydbapi.db import DBCommon, DBFileExec
from pydbapi.sql import SqlCompile
from pydbapi.conf import AUTO_RULES


import logging
import logging.config
from pydbapi.conf import LOGGING_CONFIG
logging.config.dictConfig(LOGGING_CONFIG)
bigquerylog = logging.getLogger('bigquery')


class SqlBigqueryCompile(SqlCompile):
    '''[summary]

    [description]
        构造bigquery sql
    Extends:
        SqlCompile
    '''

    def __init__(self, tablename):
        super(SqlBigqueryCompile, self).__init__(tablename)

    def create(self, columns, indexes):
        sql = self.create_nonindex(columns)
        if indexes and not isinstance(indexes, list):
            raise TypeError(f"indexes must be a list !")
        if indexes:
            indexes = ','.join(indexes)
            sql = f"{sql.replace(r');', '')},index({indexes}));"
        return sql

    def add_columns(self, col_name, col_type):
        col_type = f"{col_type}(32)" if col_type == 'varchar' else col_type
        sql = f'alter table {self.tablename} add column {col_name} {col_type};'
        return sql


class BigqueryDB(DBCommon, DBFileExec):

    def __init__(self, service_account):
        self.service_account = self.__check_service_account(service_account)
        super(BigqueryDB, self).__init__()
        self.auto_rules = AUTO_RULES

    def __check_service_account(self, service_account):
        if not os.path.exists(service_account):
            url = 'https://console.cloud.google.com/apis/credentials/serviceaccountkey'
            raise Exception(f"请前往{url}申请并下载")
        return service_account

    def get_conn(self):
        client = bigquery.Client.from_service_account_json(self.service_account)
        conn = dbapi.connect(client)
        if not conn:
            self.get_conn()
        return conn

    def create(self, tablename, columns, indexes=None):
        # tablename = f"{self.database}.{tablename}"
        sqlcompile = SqlBigqueryCompile(tablename)
        sql_for_create = sqlcompile.create(columns, indexes)
        rows, action, result = self.execute(sql_for_create)
        return rows, action, result

    def select(self, tablename, columns, condition=None):
        '''[summary]

        [description]
            查询数据，暂时不考虑join形式。如果是join形式请使用原始sql查询。
        Arguments:
            tablename {[str]} -- [表名]
            columns {[dict]} -- [列的信息]
            {'id_rename': {'order': 1, 'source_col':'datas', 'source_type': '', 'func': 'min', 'source_name': 'id'}, ……}
                # order: 用于排序
                # source_col: 原始数据列名 用于解析
                # source_type: 原始数据类型 用于解析
                # source_name: 解析的KEY或者原始数据的列名
                # func: 后续处理的函数

        Keyword Arguments:
            condition {[str]} -- [查询条件] (default: {None})

        Returns:
            [type] -- [description]
        '''

        def deal_columns(columns):
            '''[summary]

            [description]
                处理columns
            Arguments:
                columns {[dict]} -- [原始dict]
                {'id_rename': {'order': 1, 'source_col':'datas', 'source_type': '', 'func': 'min', 'source_name': 'id'}, ……}
            Returns:
                [dict] -- [构造columns]
                {'id_rename': {'source':'id', 'func': 'min', 'order': 1}, ……}
            '''
            columns_dealed = {}
            if not isinstance(columns, dict):
                raise TypeError(f"columns must be a dict !")

            for col, info in columns.items():
                if not isinstance(info, dict):
                    raise TypeError(f"【({col}){info}】info must be a dict !")

                tmp = {}
                source_col = info.get('source_col')
                source_type = info.get('source_type', 'json')  # 默认json处理
                source_name = info.get('source_name', col)  # 不存在就是用命名列
                func = info.get('func')
                order = info.get('order')

                if func:
                    tmp['func'] = func
                if order:
                    tmp['order'] = order
                if source_col and source_type == 'json':
                    source_name = f"JSON_EXTRACT({source_col}, '$.{source_name}')"
                tmp['source'] = source_name

                columns_dealed[col] = tmp
            return columns_dealed

        columns = deal_columns(columns)
        sqlcompile = SqlBigqueryCompile(tablename)
        sql_for_select = sqlcompile.select_base(columns, condition)
        rows, action, result = self.execute(sql_for_select)
        return rows, action, result

    def add_columns(self, tablename, columns):
        old_columns = self.get_columns(tablename)
        old_columns = set(old_columns)
        new_columns = set(columns)
        # bigquerylog.info(f'{old_columns}, {new_columns}')

        if old_columns == new_columns:
            bigquerylog.info(f'【{tablename}】columns not changed !')
        if old_columns - new_columns:
            raise Exception(f"【{tablename}】columns【{old_columns - new_columns}】 not set, should exists !")
        if new_columns - old_columns:
            sqlcompile = SqlBigqueryCompile(tablename)
            add_columns = new_columns - old_columns
            for col_name in add_columns:
                col_type = columns.get(col_name)
                sql = sqlcompile.add_columns(col_name, col_type)
                self.execute(sql)
            bigquerylog.info(f'【{tablename}】add columns succeed !【{new_columns - old_columns}】')
