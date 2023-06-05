# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2022-11-14 14:17:02
# @Last Modified by:   longfengpili
# @Last Modified time: 2023-06-05 18:43:33


import threading
import requests

from pydbapi.db import DBMixin, DBFileExec
from pydbapi.sql import SqlCompile


import logging
mytalogger = logging.getLogger(__name__)


class SqlTaCompile(SqlCompile):
    '''[summary]

    [description]
        构造mysql sql
    Extends:
        SqlCompile
    '''

    def __init__(self, tablename):
        super(SqlTaCompile, self).__init__(tablename)


class TaDB(DBMixin, DBFileExec):
    _instance_lock = threading.Lock()

    def __init__(self, url: str, token: str, outformat: str = 'csv_header', timeout: int = 600):
        '''[summary]
        
        [ta method]
        
        Args:
            url (str): [ta url]
            token (str): [ta token]
            outformat (str): [输出格式] (default: `'csv_header'`, 行
                                        数据格：json, csv, csv_header, tsv, tsv_header, json_object)
            timeout (int): [超时参数] (default: `600`)
            # safe_rule (bool): [description] (default: `False`)
        '''

        self.url = url
        self.token = token
        self.outformat = outformat
        self.timeout = timeout
        super(TaDB, self).__init__()
        # self.auto_rules = AUTO_RULES if safe_rule else None

    # def __new__(cls, *args, **kwargs):
    #     if not hasattr(TaDB, '_instance'):
    #         with TaDB._instance_lock:
    #             if not hasattr(TaDB, '_instance'):
    #                 TaDB._instance = super().__new__(cls)

    #     return TaDB._instance

    @classmethod
    def get_instance(cls, *args, **kwargs):
        mytalogger.info(TaDB._instance_lock)
        if not hasattr(TaDB, '_instance'):
            mytalogger.info(TaDB._instance_lock)
            with TaDB._instance_lock:
                if not hasattr(TaDB, '_instance'):
                    TaDB._instance = cls(*args, **kwargs)

        return TaDB._instance

    def get_response(self, sql):
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        params = {
            'token': self.token,
            'format': self.outformat,
            'timeoutSeconds': self.timeout,
            'sql': sql,
        }
        # print(sql)
        res = requests.post(url=self.url, headers=headers, params=params)
        mytalogger.debug(res.url)
        status_code = res.status_code
        while status_code != 200:
            mytalogger.error(f"get url {res.url}, status_code: {status_code}")
            res = requests.post(url=self.url, headers=headers, params=params)
            status_code = res.status_code

        # res.encoding = res.apparent_encoding
        return res

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
        action = 'SELECT'
        res = self.get_response(sql)
        results = res.text
        rows = len(results.split('\n')) - 2

        return rows, action, results

    def drop(self):
        raise AttributeError("TA not support drop action !!!")

    def create(self):
        raise AttributeError("TA not support create action !!!")

    def insert(self):
        raise AttributeError("TA not support insert action !!!")
