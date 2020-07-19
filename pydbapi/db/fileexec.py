# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-08 11:55:54
# @Last Modified time: 2020-07-19 17:49:59
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import os
import pandas as pd

from .base import DBbase
from pydbapi.sql import SqlFileParse

import logging
import logging.config
from pydbapi.conf import LOGGING_CONFIG
logging.config.dictConfig(LOGGING_CONFIG)
dblogger = logging.getLogger('db')


class DBFileExec(DBbase):

    def __init__(self):
        super(DBFileExec, self).__init__()

    def get_filesqls(self, filepath, **kw):
        sqlfileparser = SqlFileParse(filepath)
        sqls = sqlfileparser.get_filesqls(**kw)
        return sqls

    def file_exec(self, filepath, **kw):
        results = {}
        sqls = self.get_filesqls(filepath, **kw)
        filename = os.path.basename(filepath)
        for desc, sql in sqls.items():
            dblogger.info(f"Start Job 【{filename}】{desc}".center(80, '='))
            verbose = True if 'verbose' in desc or filename.startswith('test') \
                        or filename.endswith('test.sql') else False
            # dblogger.info(f"{os.path.basename(filepath)}=={progress}")
            rows, action, result = self.execute(sql, verbose=verbose)
            results[desc] = result
            dblogger.info(f"End Job 【{filename}】{desc}".center(80, '='))
        return results

