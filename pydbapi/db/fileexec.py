# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-08 11:55:54
# @Last Modified time: 2020-06-15 16:13:40
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
dblog = logging.getLogger('db')


class DBFileExec(DBbase):

    def __init__(self):
        super(DBFileExec, self).__init__()

    def get_filesqls(self, filepath, **kw):
        sqlfileparser = SqlFileParse(filepath)
        sqls = sqlfileparser.get_sqls(**kw)
        return sqls

    def file_exec(self, filepath, **kw):
        sqls = self.get_filesqls(filepath, **kw)
        filename = os.path.basename(filepath)
        for desc, sql in sqls.items():
            dblog.info(f"Start Job 【{filename}】{desc[:30]}".center(80, '='))
            verbose = True if 'verbose' in desc or filename.startswith('test') \
                        or filename.endswith('test.sql') else False
            # dblog.info(f"{os.path.basename(filepath)}=={progress}")
            rows, action, result = self.execute(sql, verbose=verbose)
            if action == 'SELECT':
                dblog.info(f"【rows】: {rows}, 【action】: {action}, 【result】: \n{pd.DataFrame(result[1:], columns=result[0]).head()}")
            dblog.info(f"End Job 【{filename}】{desc[:30]}".center(80, '='))

