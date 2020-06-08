# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-08 11:55:54
# @Last Modified time: 2020-06-08 18:11:45
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import os
import pandas as pd

from .base import DBbase
from dbapi.sql import SqlFileParse

import logging
from logging import config

config = config.fileConfig('./dbapi/mylogging/dblog.conf')
dblog = logging.getLogger('db')


class DBFileExec(DBbase):
    
    def __init__(self):
        super(DBFileExec, self).__init__()

    def get_filesqls(self, filepath, **kw):
        sqlfileparser = SqlFileParse(filepath)
        sqls = sqlfileparser.get_sqls(**kw)
        return sqls

    def exec_file(self, filepath, **kw):
        sqls = self.get_filesqls(filepath, **kw)
        filename = os.path.basename(filepath)
        for desc, sql in sqls.items():
            dblog.info(f"Start Job {desc[:40]}".center(80, '='))
            progress = True if 'show progress' in desc or filename.startswith('test') \
                        or filename.endswith('test.sql') else False
            # dblog.info(f"{os.path.basename(filepath)}=={progress}")
            rows, action, result = self.execute(sql, progress=progress)
            if action == 'SELECT':
                dblog.info(f"【rows】: {rows}, 【action】: {action}, 【result】: \n{pd.DataFrame(result[1:], columns=result[0]).head()}")
            dblog.info(f"End Job {desc[:40]}".center(80, '='))
            
