# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-11-20 17:29:02
# @github: https://github.com/longfengpili


import time
from pathlib import Path

from .base import DBbase
from pydbapi.sql import SqlFileParse

import logging
dblogger = logging.getLogger(__name__)


class DBFileExec(DBbase):

    def __init__(self):
        super(DBFileExec, self).__init__()

    def get_filesqls(self, filepath, **kw):
        sqlfileparser = SqlFileParse(filepath)
        arguments, sqls = sqlfileparser.get_filesqls(**kw)
        return arguments, sqls

    def file_exec(self, filepath: str, ehandling: str = None, verbose: int = 0, 
                  with_test: bool = False, with_snum: int = 0, **kw):
        st = time.time()
        results = {}

        filename = Path(filepath).stem

        if verbose != 0:
            dblogger.info(f"Start Job 【{filename}】".center(80, '='))

        arguments, sqlstatementses = self.get_filesqls(filepath, **kw)
        for desc, sqlstmts in sqlstatementses.items():
            dblogger.info(f">>> START {desc}")
            sqlverbose = verbose or (2 if 'verbose2' in desc else 1
                                     if 'verbose1' in desc or filename.startswith('test')
                                     else 0)
            sqlehandling = ehandling or ('pass' if 'epass' in desc else 'raise')
            # with_test=with_test, with_snum=with_snum, 
            if with_test:
                sqlstmts = sqlstmts.get_combination_sql(idx=with_snum)

            cursor, action, result = self.execute(sqlstmts, ehandling=sqlehandling, verbose=sqlverbose)
            results[desc] = result
            # dblogger.info(f"End {desc}")
        et = time.time()
        dblogger.info(f"End Job 【{filename}】, cost {et - st:.2f} seconds".center(80, '='))
        return results
