# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2025-01-15 13:58:58
# @github: https://github.com/longfengpili


import time
import re
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
        arguments, sqlstatementses = sqlfileparser.get_filesqls(**kw)
        return arguments, sqlstatementses

    def file_exec(self, filepath: str, ehandling: str = None, verbose: int = 0, 
                  with_test: bool = False, with_snum: int = 1, **kw):
        st = time.time()
        results = {}
        if ehandling is not None:
            self.validate_ehandling(ehandling)

        filename = Path(filepath).stem

        if verbose != 0:
            dblogger.info(f"Start Job 【{filename}】".center(80, '='))

        arguments, sqlstatementses = self.get_filesqls(filepath, with_test=with_test, with_snum=with_snum, **kw)
        for desc, sqlstmts in sqlstatementses.items():
            dblogger.info(f">>> START {desc}")
            flags = set(re.findall(r'\b(?:verbose[123]?|epass)\b', desc))
            sqlverbose = verbose or (3 if 'verbose3' in flags else 2 if 'verbose2' in flags else 1
                                     if {'verbose', 'verbose1'} & flags or filename.startswith('test') else 0)
            sqlehandling = ehandling or ('pass' if 'epass' in flags else 'raise')
            cursor, action, result = self.execute(sqlstmts, ehandling=sqlehandling, verbose=sqlverbose)
            results[desc] = result
            # dblogger.info(f"End {desc}")
        et = time.time()
        dblogger.info(f"End Job 【{filename}】, cost {et - st:.2f} seconds".center(80, '='))
        return results
