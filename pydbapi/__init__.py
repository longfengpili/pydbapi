# -*- coding: utf-8 -*-
# @Author: chunyang.xu
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-08-12 13:49:40
# @github: https://github.com/longfengpili


import os
import logging.config

from pydbapi.conf import LOGGING_CONFIG
logging.config.dictConfig(LOGGING_CONFIG)

os.environ['NUMEXPR_MAX_THREADS'] = '16'


def load_ipython_extension(ipython):
    """Expose the extension entry point used by ``%load_ext pydbapi``."""
    from pydbapi.api.pydbapimagic import load_ipython_extension as load_extension
    load_extension(ipython)

# from pydbapi.api import SqliteDB, RedshiftDB, MysqlDB, SnowflakeDB
# from pydbapi.sql import SqlParse, SqlCompile, SqlFileParse, ColumnModel, ColumnsModel

# __all__ = ['SqliteDB', 'RedshiftDB', 'MysqlDB', 'SnowflakeDB',
#            'SqlParse', 'SqlCompile', 'SqlFileParse', 'ColumnModel', 'ColumnsModel']
