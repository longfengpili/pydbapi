# -*- coding: utf-8 -*-
# @Author: chunyang.xu
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2023-11-02 15:01:53
# @github: https://github.com/longfengpili


import os
import logging.config

from pydbapi.api.pydbmagics import PydbapiMagics
from pydbapi.conf import LOGGING_CONFIG
logging.config.dictConfig(LOGGING_CONFIG)

os.environ['NUMEXPR_MAX_THREADS'] = '16'

# from pydbapi.api import SqliteDB, RedshiftDB, MysqlDB, SnowflakeDB
# from pydbapi.sql import SqlParse, SqlCompile, SqlFileParse, ColumnModel, ColumnsModel

# __all__ = ['SqliteDB', 'RedshiftDB', 'MysqlDB', 'SnowflakeDB',
#            'SqlParse', 'SqlCompile', 'SqlFileParse', 'ColumnModel', 'ColumnsModel']


# 注册magic命令(如果使用mymagics.py, 则下边的函数不重要)
def load_ipython_extension(ipython):
    ipython.register_magics(PydbapiMagics)

# #  注册魔术命令, 在ipython文档中可以这样注册
# get_ipython().register_magics(PydbapiMagics)
