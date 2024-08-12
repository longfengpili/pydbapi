# -*- coding: utf-8 -*-
# @Author: chunyang.xu
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-08-12 13:45:06
# @github: https://github.com/longfengpili


import os
import logging.config
from pathlib import Path

from pydbapi.api.pydbmagics import PydbapiMagics
from pydbapi.conf import LOGGING_CONFIG
logging.config.dictConfig(LOGGING_CONFIG)

os.environ['NUMEXPR_MAX_THREADS'] = '16'

# from pydbapi.api import SqliteDB, RedshiftDB, MysqlDB, SnowflakeDB
# from pydbapi.sql import SqlParse, SqlCompile, SqlFileParse, ColumnModel, ColumnsModel

# __all__ = ['SqliteDB', 'RedshiftDB', 'MysqlDB', 'SnowflakeDB',
#            'SqlParse', 'SqlCompile', 'SqlFileParse', 'ColumnModel', 'ColumnsModel']


# 注册magic命令
def load_ipython_extension(ipython):
    ipython.register_magics(PydbapiMagics)


def register_magic():
    # IPython 启动目录及脚本
    ipython_startup_dir = Path.home() / ".ipython" / "profile_default" / "startup"
    startup_script_path = ipython_startup_dir / "00-pydbapi-startup.py"
    
    # 确保启动目录存在
    if not os.path.exists(ipython_startup_dir):
        os.makedirs(ipython_startup_dir)
    
    # 写入启动脚本
    with open(startup_script_path, 'w') as f:
        f.write("get_ipython().run_line_magic('load_ext', 'pydbapi')\n")
