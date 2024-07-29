# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-07-26 17:46:27
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-07-29 17:46:49
# @github: https://github.com/longfengpili


import re
import os
import sys
import colorlog


# 判断是否在 IPython 环境中
def is_ipython():
    try:
        from IPython import get_ipython
        return get_ipython() is not None
    except ImportError:
        return False


AUTO_RULES = ['test_xu', 'tmp']  # 可以自动执行表名（表名包含即可）
REDSHIFT_AUTO_RULES = AUTO_RULES + ['_data_aniland']  # Amazon Redshift 可以自动执行表名（表名包含即可）

# logging settings
LOG_BASE_PATH = os.path.join(os.path.expanduser('~'), 'pydbapilog')  # 可以user目录下查看日志
PROJECT_NAME = re.sub(':?\\\\', '_', os.getcwd())
PROJECT_NAME = PROJECT_NAME[1:] if PROJECT_NAME.startswith('/') else PROJECT_NAME  # linux

LOGGING_CONFIG = {
    'version': 1,  # 保留字
    'disable_existing_loggers': False,  # 禁用已经存在的logger实例
    # 日志文件的格式
    'formatters': {
        # 详细的日志格式
        'standard': {
            'format': '%(asctime)s.%(msecs)03d - %(threadName)s:%(thread)d - %(name)s - %(levelname)s - %(pathname)s - %(lineno)d - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
        # 简单的日志格式
        'simple': {
            'format': '%(asctime)s.%(msecs)03d - %(threadName)s - %(name)s - %(levelname)s - %(filename)s - %(lineno)d - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
        # 定义一个特殊的日志格式
        'collect': {
            'format': '%(message)s'
        },
        # color
        'color': {
            '()': colorlog.ColoredFormatter,
            'format': '%(asctime)s.%(msecs)03d - %(threadName)s - %(name)s - %(levelname_log_color)s%(levelname)s%(reset)s - %(filename)s - %(lineno)d - %(log_color)s%(message)s',  # noqa: E501
            'datefmt': '%Y-%m-%d %H:%M:%S',
            'reset': True, 
            'log_colors': {
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'red,bg_white',
            },
            'secondary_log_colors': {
                'levelname': {
                    'DEBUG': 'cyan',
                    'INFO': 'green',
                    'WARNING': 'yellow',
                    'ERROR': 'red',
                    'CRITICAL': 'red,bg_white',
                }
            }
        },
    },
    # 过滤器
    'filters': {
    },
    # 处理器
    'handlers': {
        # 在终端打印
        'console': {
            'level': 'DEBUG',
            'filters': [],
            'class': 'logging.StreamHandler',  #
            'formatter': 'color' if sys.stdout.isatty() or is_ipython() else 'simple'
        },
        # 默认的
        'default': {
            'level': 'INFO',
            'class': 'pydbapi.conf.MakeFileHandler',  # 能够判断创建日持文件
            'filename': os.path.join(LOG_BASE_PATH, f'{PROJECT_NAME}_default.log'),  # 日志文件
            'when': 'd',  # 每天备份
            'interval': 1,
            'backupCount': 30,  # 最多备份几个
            'formatter': 'standard',
            'encoding': 'utf-8',
        },
        'db': {
            'level': 'INFO',
            'class': 'pydbapi.conf.MakeFileHandler',  # 保存到文件，自动切
            'filename': os.path.join(LOG_BASE_PATH, f'{PROJECT_NAME}_db.log'),  # 日志文件
            'when': 'd',  # 每小时备份
            'interval': 1,
            'backupCount': 30,
            'formatter': 'simple',
            'encoding': "utf-8"
        },
        'sql': {
            'level': 'INFO',
            'class': 'logging.handlers.TimedRotatingFileHandler',  # 保存到文件，自动切
            'filename': os.path.join(LOG_BASE_PATH, f'{PROJECT_NAME}_sql.log'),  # 日志文件
            'when': 'd',  # 每小时备份
            'interval': 1,
            'backupCount': 30,
            'formatter': 'simple',
            'encoding': "utf-8"
        },
    },
    'loggers': {
        # 默认的logger应用如下配置
        '': {
            'handlers': ['console', 'default'],
            'level': 'INFO',
            'propagate': True,  # 向不向更高级别的logger传递
        },
        'db': {
            'handlers': ['console', 'db'],
            'level': 'INFO',
            'propagate': False,  # 向不向更高级别的logger传递
        },
        'sql': {
            'handlers': ['console', 'sql'],
            'level': 'INFO',
            'propagate': False,  # 向不向更高级别的logger传递
        },
        'redshift': {
            'handlers': ['console', 'db'],
            'level': 'INFO',
            'propagate': False,  # 向不向更高级别的logger传递
        },
        'sqlite': {
            'handlers': ['console', 'db'],
            'level': 'INFO',
            'propagate': False,  # 向不向更高级别的logger传递
        },
        'mysql': {
            'handlers': ['console', 'db'],
            'level': 'INFO',
            'propagate': False,  # 向不向更高级别的logger传递
        },
        'snowflake': {
            'handlers': ['console', 'db'],
            'level': 'INFO',
            'propagate': False,  # 向不向更高级别的logger传递
        },
    },
}
