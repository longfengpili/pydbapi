# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-11-13 11:24:13
# @github: https://github.com/longfengpili


import logging.handlers
import os
import errno


def mklogdir(path):
    try:
        os.makedirs(path, exist_ok=True)  # Python>3.2
    except TypeError:
        try:
            os.makedirs(path)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else: raise


class MakeFileHandler(logging.handlers.TimedRotatingFileHandler):
    def __init__(self, filename, when='h', interval=1, backupCount=0, encoding=None, delay=True, utc=False, atTime=None):
        mklogdir(os.path.dirname(filename))
        super(MakeFileHandler, self).__init__(filename, when, interval, backupCount, encoding, delay, utc, atTime)
