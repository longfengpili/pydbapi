# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-03 10:51:08
# @Last Modified time: 2020-06-29 15:26:03
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import re
import os

from datetime import datetime, date, timedelta

import logging
import logging.config
from pydbapi.conf import LOGGING_CONFIG
logging.config.dictConfig(LOGGING_CONFIG)
sqllogger = logging.getLogger('sql')

class SqlParse(object):

    def __init__(self, orisql):
        self.orisql = orisql

    @property
    def comment(self):
        comment = re.match('(?:--)(.*?)\n', self.orisql.strip())
        comment = comment.group(1) if comment else ''
        return comment

    @property
    def sql(self):
        sql = self.orisql.replace(f"--{self.comment}", '')
        return sql.strip()

    @property
    def action(self):
        sql = re.sub('--.*?\n', '', self.orisql.strip())
        action = sql.strip().split(' ')[0]
        return action.upper()

    @property
    def tablename(self):
        create = re.search(r'table (?:if exists |if not exists )?(.*?)(?:\s|;|$)', self.orisql)
        update = re.search(r'update (.*?)(?:\s|;|$)', self.orisql)
        insert = re.search(r'insert into (.*?)(?:\s|;|$)', self.orisql)
        delete = re.search(r'delete (?:from )?(.*?)(?:\s|;|$)', self.orisql)
        select = re.search(r'select.*?from (.*?)(?:\s|;|$)', self.orisql, re.S)
        tablename = create or update or insert or delete or select
        tablename = tablename.group(1) if tablename else self.orisql.strip()
        return tablename


class SqlFileParse(object):

    def __init__(self, filepath):
        self.filepath = filepath
        self.reg_behind = r'(?=[,);:\s])'

    def get_content(self):
        if not os.path.isfile(self.filepath):
            raise Exception(f'File 【{self.filepath}】 not exists !')

        with open(self.filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        return content

    def parse_argument(self, argument):
        argument_map = {
            'today': 'date.today()',
            'now': 'datetime.now()',
        }
        key, value = argument.split('=', 1)
        key, value = key.strip(), value.strip()
        value = argument_map.get(value, value)
        try:
            value = eval(value)
        except NameError as e:
            raise NameError(f"{e}, please set it before '{key}' !!!")

        globals()[key] = value # 设置为全局变量用于后续变量的获取
        return key, value

    @property
    def arguments(self):
        '''[summary]
        
        [description]
            获取文件中配置的arguments
        Returns:
            [dict] -- [返回文件中的参数设置]
        '''
        arguments = {}
        content = self.get_content()
        content = re.sub('--.*?\n', '\n', content) #去掉注释
        arguments_temp = re.findall(r'(?<!--)\s+#【arguments】#\n(.*?)#【arguments】#', content, re.S)
        arguments_temp = ';'.join(arguments_temp).replace('\n', ';')
        arguments_temp = [argument.strip() for argument in arguments_temp.split(';') if argument]
        for argument in arguments_temp:
            key, value = self.parse_argument(argument)
            arguments[key] = value

        arguments = {k: f"'{datetime.strftime(v, '%Y-%m-%d %H:%M:%S')}'" if isinstance(v, datetime)
                        else f"'{datetime.strftime(v, '%Y-%m-%d')}'" if isinstance(v, date)
                        else v for k, v in arguments.items()} # 处理时间
        return arguments

    @property
    def parameters(self):
        content = self.get_content()
        content = re.sub('--.*?\n', '\n', content) #去掉注释
        parameters = re.findall(rf"\$(\w+){self.reg_behind}", content)
        return set(parameters)

    def replace_params(self, **kwargs):
        '''[summary]
        
        [description]
            替换具体的参数值，传入的参数值会覆盖文件中设置的参数值
        Arguments:
            **kwargs {[参数]} -- [传入的参数值]
        
        Returns:
            [str] -- [替换过后的内容]
        
        Raises:
            Exception -- [需要设置参数]
        '''
        kwargs = {k: f"'{v}'" if isinstance(v, str) else v for k, v in kwargs.items()} # str加引号处理
        arguments = self.arguments
        
        arguments_same = set(arguments) & set(kwargs)
        if arguments_same:
            input_arg = {arg: kwargs.get(arg) for arg in arguments_same}
            file_arg = {arg: arguments.get(arg) for arg in arguments_same}
            sqllogger.warning(f"{arguments_same} Use Input arguments {input_arg}, NotUse sqlfile setting {file_arg}!")

        arguments.update(kwargs)
        arguments_lack = self.parameters - set(arguments)
        if arguments_lack:
            raise Exception(f"Need params 【{'】, 【'.join(arguments_lack)}】 !")

        content = self.get_content()
        for key, value in arguments.items():
            content = re.sub(rf"\${key}{self.reg_behind}", f"{value}", content)
        sqllogger.info(f"【Final Arguments】The file 【{os.path.basename(self.filepath)}】 Use arguments {arguments}")
        return content

    def get_sqls(self, **kwargs):
        sqls = {}
        content = self.replace_params(**kwargs)
        sqls_tmp = re.findall(r'(?<!--)\s+###\n(.*?)###', content, re.S)
        for idx, sql in enumerate(sqls_tmp):
            purpose = re.match('--(【.*?】)\n', sql.strip())
            purpose = f"{purpose.group(1)}--第{idx+1}个SQL" if purpose else f'【No description】--第{idx+1}个SQL'
            sql = re.sub('--【.*?\n', '', sql.strip())
            sqls[purpose] = sql
        return sqls

