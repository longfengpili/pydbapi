# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-03 10:51:08
# @Last Modified time: 2020-06-28 12:05:43
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import re
import os

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
        arguments_temp = re.findall(r'(?<!--)\s+#【argument】#\n(.*?)#【argument】#', content, re.S)
        arguments_temp = ';'.join(arguments_temp).replace('\n', ';')
        arguments_temp = [argument.strip() for argument in arguments_temp.split(';') if argument]
        for argument in arguments_temp:
            arg, value = argument.split('=', 1)
            arguments[arg.strip()] = value.strip()
        return arguments

    @property
    def parameters(self):
        content = self.get_content()
        content = re.sub('--.*?\n', '\n', content) #去掉注释
        parameters = re.findall(rf"\$(\w+){self.reg_behind}", content)
        return set(parameters)

    def replace_params(self, **kw):
        '''[summary]
        
        [description]
            替换具体的参数值，传入的参数值会覆盖文件中设置的参数值
        Arguments:
            **kw {[参数]} -- [传入的参数值]
        
        Returns:
            [str] -- [替换过后的内容]
        
        Raises:
            Exception -- [需要设置参数]
        '''
        arguments = self.arguments
        arguments_same = set(arguments) & set(kw)
        if arguments_same:
            sqllogger.warning(f"{arguments_same} will use the func input arguments, not use sqlfile setting !")

        arguments.update(kw)
        params_diff = self.parameters - set(arguments)
        if params_diff:
            raise Exception(f"Need params 【{'】, 【'.join(params_diff)}】 !")

        content = self.get_content()
        for key, value in arguments.items():
            content = re.sub(rf"\${key}{self.reg_behind}", f"{value}", content)
        sqllogger.warning(f"The file {os.path.basename(self.filepath)} use arguments {arguments}")
        return content

    def get_sqls(self, **kw):
        sqls = {}
        content = self.replace_params(**kw)
        sqls_tmp = re.findall(r'(?<!--)\s+###\n(.*?)###', content, re.S)
        for idx, sql in enumerate(sqls_tmp):
            purpose = re.match('--(【.*?】)\n', sql.strip())
            purpose = f"{purpose.group(1)}--第{idx+1}个SQL" if purpose else f'【No description】--第{idx+1}个SQL'
            sql = re.sub('--【.*?\n', '', sql.strip())
            sqls[purpose] = sql
        return sqls

