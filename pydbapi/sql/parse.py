# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-03 10:51:08
# @Last Modified time: 2021-08-05 11:44:44
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import re
import os

from datetime import datetime, date, timedelta

import logging
sqllogger = logging.getLogger(__name__)


class SqlParse(object):

    def __init__(self, orisql):
        self.orisql = orisql
        self.reg_behind = r'(?=[,);:\s])'

    @property
    def sql(self):
        sql = re.sub('^(--.*?\n){0,}', '', self.orisql.strip() + '\n')
        return sql.strip()

    @property
    def comment(self):
        comment = self.orisql.replace(self.sql, '')
        comment = re.search('-- *(.*?)$', comment.strip())
        comment = comment.group(1) if comment else ''
        return comment.strip()

    @property
    def action(self):
        action = self.sql.split(' ')[0]
        return action.upper()

    @property
    def tablename(self):
        sql = self.sql
        create = re.search(r'table (?:if exists |if not exists )?(.*?)(?:\s|;|$)', sql)
        update = re.search(r'update (.*?)(?:\s|;|$)', sql)
        insert = re.search(r'insert into (.*?)(?:\s|;|$)', sql)
        delete = re.search(r'delete (?:from )?(.*?)(?:\s|;|$)', sql)
        select = re.search(r'select.*?from (.*?)(?:\s|;|$)', sql, re.S)
        tablename = create or update or insert or delete or select
        tablename = tablename.group(1) if tablename else sql
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

    def parse_argument(self, argument, arguments):
        key, value = argument.split('=', 1)
        key, value = key.strip(), value.strip()
        try:
            value = eval(value, globals(), arguments)
        except NameError as e:
            raise NameError(f"{e}, please set it before '{key}' !!!")
        return key, value

    @property
    def arguments(self):
        '''[summary]

        [description]
            获取文件中配置的arguments
        Returns:
            [dict] -- [返回文件中的参数设置]
        '''
        arguments = {
            'today': date.today(),
            'now': datetime.now(),
        }
        content = self.get_content()
        content = re.sub('--.*?\n', '\n', content)  # 去掉注释
        arguments_infile = re.findall(r'(?<!--)\s+#【arguments】#\n(.*?)#【arguments】#', content, re.S)
        arguments_infile = ';'.join(arguments_infile).replace('\n', ';')
        arguments_infile = [argument.strip() for argument in arguments_infile.split(';') if argument]
        for argument in arguments_infile:
            key, value = self.parse_argument(argument, arguments)
            arguments[key] = value

        arguments = {k: f"'{datetime.strftime(v, '%Y-%m-%d %H:%M:%S')}'" if isinstance(v, datetime)
                        else f"'{datetime.strftime(v, '%Y-%m-%d')}'" if isinstance(v, date)
                        else v for k, v in arguments.items()}  # 处理时间
        return arguments

    @property
    def parameters(self):
        content = self.get_content()
        content = re.sub('--.*?\n', '\n', content)  # 去掉注释
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
        filename = os.path.basename(self.filepath)
        kwargs = {k: f"'{v}'" if isinstance(v, str) else v for k, v in kwargs.items()}  # str加引号处理
        arguments = self.arguments

        arguments_same = set(arguments) & set(kwargs)
        argsamelog = None
        if arguments_same:
            arguments_same = sorted(arguments_same)
            # input_arg = {arg: kwargs.get(arg) for arg in arguments_same}
            file_arg = {arg: arguments.get(arg) for arg in arguments_same}
            argsamelog = f"Replace FileSetting {file_arg}"
            # sqllogger.warning(f"File 【{filename}】 {arguments_same} Use Input arguments {input_arg}, NotUse sqlfile setting {file_arg}!")

        arguments.update(kwargs)
        arguments_lack = self.parameters - set(arguments)
        if arguments_lack:
            raise Exception(f"Need params 【{'】, 【'.join(arguments_lack)}】 !")

        content = self.get_content()
        for key, value in arguments.items():
            content = re.sub(rf"\${key}{self.reg_behind}", f"{value}", content)
        arguments = {k: arguments.get(k) for k in self.parameters}

        arglog = f"【Final Arguments】【{filename}】 Use arguments {arguments}" \
                 if arguments else f"【Final Arguments】【{filename}】 no arguments !!!"
        arglog = arglog + f", {argsamelog}" if argsamelog else arglog
        sqllogger.warning(arglog)

        return arguments, content

    def get_filesqls(self, **kwargs):
        sqls = {}
        arguments, content = self.replace_params(**kwargs)
        sqls_tmp = re.findall(r'(?<!--)\s+###\n(.*?)###', content, re.S)
        for idx, sql in enumerate(sqls_tmp):
            purpose = re.match('-- *(【.*?】)\n', sql.strip())
            purpose = f"【{idx+1:0>3d}】{purpose.group(1)}" if purpose else f'【{idx+1:0>3d}】【No description】'
            sql = re.sub('--【.*?\n', '', sql.strip())
            sqls[purpose] = sql
        return arguments, sqls
