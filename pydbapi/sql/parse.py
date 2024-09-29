# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-09-29 11:45:27
# @github: https://github.com/longfengpili


import re
import os
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Tuple

import logging
sqllogger = logging.getLogger(__name__)


class SqlParse(object):

    def __init__(self, orisql):
        self.orisql = orisql

    @property
    def purpose(self):
        purpose = re.match('-- *(【.*?】)\n', self.orisql.strip())
        purpose = f"{purpose.group(1)}" if purpose else 'No Description'
        return purpose

    @property
    def sql(self):
        sql = re.sub('^--.*?\n', '\n', self.orisql.strip() + '\n')
        sql = re.sub('--.*?\n', '\n', sql)  # 去掉注释
        return sql.strip().rstrip(';')

    @staticmethod
    def split_sqls(sql):
        sql = sql.strip()
        sql = sql if sql.endswith(';') else sql + ';'
        sql_r1 = re.sub('; *\n', ';\n', sql)  # 处理多余的；
        sql_r2 = re.sub(';$', ';\n', sql_r1)  # 末尾增加'\n'
        sqls = re.split(';\n', sql_r2)[:-1]
        return sqls

    @property
    def comment(self):
        orisql = self.orisql.strip()
        orisql = orisql if orisql.endswith(';') else orisql + ';'
        comment = re.search('^-- *(.*?)\n', orisql)
        comment = comment.group(1).strip() if comment else ''
        return comment

    @property
    def action(self):
        if not self.sql:
            return 'NoSql'

        sql = self.sql.replace('\n', '')
        splits = sql.split(' ')
        action = splits[0]
        if splits[1].lower() == 'index':
            action += splits[1]
        return action.upper()

    @property
    def tablename(self):
        sql = self.sql
        patterns = [
            r'create (?:temp |temporary )?table (?:if not exists |if exists )?([\w.]+)',
            r'update\s+([\w.]+)',
            r'delete\s+(?:from\s+)?([\w.]+)',
            r'insert\s+into\s+([\w.]+)',
            r'from\s+([\w.]+)',
            r'on\s+([\w.]+)',
            r'join\s+([\w.]+)',  # 增加对 JOIN 语句的支持
            r'\(\s*select\s+.*?\s+from\s+([\w.]+)',  # 支持 SELECT 子查询
            r',\s*([\w.]+)'  # 支持多个表名的情况
        ]

        for pattern in patterns:
            match = re.search(pattern, sql, re.I)
            if match:
                return match.group(1)
                
        return sql

    @property
    def split_withsqls(self):
        sql = self.sql
        if not sql.lower().startswith('with'):
            raise Exception(f"Sql should startswith [with], but startswith [{sql[:4]}] !!!")

        sqls = re.split(r'(?<=\)), *\n+.*?\n*(?=.*?as *\n)', sql)
        if sqls[-1].startswith('select'):
            sql_last = sqls[-2] + sqls[-1]
            sqls = sqls[:-2] + [sql_last]
        return sqls

    @property
    def combination_sqls(self):
        combination_sqls = []
        sqls = self.split_withsqls
        for idx, sql in enumerate(sqls):
            tablename = re.search(r'(?:with )?(.*?)(?: as)', sql)
            tablename = tablename.group(1) if tablename else ''

            # 生成注释内容和SELECT语句
            content = f"-- {tablename}_{idx + 1:03d}"
            sql_select = f'select * from {tablename} limit 10' if idx + 1 < len(sqls) else 'limit 10'

            # 组合前面的SQL
            combined_sql = ',\n'.join(sqls[:idx+1])
            combined_sql = f'{content}\n{combined_sql}\n{sql_select}'

            combination_sqls.append(combined_sql)
        return combination_sqls

    @property
    def parameters(self):
        sql = self.sql
        parameters = re.findall(r"\$(\w+)", sql)
        return set(parameters)

    def substitute_parameters(self, **kwargs):
        '''[summary]

        [description]
            替换具体的参数值，传入的参数值会覆盖文件中设置的参数值
        Arguments:
            **kwargs {[参数]} -- [传入的参数值]

        Returns:
            [str] -- [替换过后的内容]

        '''
        params_diff = set(self.parameters) - set(kwargs)
        if params_diff:
            missing_params = ', '.join(params_diff)
            raise Exception(f"Missing parameters: {missing_params}. Please provide values for all parameters.")

        sql = self.sql
        for key, value in kwargs.items():
            sql = re.sub(rf"\${key}", f"{value}", sql)

        return sql


class SqlFileParse(object):

    def __init__(self, filepath):
        self.filepath = filepath

    def get_content(self):
        if not os.path.isfile(self.filepath):
            raise Exception(f'File 【{self.filepath}】 not exists !')

        with open(self.filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        return content

    def parse_argument(self, argument: str, arguments: Dict[str, Any]) -> Tuple[str, Any]:
        key, value = argument.split('=', 1)
        key, value = key.strip(), value.strip()
        try:
            globals_value = {'timedelta': timedelta}
            value = eval(value, globals_value, arguments)
        except NameError as e:
            raise NameError(f"{e}, please set it before '{key}' !!!")
        return key, value

    def get_arguments_infile(self, content: str):
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
        arguments_infile = re.findall(r'(?<!--)\s*#【arguments】#\s*\n(.*?)#【arguments】#', content, re.S)
        arguments_infile = ';'.join(arguments_infile).replace('\n', ';')
        arguments_infile = [argument.strip() for argument in arguments_infile.split(';') if argument]
        for argument in arguments_infile:
            if argument.startswith('--'):
                continue

            key, value = self.parse_argument(argument, arguments)
            arguments[key] = value

        arguments = {k: f"'{datetime.strftime(v, '%Y-%m-%d %H:%M:%S')}'" if isinstance(v, datetime)
                        else f"'{datetime.strftime(v, '%Y-%m-%d')}'" if isinstance(v, date)
                        else v for k, v in arguments.items()}  # 处理时间
        # print(arguments)
        return arguments

    def get_sqls_infile(self, content: str):
        sqls = re.findall(r'(?<!--)\s*###\s*\n(.*?)###', content, re.S)
        return sqls

    def parse_file(self):
        content = self.get_content()
        arguments = self.get_arguments_infile(content)
        sqls = self.get_sqls_infile(content)
        return arguments, sqls

    def update_arguments(self, arguments_infile: Dict[str, Any], **kwargs: Dict[str, Any]) -> Dict[str, Any]:
        '''[summary]

        [description]
            替换具体的参数值，传入的参数值会覆盖文件中设置的参数值
        Arguments:
            **kwargs {[参数]} -- [传入的参数值]

        Returns:
            [str] -- [替换过后的内容]

        '''
        # 获取文件名
        filename = os.path.basename(self.filepath)
        
        # 过滤掉值为空的参数
        kwargs = {k: v for k, v in kwargs.items() if v}
        # 组合文件参数和传入参数
        arguments_final = {**arguments_infile, **kwargs}

        # 记录最终参数的日志
        if arguments_final:
            arglog = f"【Final Arguments】【{filename}】 Use arguments {arguments_final}"
        else:
            arglog = f"【Final Arguments】【{filename}】 no arguments !!!"

        # 计算文件参数和传入参数的交集，并记录日志
        arguments_same = set(arguments_infile) & set(kwargs)
        if arguments_same:
            arguments_same = sorted(arguments_same)
            file_arg = {arg: arguments_infile.get(arg) for arg in arguments_same}
            argsamelog = f"Replace FileSetting {file_arg}"
            arglog = f"{arglog}, {argsamelog}"
        
        sqllogger.warning(arglog)

        return arguments_final

    def get_filesqls(self, with_test: bool = False, with_snum: int = 0, **kwargs: Any) -> Tuple[Dict[str, Any], Dict[str, str]]:
        fsqls = {}
        content = self.get_content()
        arguments_infile = self.get_arguments_infile(content)
        farguments = self.update_arguments(arguments_infile, **kwargs)
        sqls = self.get_sqls_infile(content)

        for idx, sql in enumerate(sqls):
            sqlparser = SqlParse(sql)
            purpose = f"【{idx + 1:0>3d}】{sqlparser.purpose}"
            sql = sqlparser.substitute_parameters(**farguments)

            if with_test:
                combination_sqls = sqlparser.combination_sqls
                fsqls[f"{purpose}_{with_snum:03d}"] = combination_sqls[with_snum-1]
            else:
                fsqls[purpose] = sql

        return farguments, fsqls
