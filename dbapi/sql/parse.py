# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-03 10:51:08
# @Last Modified time: 2020-06-05 17:22:21
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import re
import os

class SqlParse(object):

    def __init__(self, sql):
        self.sql = sql

    @property
    def comment(self):
        comment = re.match('(?:--)(.*?)\n', self.sql.strip())
        comment = comment.group(1) if comment else ''
        return comment

    @property
    def action(self):
        sql = re.sub('--.*?\n', '', self.sql.strip())
        action = sql.strip().split(' ')[0]
        return action.upper()

    @property
    def tablename(self):
        create = re.search('table (?:if exists |if not exists )?(.*?)(?:\s|;|$)', self.sql)
        update = re.search('update (.*?)(?:\s|;|$)', self.sql)
        insert = re.search('insert into (.*?)(?:\s|;|$)', self.sql)
        delete = re.search('delete (?:from )?(.*?)(?:\s|;|$)', self.sql)
        select = re.search('select.*?from (.*?)(?:\s|;|$)', self.sql, re.S)
        tablename = create or update or insert or delete or select
        tablename = tablename.group(1) if tablename else self.sql.strip()
        return tablename


class SqlFileParse(object):

    def __init__(self, filepath):
        self.filepath = filepath
        self.reg_behind = '(?=[,);:\s])'

    def get_content(self):
        if not os.path.isfile(self.filepath):
            raise Exception(f'File 【{self.filepath}】 not exists !')

        with open(self.filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        return content

    @property
    def params(self):
        content = self.get_content()
        content = re.sub('--.*?\n', '\n', content) #去掉注释
        params = re.findall(f"\$(\w+){self.reg_behind}", content)
        return set(params)

    def replace_params(self, **kw):
        params_diff = self.params - set(kw)
        if params_diff:
            raise Exception(f"Need params 【{'】, 【'.join(params_diff)}】 !")

        content = self.get_content()
        for key, value in kw.items():
            if re.search(f'(?<!in )(\${key})', content): # 检查是否有非in的情况
                content = re.sub(f"(?<!in )\${key}{self.reg_behind}", f"'{value}'", content)
            else:
                value = "('" + "', '".join([v for v in value.split(',')]) + "')"
                content = re.sub(f"(?<=in )\${key}{self.reg_behind}", f"{value}", content)
        return content

    def get_sqls(self, **kw):
        sqls = {}
        content = self.replace_params(**kw)
        sqls_tmp = re.findall('(?<!--)\s+###\n(.*?)###', content, re.S)
        for idx, sql in enumerate(sqls_tmp):
            purpose = re.match('--(【.*?)\n', sql.strip())
            purpose = purpose.group(1) if purpose else f'No description {idx}'
            sql = re.sub('--【.*?\n', '', sql.strip())
            sqls[purpose] = sql
        return sqls



