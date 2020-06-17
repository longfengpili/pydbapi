# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-03 10:51:08
# @Last Modified time: 2020-06-17 21:08:18
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import re
import os

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
    def params(self):
        content = self.get_content()
        content = re.sub('--.*?\n', '\n', content) #去掉注释
        params = re.findall(rf"\$(\w+){self.reg_behind}", content)
        return set(params)

    def replace_params(self, **kw):
        params_diff = self.params - set(kw)
        if params_diff:
            raise Exception(f"Need params 【{'】, 【'.join(params_diff)}】 !")

        content = self.get_content()
        for key, value in kw.items():
            if re.search(rf'(?<!in )(\${key})', content): # 检查是否有非in的情况
                content = re.sub(rf"(?<!in )\${key}{self.reg_behind}", f"'{value}'", content)
            else:
                value = "('" + "', '".join([v for v in value.split(',')]) + "')"
                content = re.sub(rf"(?<=in )\${key}{self.reg_behind}", f"{value}", content)
        return content

    def get_sqls(self, **kw):
        sqls = {}
        content = self.replace_params(**kw)
        sqls_tmp = re.findall(r'(?<!--)\s+###\n(.*?)###', content, re.S)
        for idx, sql in enumerate(sqls_tmp):
            purpose = re.match('--(【.*?)\n', sql.strip())
            purpose = purpose.group(1) if purpose else f'No description {idx}'
            sql = re.sub('--【.*?\n', '', sql.strip())
            sqls[purpose] = sql
        return sqls



