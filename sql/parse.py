# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-03 10:51:08
# @Last Modified time: 2020-06-03 18:13:25
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import re

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
        return action

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


