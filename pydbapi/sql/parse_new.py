# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2024-10-09 16:33:05
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-11-19 15:55:17
# @github: https://github.com/longfengpili


import re

import sqlparse
from sqlparse.sql import Statement, Token, TokenList, Identifier, IdentifierList, Comment
from sqlparse.tokens import DML, DDL, CTE
from sqlparse.tokens import Keyword, Newline, Punctuation

import logging
sqllogger = logging.getLogger(__name__)


class SqlStatement:

    def __init__(self, sql: str):
        self._sql = sql

    @property
    def tokens(self):
        sp = sqlparse.parse(self._sql)[0]
        return sp.tokens

    @property
    def sql(self):
        sql = sqlparse.format(self._sql, keyword_case='lower', strip_comments=True, use_space_around_operators=True)
        return sql.strip()

    @property
    def comment(self):
        for token in self.tokens:
            if token.ttype == Newline:
                continue
            if isinstance(token, Comment):
                comment = token.value
                comment = re.search(r'^[-#]*(.*?)$', comment, re.S)
                if comment:
                    comment = comment.group(1).strip()
                return comment
        return 'NoComment'

    @property
    def action(self):
        for token in self.tokens:
            if token.ttype in (DML, DDL, CTE):
                return token.value
        return 'NoAction'

    @property
    def tablename(self):
        from_seen = True
        for token in self.tokens:
            if from_seen and isinstance(token, Identifier):
                tablename = token.value
                if ' ' in tablename:
                    tablename = token.get_real_name()
                return tablename
            elif token.ttype in (DML, DDL, CTE):
                if token.value.lower() == 'select':
                    from_seen = False
            elif token.ttype is Keyword and token.value.lower() in ('from', 'join', 'into'):
                from_seen = True
            else:
                pass

        return 'NoTablename'

    @property
    def params(self):
        params = re.findall(r"\$(\w+)", self.sql)
        return set(params)

    def substitute_params(self, **kwargs):
        '''[summary]

        [description]
            替换具体的参数值，传入的参数值会覆盖文件中设置的参数值
        Arguments:
            **kwargs {[参数]} -- [传入的参数值]

        Returns:
            [str] -- [替换过后的内容]

        '''
        params_diff = set(self.params) - set(kwargs)
        if params_diff:
            missing_params = ', '.join(params_diff)
            raise Exception(f"Missing params: {missing_params}. Please provide values for all params.")

        for key, value in kwargs.items():
            self._sql = re.sub(rf"\${key}", f"{value}", self._sql)

        return self

