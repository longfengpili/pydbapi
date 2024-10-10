# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2024-10-09 16:33:05
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-10-10 19:02:04
# @github: https://github.com/longfengpili


import re

import sqlparse
from sqlparse.sql import Statement, TokenList, Identifier, IdentifierList, Comment
from sqlparse.tokens import DML, DDL, CTE
from sqlparse.tokens import Keyword, Newline, Punctuation

import logging
sqllogger = logging.getLogger(__name__)


class SqlParse:

    def __init__(self, orisql: str):
        self.orisql = orisql

    @property
    def statements(self):
        statements = sqlparse.parse(self.orisql)
        return statements

    @property
    def sql(self):
        sql = sqlparse.format(self.orisql, keyword_case='lower', strip_comments=True, use_space_around_operators=True)
        return sql

    def get_statement(self, idx: int = 0):
        statements = self.statements
        stmt = statements[idx]
        if len(statements) > 1 and idx > 0:
            sqllogger.warning(f'SQL has {len(statements)} statements, only parse NO.{idx + 1} statement ~')
        return stmt

    def get_first_comment(self):
        first_stmt = self.get_statement()
        for token in first_stmt.tokens:
            if token.ttype == Newline:
                continue
            elif isinstance(token, Comment):
                first_comment = token.value
                return first_comment
            else:
                break

    @property
    def purpose(self):
        purpose = 'No Description'
        first_comment = self.get_first_comment()
        purpose = re.search(r'--(.*?)\n', first_comment)
        if purpose:
            purpose = purpose.group(1).strip()
        return purpose

    @property
    def comment(self):
        comment = 'No Description'
        first_comment = self.get_first_comment()
        comment = re.search(r'--.*?--(.*?)$', first_comment, re.S)
        if comment:
            comment = comment.group(1).strip()
        return comment

    @property
    def action(self):
        first_stmt = self.get_statement()
        for token in first_stmt.tokens:
            if token.ttype in (DML, DDL, CTE):
                return token.value

    def get_tables(self, tokenlist: [TokenList, Statement]):
        tables = []
        from_seen = True
        for token in tokenlist.tokens:
            if from_seen and isinstance(token, Identifier):
                tablename = token.value
                if not re.match(r'([\w_]+\.)*\w+$', tablename):
                    tablename = token.get_real_name()
                tables.append(tablename)
            if token.ttype == DML and token.value.lower() == 'select':
                from_seen = False
            if token.ttype is Keyword and token.value.lower() in ('from', 'join', 'into', 'update', 'delete'):
                from_seen = True

            # 暂时不考虑IdentifierList，后面再解决
            # if isinstance(token, IdentifierList):
            #     for identifier in token.get_identifiers():
            #         tables.append(identifier.get_real_name())
        return tables

    @property
    def tablename(self):
        first_stmt = self.get_statement()
        tables = self.get_tables(first_stmt)
        return tables[0]

    def get_subqueries(self, keep_last: bool = True):
        def append_subquery(subtokens: list, subqueries: list):
            subtokens = TokenList(subtokens)
            subtoken_first = subtokens.token_first(skip_cm=True)
            if subtoken_first:
                subqueries.append(subtokens)

        subqueries = []
        subtokens = []
        islast = False

        first_stmt = self.get_statement()
        for token in first_stmt.tokens:
            if isinstance(token, Comment):
                continue
            elif token.ttype in (DML, DDL, CTE):
                append_subquery(subtokens, subqueries)
                if token.value.lower() == 'select':
                    islast = True
                subtokens = [token]
            elif token.ttype == Punctuation and not islast:
                append_subquery(subtokens, subqueries)
                subtokens = []
            else:
                subtokens.append(token)

        if keep_last:
            append_subquery(subtokens, subqueries)

        return subqueries

    @property
    def combination_sqls(self):
        combination_sqls = []
        subqueries = self.get_subqueries(keep_last=False)
        for idx, tokenlist in enumerate(subqueries):
            tables = self.get_tables(tokenlist)
            tablename = tables[0]

            # 生成注释内容和SELECT语句
            content = f"-- {tablename}_{idx + 1:03d}"
            sql_select = f'select * from {tablename} limit 10'

            # 组合前面的SQL
            combined_sql = ',\n'.join([subquery.value for subquery in subqueries[:idx+1]])
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
