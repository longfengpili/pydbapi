# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2024-10-09 16:33:05
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-11-20 16:45:15
# @github: https://github.com/longfengpili


import re

import sqlparse
from sqlparse.sql import Token, TokenList, Identifier, IdentifierList, Comment
from sqlparse.tokens import DML, DDL, CTE
from sqlparse.tokens import Keyword, Newline, Punctuation

import logging
sqllogger = logging.getLogger(__name__)


class SqlStatement:

    def __init__(self, sql: str):
        self._sql = sql
        self._parsed = sqlparse.parse(self._sql)[0]

    def __repr__(self):
        _repr = f"[{self.action}]{self.tablename}"
        _repr = f"{_repr}::{self.comment}" if self.comment else _repr
        return _repr

    @classmethod
    def from_sqlsnippets(cls, *sqlsnippets: tuple[str, ]) -> 'SqlStatement':
        sql = '\n'.join(sqlsnippets)
        return cls(sql)

    @property
    def tokens(self):
        return self._parsed.tokens

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
                    comment = comment.replace('\n', ' ')
                return comment
        # return 'NoComment'

    @property
    def action(self):
        for token in self.tokens:
            if token.ttype in (DML, DDL, CTE):
                return token.value.lower()

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

    @property
    def params(self):
        params = re.findall(r"\$(\w+)", self._sql)
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

    def get_subqueries(self, tokens: list[Token], 
                       subtokens: list[Token] = None, 
                       subqueries: list[TokenList, ] = None, 
                       keep_last: bool = True) -> list[TokenList]:
        def append_subquery(subtokens: list, subqueries: list):
            _subqueries = subtokens.copy()  # subtokens会clear，所以需要复制到另外的变量
            identifier = Identifier(_subqueries)
            tokenfirst = identifier.token_first(skip_cm=True)
            if tokenfirst:
                subqueries.append(identifier)
            subtokens.clear()

        if subqueries is None:
            subqueries = []
        if subtokens is None:
            subtokens = []
        islast = False

        for token in tokens:
            # print(f"{type(token)}::{token.ttype}::{token.value}")  
            if isinstance(token, Comment) or (token.ttype == Newline and not subtokens):
                continue

            if token.ttype in (DML, DDL, CTE):
                append_subquery(subtokens, subqueries)
                subtokens.append(token)
                if token.value.lower() == 'select':
                    islast = True
            elif token.ttype == Punctuation and not islast:
                append_subquery(subtokens, subqueries)
            elif isinstance(token, Identifier):  # Identifier 也是 TokenList, 所有必须在下个判断之前
                subtokens.append(token)
            elif isinstance(token, (TokenList, IdentifierList)):
                self.get_subqueries(token.tokens, subtokens, subqueries)
            else:
                subtokens.append(token)

        if keep_last: 
            append_subquery(subtokens, subqueries)

        return subqueries

    @property
    def subqueries(self):
        tokens = self.tokens
        subqueries = self.get_subqueries(tokens, keep_last=False)
        return subqueries

    def get_combination_sql(self, idx: int = 0):
        combination_sqls = []
        subqueries = self.subqueries
        for idx, identifier in enumerate(subqueries):
            tablename = identifier.get_real_name()

            # 生成注释内容和SELECT语句
            comment = f"-- {tablename}_{idx + 1:03d}"
            selectsql = f'select * from {tablename} limit 10'
            # 组合前面的SQL
            sqlsnippets = ',\n'.join([subquery.value for subquery in subqueries[:idx+1]])

            combined_sql = SqlStatement.from_sqlsnippets(comment, sqlsnippets, selectsql)

            combination_sqls.append(combined_sql)
        return combination_sqls


class SqlParse:

    def __init__(self, sql: str):
        self.sql = sql
        self._statements = None

    def __str__(self):
        return f"[Statement: {len(self._statements)}]{self._statements[0]}..."

    def __repr__(self):
        return f"[Statement: {len(self._statements)}]{self._statements[0]}..."

    @property
    def statements(self) -> list[SqlStatement, ]:
        if self._statements is None:
            sqls = [sql.strip() for sql in self.sql.split(';')]
            self._statements = [SqlStatement(sql) for sql in sqls if sql]
            if len(self._statements) > 1:
                sqllogger.warning(f'SQL has {len(self._statements)} statements ~')
        return self._statements

    def get_statement(self, idx: int = 0):
        statements = self.statements
        stmt = statements[idx]
        return stmt

    def get_combination_sql(self, idx: int = 0):
        stmt = self.get_statement()
        combination_sql = stmt.combination_sqls[idx]
        return combination_sql

    def substitute_params(self, **kwargs):
        stmts = self.statements
        self._statements = [stmt.substitute_params(**kwargs) for stmt in stmts if stmt]
        return self
