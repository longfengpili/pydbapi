# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2024-10-09 16:33:05
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-11-21 10:07:06
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
        self._sql = sql.strip()
        self._parsed = sqlparse.parse(self._sql)[0]

    def __repr__(self):
        _repr = f"[{self.action}]{self.tablename}"
        _repr = f"{_repr}::{self.comment}" if self.comment else _repr
        return _repr

    def __sub__(self, sqlstmt: str):
        self._sql = self._sql.strip(';')
        if self._sql.endswith(sqlstmt):
            self._sql = self._sql[:-len(sqlstmt)]
        self._parsed = sqlparse.parse(self._sql)[0]
        return self

    def __add__(self, sqlstmt: str):
        sql = self._sql.strip(';')
        self._sql = f"{sql}\n{sqlstmt};"
        self._parsed = sqlparse.parse(self._sql)[0]
        return self

    @classmethod
    def from_sqlsnippets(cls, *sqlsnippets: tuple[str, ]) -> 'SqlStatement':
        sql = '\n'.join(map(str.strip, sqlsnippets))
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
                comment = re.sub(r'^[-# \t]*', '', comment, flags=re.M).strip()
                return comment.replace('\n', ' ')
        # return 'NoComment'

    @property
    def action(self):
        for token in self.tokens:
            if token.ttype in (DML, DDL, CTE):
                return token.value.lower()

    @property
    def tablename(self):
        from_seen = False
        for token in self.tokens:
            if from_seen and isinstance(token, Identifier):
                return token.get_real_name() or token.value
            elif token.ttype in (DML, DDL, CTE):
                if token.value.lower() == 'select':
                    from_seen = False
            elif token.ttype is Keyword and token.value.lower() in ('from', 'join', 'into'):
                from_seen = True

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

        self._parsed = sqlparse.parse(self._sql)[0]  # Update parsed SQL
        return self

    def get_subqueries(self, tokens: list[Token], 
                       subtokens: list[Token] = None, 
                       subqueries: list[TokenList, ] = None, 
                       keep_last: bool = True) -> list[TokenList]:
        def append_subquery(subtokens: list, subqueries: list):
            _subqueries = subtokens.copy()  # subtokens会clear，所以需要复制到另外的变量
            identifier = Identifier(_subqueries)
            if identifier.token_first(skip_cm=True):
                subqueries.append(identifier)
            subtokens.clear()

        if subqueries is None:
            subqueries = []
        if subtokens is None:
            subtokens = []

        for token in tokens:
            # print(f"{type(token)}::{token.ttype}::{token.value}")  
            if isinstance(token, Comment) or (token.ttype == Newline and not subtokens):
                continue

            if token.ttype in (DML, DDL, CTE):
                append_subquery(subtokens, subqueries)
                subtokens.append(token)
            elif token.ttype == Punctuation:
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
        subqueries = self.get_subqueries(self.tokens, keep_last=False)
        return subqueries

    def get_combination_sql(self, idx: int = 0):
        subqueries = self.subqueries
        last_subquery = subqueries[idx]
        tablename = last_subquery.get_real_name()

        # 生成注释内容和SELECT语句
        comment = f"-- {tablename}_{idx + 1:03d}"
        selectsql = f'select * from {tablename} limit 10'
        # 组合前面的SQL
        sqlsnippets = ',\n'.join([subquery.value for subquery in subqueries[:idx+1]])

        return SqlStatement.from_sqlsnippets(comment, sqlsnippets, selectsql)


class SqlStatements:

    def __init__(self, sql: str):
        self.sql = sql.strip()
        self._statements = None

    def __str__(self):
        return f"[Statement: {len(self)}]{self[0]}..."

    def __repr__(self):
        return self.__str__()

    def __iter__(self):
        return iter(self.statements)

    def __len__(self):
        return len(self.statements)

    def __getitem__(self, index):
        return self.statements[index]

    @classmethod
    def from_sqlstatements(cls, *statements):
        sqls = [stmt._sql for stmt in statements]
        sql = ';'.join(sqls)
        return cls(sql)

    @property
    def statements(self) -> list[SqlStatement, ]:
        if self._statements is None:
            self._statements = [SqlStatement(sql) for sql in self.sql.split(';') if sql.strip()]
            if len(self._statements) > 1:
                sqllogger.warning(f'SQL has {len(self._statements)} statements ~')
        return self._statements

    def get_statement(self, idx: int = 0) -> SqlStatement:
        statements = self.statements
        stmt = statements[idx]
        return stmt

    def get_combination_sql(self, idx: int = 0) -> SqlStatement:
        return self.get_statement().combination_sqls[idx]

    def substitute_params(self, **kwargs):
        self._statements = [stmt.substitute_params(**kwargs) for stmt in self.statements if stmt]
        return self
