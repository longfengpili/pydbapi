# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2024-10-09 16:33:05
# @Last Modified by:   longfengpili
# @Last Modified time: 2025-03-21 18:29:16
# @github: https://github.com/longfengpili


import re

import sqlparse
from sqlparse.sql import Token, TokenList, Identifier, IdentifierList, Comment
from sqlparse.tokens import DML, DDL, CTE
from sqlparse.tokens import Keyword, Newline, Punctuation, Comment as CommentToken

import logging
sqllogger = logging.getLogger(__name__)


class SqlStatement:

    def __init__(self, sql: str):
        self._sql = sql.strip()
        self._parsed = sqlparse.parse(sql)[0]

    def __repr__(self):
        _repr = f"<{self.action}>{self.tablename}"
        _repr = f"{_repr}::{self.comment}" if self.comment else _repr
        return f"SqlStatement({_repr})"

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
        """SQL for execution, preserving comments and string literals."""
        return self._sql

    @property
    def sql_without_terminator(self):
        """Omit top-level statement delimiters for drivers such as Trino."""
        return ''.join(token.value for token in self.tokens
                       if not token.match(Punctuation, ';')).strip()

    @property
    def formatted_sql(self):
        """Formatted SQL for display only; never used by the execution path."""
        # keyword_case='lower'：将 SQL 关键字转为小写。
        # strip_comments=True：去除所有注释。
        # use_space_around_operators=True：在运算符周围加空格。
        formatted_sql = sqlparse.format(self._sql, keyword_case='lower', strip_comments=True, use_space_around_operators=True)
        # 手动移除空白行
        non_blank_lines = [line.strip() for line in formatted_sql.splitlines() if line.strip() != '']
        sql = '\n'.join(non_blank_lines)
        return sql.strip(';')

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

    @property
    def params(self):
        params = re.findall(r"\$(\w+)", self.formatted_sql)
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

    def get_with_testsql(self, idx: int = 1):
        """Select up to ten rows from the one-based CTE index, including dependencies."""
        if self.action != 'with':
            raise ValueError('The function only support CTEs')
        # Flatten grouping only; quoted literals remain atomic tokens. This also
        # handles valid CTE names that sqlparse classifies as keywords.
        tokens = list(self._parsed.flatten())
        significant = [(i, token) for i, token in enumerate(tokens)
                       if not token.is_whitespace and token.ttype not in CommentToken]
        position = 1  # WITH
        if position < len(significant) and significant[position][1].normalized == 'RECURSIVE':
            position += 1
        ctes = []
        while position < len(significant):
            name = significant[position][1].value
            position += 1
            depth, seen_as, body_started = 0, False, False
            while position < len(significant):
                raw_index, token = significant[position]
                position += 1
                if token.match(Keyword, 'AS') and depth == 0:
                    seen_as = True
                elif token.match(Punctuation, '('):
                    depth += 1
                    body_started = body_started or seen_as
                elif token.match(Punctuation, ')'):
                    depth -= 1
                    if body_started and depth == 0:
                        ctes.append((name, raw_index + 1))
                        break
            else:
                raise ValueError('Invalid CTE definition')
            if position >= len(significant) or not significant[position][1].match(Punctuation, ','):
                break
            position += 1
        if not ctes:
            raise ValueError('No CTE definitions found')
        if isinstance(idx, bool) or not isinstance(idx, int) or not 1 <= idx <= len(ctes):
            raise ValueError(f'CTE index must be between 1 and {len(ctes)}')

        name, end = ctes[idx - 1]
        prefix = ''.join(token.value for token in tokens[:end])
        return SqlStatement(f'{prefix}\nselect * from {name} limit 10;')


class SqlStatements:

    def __init__(self, sql: str):
        self._sql = sql.strip()
        self._statements = None

    def __str__(self):
        return f"SqlStatements([stmt:{len(self)}]{self[0]}, ...)"

    def __repr__(self):
        return self.__str__()

    def __iter__(self):
        return iter(self.statements)

    def __len__(self):
        return len(self.statements)

    def __getitem__(self, index):
        if isinstance(index, slice):
            # 返回新的 `SqlStatements` 包含切片结果
            sliced_statements = self.statements[index]
            sql_strings = '\n'.join([stmt.sql for stmt in sliced_statements])
            return SqlStatements(sql_strings)
        elif isinstance(index, int):
            return self.statements[index]
        else:
            raise TypeError("Invalid argument type.")

    def __add__(self, sqlstmt: str):
        if len(self) == 1:
            self.statements[0] += sqlstmt
            return self
        raise Exception("'SqlStatements' object cannot perform add with multiple statements.")

    def __sub__(self, sqlstmt: str):
        if len(self) == 1:
            self.statements[0] -= sqlstmt
            return self
        raise Exception("'SqlStatements' object cannot perform sub with multiple statements.")

    def __getattr__(self, item: str):
        if len(self) == 1:
            single_statement = self.statements[0]
            attribute = getattr(single_statement, item)
            if callable(attribute):
                def wrapped(*args, **kwargs):
                    return attribute(*args, **kwargs)
                return wrapped
            return attribute
        raise AttributeError(f"'SqlStatements' object has no attribute '{item}'")

    @property
    def statements(self) -> list[SqlStatement, ]:
        if self._statements is None:
            statements = [SqlStatement(sql) for sql in sqlparse.split(self._sql) if sql.strip()]
            self._statements = [stmt for stmt in statements if stmt._parsed.token_first(skip_cm=True)]
            if len(self._statements) > 1:
                sqllogger.warning(f'SQL has {len(self._statements)} statements ~')
        return self._statements

    def substitute_params(self, **kwargs):
        self._statements = [stmt.substitute_params(**kwargs) for stmt in self.statements if stmt]
        return self
