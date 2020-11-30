# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-11-30 16:28:21
# @Last Modified time: 2020-11-30 19:04:30
# @github: https://github.com/longfengpili

# !/usr/bin/env python3
# -*- coding:utf-8 -*-


class ColumnModel(object):

    def __init__(self, newname, coltype, oldname, sqlexpr, func=None, order=0):
        self.newname = newname
        self.coltype = coltype
        self.oldname = oldname
        self.sqlexpr = sqlexpr
        self.func = func
        self.order = order

    def __repr__(self):
        return f"{self.newname}({self.coltype})"

    @property
    def func_sqlexpr(self):
        if self.func:
            func_sqlexpr = f"{self.func}({self.sqlexpr}) as {self.newname}"
            return func_sqlexpr

    @property
    def create_sqlexpr(self):
        create_sqlexpr = f"{self.newname} {self.coltype}"
        return create_sqlexpr


class ColumnsModel(object):

    def __init__(self, *columns):
        self.columns = columns

    @property
    def new_cols(self):
        new_cols = [col.newname for col in self.columns]
        new_cols = ', '.join(new_cols)
        return new_cols

    @property
    def create_cols(self):
        create_cols = [col.create_sqlexpr for col in self.columns]
        create_cols = '(' + '),\n('.join(create_cols) + ')'
        return create_cols

    @property
    def nonfunc_cols(self):
        nonfunc_cols = [col.sqlexpr for col in self.columns if not col.func]
        nonfunc_cols = ',\n'.join(nonfunc_cols)
        return nonfunc_cols

    @property
    def func_cols(self):
        func_cols = [col.func_sqlexpr for col in self.columns if col.func]
        func_cols = ',\n'.join(func_cols)
        return func_cols

    @property
    def select_cols(self):
        select_cols = self.nonfunc_cols + ',\n' + self.func_cols if self.func_cols else self.nonfunc_cols
        return select_cols

    @property
    def order_cols(self):
        order_cols = [col for col in self.columns if col.order]
        order_cols = [col.newname for col in sorted(order_cols, key=lambda x: [x.order, x.newname])]
        order_cols = ', '.join(order_cols)
        return order_cols
