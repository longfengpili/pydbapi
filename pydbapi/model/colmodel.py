# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-03-05 18:33:56
# @github: https://github.com/longfengpili

from typing import Iterable, List, Any


class ColumnModel(object):

    def __init__(self, newname, coltype='varchar', sqlexpr=None, func=None, order=0, desc=None):
        self.newname = newname
        self.coltype = coltype
        self.sqlexpr = sqlexpr or newname
        self.func = self.__check_func(func)
        self.order = order
        self.desc = desc

    def __repr__(self):
        col = f"{self.newname}({self.coltype})"
        col = col + f':{self.desc}' if self.desc else col
        return col

    def __eq__(self, other):
        if self.newname == other.newname and self.coltype == other.coltype and self.sqlexpr == other.sqlexpr:
            return True
        return False

    def __contains__(self, newname):
        return newname == self.newname

    def __check_func(self, func):
        if func and func not in ['min', 'max', 'sum', 'count']:
            raise ValueError(f"func:{func} not supported!")
        return func

    @property
    def final_sqlexpr(self):
        if self.func:
            final_sqlexpr = f"{self.func}({self.sqlexpr}) as {self.newname}"
        elif self.newname != self.sqlexpr:
            final_sqlexpr = f"{self.sqlexpr} as {self.newname}"
        else:
            final_sqlexpr = self.newname

        return final_sqlexpr

    @property
    def create_sqlexpr(self):
        create_sqlexpr = f"{self.newname} {self.coltype}"
        return create_sqlexpr


class ColumnsModel(object):

    def __init__(self, *columns):
        self.columns = list(columns)

    def __repr__(self):
        return f"ColumnsModel({self.columns})"

    def __getitem__(self, index: int):
        if isinstance(index, slice):
            start, stop, step = index.indices(len(self.columns))
            return ColumnsModel(*[self.columns[i] for i in range(start, stop, step)])
        elif -len(self.columns) <= index < len(self.columns):
            return ColumnsModel(self.columns[index])
        else:
            raise IndexError("Index out of range")

    def __setitem__(self, index: int, column: ColumnModel):
        if -len(self.columns) <= index < len(self.columns):
            self.columns[index] = column
        else:
            raise IndexError("Index out of range")

    def append(self, item: Any) -> None:
        self.columns.append(item)

    def extend(self, iterable: Iterable[Any]) -> None:
        self.columns.extend(iterable)

    def __len__(self) -> int:
        return len(self.columns)

    def __iter__(self):
        for column in self.columns:
            yield column

    def __contains__(self, name):
        col = self.get_column_by_name(name)
        isin = True if col else False
        return isin

    @property
    def func_cols(self):
        func_cols = [col for col in self.columns if col.func]
        return func_cols

    @property
    def nonfunc_cols(self):
        nonfunc_cols = [col for col in self.columns if not col.func]
        return nonfunc_cols

    @property
    def all_cols(self):
        all_cols = self.nonfunc_cols + self.func_cols
        all_cols = [col.newname for col in all_cols]
        return all_cols

    @property
    def new_cols(self):
        new_cols = ', '.join(self.all_cols)
        return new_cols

    @property
    def create_cols(self):
        create_cols = [col.create_sqlexpr for col in self.columns]
        create_cols = '(' + ',\n'.join(create_cols) + ')'
        return create_cols

    @property
    def select_cols(self):
        all_cols = self.nonfunc_cols + self.func_cols
        all_cols = [col.final_sqlexpr for col in all_cols]
        select_cols = ',\n'.join(all_cols)
        return select_cols

    @property
    def group_cols(self):
        if self.func_cols:
            group_cols = [col.newname for col in self.nonfunc_cols]
            group_cols = ', '.join(group_cols)
            return group_cols

    @property
    def order_cols(self):
        all_cols = self.nonfunc_cols + self.func_cols
        order_cols = [(idx+1, col) for idx, col in enumerate(all_cols) if col.order > 0]
        order_cols_sorted = sorted(order_cols, key=lambda x: [x[1].order, x[1].newname])
        order_cols = [f"{col[0]}" for col in order_cols_sorted]
        order_cols = ', '.join(order_cols)
        return order_cols

    def get_column_by_name(self, name):
        for col in self.columns:
            if col.newname == name:
                return col

    def remove(self, remove_column: str):
        new_columns = []
        columns = self.columns
        for column in columns:
            if column.newname != remove_column:
                new_columns.append(column)
        self.columns = new_columns

    def alter(self, column: str, newcol: ColumnModel):
        columns = self.columns
        new_columns = [newcol if col.newname == column else col for col in columns]
        return ColumnsModel(*new_columns)

    def to_list(self):
        return self.columns
