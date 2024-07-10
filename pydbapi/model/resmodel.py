# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2024-07-09 14:05:19
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-07-10 11:45:59
# @github: https://github.com/longfengpili


import re
import csv
from pathlib import Path

import pandas as pd


class ResModel:

    def __init__(self, cols: list, values: list):
        self.cols = cols
        self.values = values

    def __repr__(self):
        if not self.cols or not self.values: 
            return 'null'

        cols = ','.join(self.cols.all_cols[:3])
        counts = len(self.values) if self.values else 0
        return f"[{counts} rows]{cols}..."

    def __len__(self):
        if self.values:
            return len(self.values)
        return 0

    def format_value(self, value: list):
        dtype = [col.coltype for col in self.cols]
        formatted_value = []
        for d, v in zip(dtype, value):
            if v is None:
                formatted_value.append('Null')
            elif d.startswith('varchar') or d.startswith('str'):
                if isinstance(v, str):
                    v = v.replace("'", "''")
                formatted_value.append(f"'{v}'")
            elif d in ('date', 'datetime', 'timestamp') and isinstance(v, str) and re.match(r'\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?', v):  # noqa: E501
                formatted_value.append(f"'{v}'")
            else:
                formatted_value.append(f'{v}')
        return '(' + ','.join(formatted_value) + ')'

    def to_dataframe(self):
        columns = self.cols.all_cols
        data = pd.DataFrame(self.values, columns=columns)
        return data

    def to_csv(self, fpath: str):
        with Path(fpath).open(mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(self.cols)
            writer.writerows(self.values)

    def to_insert_values(self):
        insert_values = [self.format_value(value) for value in self.values]
        return ',\n'.join(insert_values)
