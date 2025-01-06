# pydbapi
一个简单的数据库 API，支持多种数据库类型，包括 SQLite、Amazon Redshift、MySQL 和 Trino。

## 安装
```python
pip install pydbapi
```

## 支持的数据库类型
### SQLite
```python
from pydbapi.api import SqliteDB
db = SqliteDB(database=None)  # 或者提供路径
sql = 'select * from [table];'
cursor, action, result = db.execute(sql)
```
### Amazon Redshift
```python
from pydbapi.api import RedshiftDB
db = RedshiftDB(host, user, password, database, port='5439', safe_rule=True)
sql = 'select * from [schema].[table];'
cursor, action, result = db.execute(sql)
```
### MySQL
```python
from pydbapi.api import MysqlDB
db = MysqlDB(host, user, password, database, port=3306, safe_rule=True, isdoris=False)
sql = 'select * from [table];'
cursor, action, result = db.execute(sql)
```
### Trino
```python
from pydbapi.api import TrinoDB
db = TrinoDB(host, user, password, database, catalog, port=8443, safe_rule=True)
sql = 'select * from [table];'
cursor, action, result = db.execute(sql)
```

## 实例模式
```python
from pydbapi.api import SqliteDB
db = SqliteDB.get_instance(database=None)  # 或者提供路径
sql = 'select * from [table];'
cursor, action, result = db.execute(sql)
```

## 结果
### 转换为 DataFrame
```python
from pydbapi.api import TrinoDB
db = TrinoDB(host, user, password, database, catalog, port=8443, safe_rule=True)
sql = 'select * from [table];'
cursor, action, result = db.execute(sql)

df = result.to_dataframe()
df
```
### 输出为 CSV
```python
from pydbapi.api import TrinoDB
db = TrinoDB(host, user, password, database, catalog, port=8443, safe_rule=True)
sql = 'select * from [table];'
cursor, action, result = db.execute(sql)

result.to_csv(outfile)
```

## 列
`from pydbapi.model import ColumnModel`
### ColumnModel
    + 代码
        `col = ColumnModel(newname, coltype='varchar', sqlexpr=None, func=None, order=0)`
    + 参数
        * `newname`: 新名称;
        * `coltype`: 类型
        * `sqlexpr`: SQL 表达式
        * `func`: 查询函数，当前支持 'min'、'max'、'sum'、'count'
        * `order`: 排序

### ColumnsModel
    + 代码
        `cols = ColumnsModel(ColumnModel, ColumnModel, ……)`
    + 属性
        * `func_cols`: 返回列的列表
        * `nonfunc_cols`: 返回列的列表
        * `new_cols`: 返回拼接字符串
        * `create_cols`: 返回拼接字符串
        * `select_cols`: 返回拼接字符串
        * `group_cols`: 返回拼接字符串
        * `order_cols`: 返回拼接字符串
    + 方法
        * get_column_by_name
            - `cols.get_column_by_name(name)`
            - 返回 `ColumnModel`

## SQL
+ SqlStatement
```python
from pydbapi.sql import SqlStatement
sql = 'select * from tablename where part_date >= $part_date;'
sqlstmt = SqlStatement(sql)
```

+ **属性**  

    - tokens  
    - sql
    - comment
    - action
    - tablename
    - params
    - subqueries

+ **方法**  

    + from_sqlsnippets
    ```python
    sstmt1 = '-- comment'
    sqlstmt = SqlStatement.from_sqlsnippets(sstmt1, sql)
    ```
    + add
    ```python
    sstmt2 = 'and part_date <= $end_date'
    sqlstmt += sstmt2
    ```
    + sub
    ```python
    sqlstmt -= sstmt2
    ```
    + substitute_params
    ```python
    sqlstmt = sqlstmt.substitute_params(part_date="'2024-01-01'")
    ```
    + get_with_testsql(仅支持 CETs)
    ```python
    sqlstmt = sqlstmt.get_with_testsql(idx=1)
    ```

+ SqlStatements
```python
from pydbapi.sql import SqlStatements
sql = '''
    select * from tablename1 where part_date >= $part_date;
    select * from tablename2 where part_date >= $part_date;
'''
sqlstmts = SqlStatements(sql)
```

+ **属性**  

    - statements  
    - SqlStatement 的属性

+ **方法**  

    + SqlStatement
    ```python
    sqlstmts = sqlstmts.substitute_params(part_date='2024-01-01')
    ```
    + iter
    ```python
    for stmts in sqlstmts:
        stmts
    ```
    - len
    ```python
    len(sqlstmts)
    ```
    - getitem
    ```python
    sqlstmts[0]
    sqlstmts[:2]
    ```

## 支持的操作
+ execute[【db/base.py】](https://github.com/longfengpili/pydbapi/blob/master/pydbapi/db/base.py)
    + 代码  
        `db.execute(sql, count=None, ehandling=None, verbose=0)`
    + 参数
        * `count`: 返回结果的数量;
        * `ehandling`: SQL 执行出错时的处理方式，默认: None
        * `verbose`: 执行的进度展示方式（0：不打印， 1：文字进度， 2：进度条）
+ select
    + 代码  
        `db.select(tablename, columns, condition=None, verbose=0)`
    + 参数
        * `tablename`: 表名;
        * `columns`： 列内容; 
        * `condition`: SQL where 中的条件
+ create
    + sqlite/redshift
        + 代码  
        `db.create(tablename, columns, indexes=None, verbose=0)`
        + 参数
            - `tablename`: 表名;
            - `columns`： 列内容;
            - `indexes`: 索引，sqlite 暂不支持索引
            - `verbose`： 是否打印执行进度。
    + mysql
        + 代码  
        `db.create(tablename, columns, indexes=None, index_part=128, ismultiple_index=True, partition=None, verbose=0)`
        + 参数
            - `tablename`: 表名;
            - `columns`： 列内容;
            - `indexes`: 索引
            - `index_part`: 索引部分
            - `ismultiple_index`: 多重索引
            - `partition`: 分区
            - `verbose`： 是否打印执行进度。
    + trino
        + 代码  
        `db.create(tablename, columns, partition=None, verbose=0)`
        + 参数
            - `tablename`: 表名;
            - `columns`： 列内容;
            - `partition`: 分区
            - `verbose`： 是否打印执行进度。
+ insert[【db/base.py】](https://github.com/longfengpili/pydbapi/blob/master/pydbapi/db/base.py)
    + 代码  
        `db.insert(tablename, columns, inserttype='value', values=None, chunksize=1000, fromtable=None, condition=None)`
    + 参数
        * `tablename`: 表名;
        * `columns`： 列内容;
        * `inserttype`: 插入数据类型，支持 value、select
        * `values`: inserttype='value'，插入的数值; 
        * `chunksize`: inserttype='value'，每个批次插入的量级; 
        * `fromtable`: inserttype='select'，数据来源表;
        * `condition`:  inserttype='select'，数据来源条件;
+ drop[【db/base.py】](https://github.com/longfengpili/pydbapi/blob/master/pydbapi/db/base.py)
    + 代码  
        `db.drop(tablename)`
    + 参数
        * `tablename`: 表名;
+ delete[【db/base.py】](https://github.com/longfengpili/pydbapi/blob/master/pydbapi/db/base.py)
    + 代码  
        `db.delete(tablename, condition)`
    + 参数
        * `tablename`: 表名;
        * `condition`: 删除条件; 
+ get_columns
    + 代码  
        `db.get_columns(tablename)`
    + 参数
        * `tablename`: 表名;
+ add_columns
    + 代码  
        `db.add_columns(tablename, columns)`
    + 参数
        * `tablename`: 表名;
        * `columns`： 列内容; 
+ get_filesqls[【db/fileexec.py】](https://github.com/longfengpili/pydbapi/blob/master/pydbapi/db/fileexec.py)
    + 代码  
        `db.get_filesqls(filepath, **kw)`
    + 参数
        * `filepath`: SQL 文件路径;
        * `kw`： SQL 文件中需要替换的参数，会替换 SQL 文件中的 arguments;
+ file_exec[【db/fileexec.py】](https://github.com/longfengpili/pydbapi/blob/master/pydbapi/db/fileexec.py)
    + 代码  
        `db.file_exec(filepath, ehandling=None, verbose=0, **kw)`
    + 参数
        * `filepath`: SQL 文件路径; 文件名以<font color=red>`test`</font>开始或者结尾会打印 SQL 执行的步骤;
        * `ehandling`: SQL 执行出错时的处理方式，默认: None
        * `verbose`: 执行的进度展示方式（0：不打印， 1：文字进度， 2：进度条）
        * `kw`： SQL 文件中需要替换的参数，在 SQL 文件中用`$param`，会替换 SQL 文件中的 arguments;
    + SQL 文件格式(在 desc 中增加<font color=red>`verbose`</font>会打印 SQL 执行的步骤;)
        ```sql
        #【arguments】#
        ts = '2020-06-28'
        date = today
        date_max = date + timedelta(days=10)
        #【arguments】#
        ###
        --【desc1 [verbose]】 # SQL 描述
        --step1
        sql1;
        --step2
        sql2 where name = $name;
        ###
        ###
        --【desc2 [verbose]】 # SQL 描述
        --step1
        sql1;
        --step2
        sql2;
        ###
        ```
    + 参数
        * 支持 Python 表达式（datetime、date、timedelta）
        * 支持全局变量和当前 SQL 文件设置过的变量
        * now：获取执行的时间
        * today: 获取执行的日期

## 魔法命令
+ 注册方法  
命令行中执行`pydbapimagic`

+ 参数
    * 帮助  
    `%dbconfig`
    * 配置  
        ```python
        %dbconfig DBTYPE = 'mysql'
        %dbconfig HOST = 'localhost'
        %dbconfig USER = 'longfengpili'
        %dbconfig PASSWORD = '123456abc'
        %dbconfig DATABASE = 'test'
        %dbconfig PORT = 3306
        ```
    * 查看  
    `%dbconfig DBTYPE`

## 支持的设置[【conf/settings.py】](https://github.com/longfengpili/pydbapi/blob/master/pydbapi/conf/logconf.py)
+ AUTO_RULES  
    可以自动执行表名（表名包含即可）
