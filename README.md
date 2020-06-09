# dbapi
## 支持的数据库类型
+ sqlite
```python
from dbapi import SqliteDB
db = SqliteDB(database=None) #或者传入路径
sql = 'select * from [table];'
row, action, result = db.execute(sql)
```
+ Amazon Redshift
```python
from dbapi import RedshiftDB
db = RedshiftDB(host, user, password, database)
sql = 'select * from [schema].[table];'
row, action, result = db.execute(sql)
```

## 支持的操作
+ execute[【db/base.py】](https://github.com/longfengpili/dbapi/blob/master/dbapi/db/base.py)
    + 代码  
        `db.execute(sql, count=None, progress=None)`
    + params
        * `count`: 返回结果的数量;
        * `progress`： 是否打印执行进度。文件名以'<font color=red>test</font>'开始或者结尾，或者在desc中增加'<font color=red>show progress</font>';
+ select
    + 代码  
        `db.select(tablename, columns, condition=None)`
    + params
        * `tablename`: 表名;
        * `columns`： 列内容; Example: `{'id_rename': {'source_name': 'id', 'source_col':'datas',  'source_type': '', 'func': 'min', 'order': 1}, ……}`
            - source_name: 解析的KEY或者原始数据的列名
            - source_col: 原始数据列名 用于解析
            - source_type: 原始数据类型 用于解析
            - func: 后续处理的函数
            - order: 用于排序
        * `condition`: sql where 中的条件
+ create
    + 代码  
        `db.create(tablename, columns, indexes=None)`
    + params
        * `tablename`: 表名;
        * `columns`： 列内容; Example: `{'id': 'integer', 'name': 'varchar', 'address': 'varchar(1024)'}`
        * `indexes`: 索引，sqlite暂不支持索引
+ insert[【db/base.py】](https://github.com/longfengpili/dbapi/blob/master/dbapi/db/base.py)
    + 代码  
        `db.insert(tablename, columns, values)`
    + params
        * `tablename`: 表名;
        * `columns`： 列内容; Example: `{'id': 'integer', 'name': 'varchar', 'address': 'varchar(1024)'}`
        * `values`: 插入的数值; 
+ drop[【db/base.py】](https://github.com/longfengpili/dbapi/blob/master/dbapi/db/base.py)
    + 代码  
        `db.drop(tablename)`
    + params
        * `tablename`: 表名;
+ delete[【db/base.py】](https://github.com/longfengpili/dbapi/blob/master/dbapi/db/base.py)
    + 代码  
        `db.delete(tablename, condition)`
    + params
        * `tablename`: 表名;
        * `condition`: 插入的数值; 
+ get_columns
    + 代码  
        `db.get_columns(tablename)`
    + params
        * `tablename`: 表名;
+ add_columns
    + 代码  
        `db.add_columns(tablename, columns)`
    + params
        * `tablename`: 表名;
        * `columns`： 列内容; Example: `{'id': 'integer', 'name': 'varchar', 'address': 'varchar(1024)'}`
+ get_filesqls[【db/fileexec.py】](https://github.com/longfengpili/dbapi/blob/master/dbapi/db/fileexec.py)
    + 代码  
        `db.get_filesqls(filepath, **kw)`
    + params
        * `filepath`: sql文件路径;
        * `kw`： sql文件中需要替换的参数
+ exec_file[【db/fileexec.py】](https://github.com/longfengpili/dbapi/blob/master/dbapi/db/fileexec.py)
    + 代码  
        `db.exec_file(filepath, **kw)`
    + params
        * `filepath`: sql文件路径;
        * `kw`： sql文件中需要替换的参数
