# 批量落地daframe数据工具
### 在工作的工程中经常遇到pandas Daframe格式的数据需要落地，且数据量很大，虽然pandas自带了pandas.to_sql()方法
### 但是效率很低，如果迭代过多，大量时间花在连接数据库中。
### 因此这里记录下自己写的工具，为有需要的码友提供方便。

## 环境要求：
#### pyhon3
#### pandas
#### retry
#### pymysql
#### sqlalchemy
#### DBUtils

## 主要分为2大模块：
### 1，mysqlManager 
数据库管理器，封装了连接池，以及sql常用操作方法

```python
from manager.mysqlManager import MysqlManager
session = MysqlManager(user='root', port=3306, host='localhost', charset='utf8', db='test', password='x')
sql = 'select * from table_name;'
result_list = session.read_sql(sql=sql)  # 返回查询结果，list结构
result_pd = session.read_sql(sql=sql, to_DataFrame=True)  # 返回查询结果，pandas结构

```
### 2 batchDecorator batchLoader
主要思想：利用mysql的load file的效率是最快的，将pandas数据写入文件，再load进数据库，减少数据处理及io读写。

#### 使用注意事项：
#### 1，pandas结构的列名需要对应导入数据表的列名。
#### 2，在多线程中慎用。

### batchLoader
负责多进程/单进程创建，删除文件及load file导入数据落地

### batchUpLoad
负责将dataframe数据迭代写入文件

### 两种测试案例：

1, 庞大的单个dataframe使用。
```python
from manager.batchDecorator import batchUpLoad
from manager.batchLoader import batchLoader
from manager.mysqlManager import MysqlManager
session = MysqlManager(user='root', port=3306, host='localhost', charset='utf8', db='test', password='x')
sql = 'select * from table_name;'
result_pd = session.read_sql(sql=sql, to_DataFrame=True)
with batchLoader(session): # 传入session对象
    batchUpLoad(result_pd, 'table_name')  # result_pd数据源，table_name对应的表名
```

2，多个dataframe迭代写入文件后，组成最终的导入数据。
```python
from manager.batchDecorator import batchUpLoad
from manager.batchLoader import batchLoader
from manager.mysqlManager import MysqlManager
session = MysqlManager(user='root', port=3306, host='localhost', charset='utf8', db='test', password='x')
sql = 'select * from table_name;'
result_pd = session.read_sql(sql=sql, to_DataFrame=True)
with batchLoader(session): # 传入session对象，创建
    for i in range(10):
        batchUpLoad(result_pd, 'table_name')  # result_pd数据源，table_name对应的表名
```
