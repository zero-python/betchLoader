# -*- coding: utf-8 -*-
# @Date: 2019-04-25
# @Author:zero

import decimal
from collections import OrderedDict
import pymysql
import sqlalchemy
from DBUtils.PooledDB import PooledDB
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import scoped_session, sessionmaker
from contextlib import contextmanager
import pandas as pd

base_conf = dict(
                creator=pymysql,  # 使用链接数据库的模块
                maxusage=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
                maxconnections=None,  # 连接池允许的最大连接数，0和None表示不限制连接数
                blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
                setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
                mincached=5,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
                maxcached=20,  # 链接池中最多闲置的链接，0和None不限制
                maxshared=0,
                # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
                ping=1,  # ping MySQL服务端，检查是否服务可用。
                local_infile=1,  # 服务器变量指示能否使用load data local infile命令
               )  # DBUtils 配置专用

class MysqlPool:
    """
    数据库连接池的单例模式
    """
    __intance = {}  # 单例使用
    __pool = {}  # 区分不同的连接池

    def __new__(cls, *args, **kwargs):
        if str(kwargs) not in cls.__intance:
            cls.__intance[str(kwargs)] = super(MysqlPool, cls).__new__(cls)
        return cls.__intance[str(kwargs)]

    def __init__(self, *args, **kwargs):
        if str(kwargs) not in self.__pool:
            MysqlPool.init_pool(kwargs)
        self.pool = self.__pool[str(kwargs)]

    @staticmethod
    def init_pool(kwargs):
        conf = dict(
                    host=kwargs.get("host"),
                    port=kwargs.get("port"),
                    user=kwargs.get("user"),
                    passwd=kwargs.get("password"),
                    charset=kwargs.get("charset"),
                    db=kwargs.get("db")
                   )  # 获取到数据库信息
        conf.update(base_conf)
        MysqlPool.__pool[str(kwargs)] = PooledDB(**conf)


class ORMBase:
    """
    数据库orm的单例模式
    """
    __intance = {}
    __engine_obj = {}

    def __new__(cls, *args, **kwargs):
        if str(kwargs) not in cls.__intance:
            cls.__intance[str(kwargs)] = super(ORMBase, cls).__new__(cls)
        return cls.__intance[str(kwargs)]

    def __init__(self, *args, **kwargs):

        if str(kwargs) not in self.__engine_obj:
            ORMBase.init_engine(kwargs)
        self.engine = self.__engine_obj[str(kwargs)]
        self.session = scoped_session(sessionmaker(bind=self.engine, autoflush=False, autocommit=False))

    @staticmethod
    def init_engine(kwargs):
        MYSQL_PATH = 'mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)s/%(db)s?charset=%(charset)s&local_infile=1'%kwargs
        engine = create_engine(MYSQL_PATH, pool_recycle=10, pool_size=30, max_overflow=0, pool_timeout=60)
        ORMBase.__engine_obj[str(kwargs)] = engine

class MysqlManager:
    """
    mysql数据库封装，使用pooledDB库实现单例数据库连接池，以及SQLALCHAMY的orm实例。
    ##如果想直接通过sql获取到结果，使用read_sql方法，参数to_pandas默认为False，
    ##返回list结果，True代表返回pandas结果。
    >>> sql = "SELECT * FROM `macd_daily_bfq` limit 1;"
    >>> result_list = MysqlManager('quant').read_sql(sql)
    >>> print(isinstance(result_list, list))
    True
    >>> result_pd = MysqlManager('quant').read_sql(sql, to_DataFrame=True)##to_DataFrame
    >>> print(isinstance(result_pd, pd.DataFrame))
    True
    >>> with MysqlManager('quant') as session:
    ...   result = session.fetchall(sql)
    >>> print(isinstance(result_list, list))
    True
    >>> with MysqlManager('quant').Session as session:
    ...    print(isinstance(session, sqlalchemy.orm.session.Session))
    True
    """
    __intance = {}

    def __init__(self, *args, **kwargs):
        assert kwargs.get('host')
        assert kwargs.get('port')
        assert kwargs.get('user')
        assert kwargs.get('password')
        assert kwargs.get('charset')
        assert kwargs.get('db')
        self.__pool = MysqlPool(*args, **kwargs).pool  # 单例数据库连接池
        self.__session = ORMBase(*args, **kwargs).session

    @property
    def __init_conn(self):
        self.__conn = self.__pool.connection()  # 获取连接
        self.__cursor = self.__conn.cursor(pymysql.cursors.DictCursor)

    @property
    @contextmanager
    def Session(self):
        session = self.__session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def execute_many(self, sql, params=None):
        sql_list = sql.strip().split(';')
        sql_list.remove('')
        try:
            self.__init_conn
            for sql in sql_list:
                self.__cursor.execute(sql, params)
            self.__conn.commit()
            return True
        except Exception as e:
            self.__conn.rollback()
            raise e

    def execute(self, sqls, params=None):
        """
        等同mysql中的execute，执行sql语句.
        :param sqls:
        :param params:u
        :return:
        """
        try:
            self.__init_conn
            self.__cursor.execute(sqls, params)
            self.__conn.commit()
            return True
        except Exception as e:
            self.__conn.rollback()
            raise e

    def read_sql(self, sql, to_DataFrame=False):
        """
        执行查询sql，返回结果。
        :param sql:
        :param to_DataFrame:是否返回panda结构数据。
        :return:
        """
        return self.__read_main(sql=sql, to_DataFrame=to_DataFrame)

    def read_safe_sql(self, sql, params, to_DataFrame=False):
        """
        安全执行查询sql，添加params，防止sql注入
        :param sql:
        :param params:
        :param to_DataFrame:
        :return:
        """
        return self.__read_main(sql, params, to_DataFrame)

    def __read_main(self, sql, params=None, to_DataFrame=False):
        """
        执行sql查询
        :param sql:
        :param params:
        :param to_DataFrame:
        :return:
        """
        try:
            result = self.fetchall(sql, params, to_DataFrame)
            return result
        except Exception as e:
            print(e)
            raise e

    @staticmethod
    def __change_type(result):

        for info_dict in result:
            for k, v in info_dict.items():
                if isinstance(v, decimal.Decimal):
                    info_dict[k] = float(v)
        return result

    def fetchall(self, sql, params=None, to_DataFrame=False):
        """
        获取sql查询出的所有数据，默认转换为列表字典格式
        :param sql:
        :param params:
        :return:
        """
        try:
            self.execute(sql, params)
            result = self.__cursor.fetchall()

            if result:
                result = self.__change_type(result)  ##替换decimal类型数据
            if to_DataFrame:
                # Create DataFrame Preserving Order of the columns:  noqa
                result_fix = list(map(lambda x: OrderedDict(x), result))
                result = pd.DataFrame(list(result_fix))
            return result
        except Exception as e:
            print('sql error %s' % str(e))
            raise e
        finally:
            self.close()

    def insert_many(self, sql, values=[]):
        """
        批量插入，args为数据列表。
        :param sql: insert into tablename (id,name) values (%s,%s)
        :param values:[(1,'test'),(2, 'new')]
        :return:
        """
        try:
            self.__init_conn
            self.__cursor.executemany(sql, values)
            self.__conn.commit()
        except Exception as e:
            self.__conn.rollback()
            raise e
        finally:
            self.close()

    def __enter__(self):
        """
        上下文管理器中进入，则返回该对象
        :return:
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.__cursor.close()
        self.__conn.close()


if __name__ == '__main__':
    manager_obj = MysqlManager(user='root', port=3306, host='localhost', charset='utf8',db='test',password='x')
    result = manager_obj.read_sql('select * from history_20180101 limit 10', to_DataFrame=True)
    print(result)