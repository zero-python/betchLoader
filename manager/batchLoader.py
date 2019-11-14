# coding=utf-8
from __future__ import print_function
import os
import sys
from retry import retry

from manager.mysqlManager import MysqlManager

try:
    import cPickle as pickle
except Exception as e:
    import pickle
import time
import shutil
import platform


class batchLoader(object):

    def __init__(self, session):
        self.session = session
        self.parent_dir = os.path.splitext(os.path.basename(sys.argv[0]))[0]  #
        self.dir = os.path.join(os.path.dirname(__file__), '__temp__' + self.parent_dir)
        self.__execute_list = []
        self.__str_col_path = os.path.join(self.dir, 'str_columns')  # df的列名，用来作为load file的数据库列名
        self.__terminated = '\r\n' if platform.system() == 'Windows' else '\n'

    def __enter__(self):
        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)
        os.mkdir(self.dir)

    @retry(tries=3, delay=0.5)
    def __try_load_command(self, _fname, fname, str_columns):
        try:
            # -- chyi modified: #noqa
            modified_str_pre = str_columns.replace('(', '').replace(')', '').split(',')
            modified_str = str(list(map(lambda x: '@v{}'.format(str(x).strip()), modified_str_pre))).replace('[', '(').replace(']', ')').replace("'", '')
            isnull_str = str(list(map(lambda x: "{0} = nullif(@v{0}, '')".format(str(x).strip()), modified_str_pre))).replace('[', '').replace(']', '').replace('"', '')
            load_sql = """LOAD DATA LOCAL INFILE '{}' REPLACE INTO TABLE {} 
            FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' escaped by '' 
            LINES TERMINATED BY '{}' {} SET {};""".format(_fname,
                                                   self.__format_2(fname),
                                                   self.__terminated,
                                                   modified_str,
                                                   isnull_str)

            self.session.execute(load_sql)
        except Exception as e:
            print(e)
            raise e

    def __execute(self):
        start = time.time()
        with open(self.__str_col_path, 'rb') as inputs:
            self.str_columns = pickle.load(inputs)
        for dir_path, dir_names, file_names in os.walk(self.dir):
            for file_name in file_names:
                if file_name != 'str_columns':
                    self.__execute_list.append(os.path.join(dir_path, file_name))
        for fname in self.__execute_list:
            _fname = self.__format_1(fname)
            _start = time.time()
            self.__try_load_command(_fname, fname, self.str_columns)
            _end = time.time()
            os.remove(fname)
            print('{} loading costs {} seconds'.format(_fname, _end - _start))

        end = time.time()
        print('Batch of loading data costs {} seconds'.format(end - start))
        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)
        if os.path.exists(self.__str_col_path):
            os.remove(self.__str_col_path)
        print('Finish load data Successfully.')

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__execute()

    def __format_1(self, fname):
        return fname.replace('\\', '/')

    def __format_2(self, fname):
        return os.path.splitext(os.path.basename(fname))[0]

    def execute(self):
        self.__execute()



