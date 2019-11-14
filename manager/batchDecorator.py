# coding=utf-8
import pandas as pd
import os
try:
    import cPickle as pickle
except Exception as e:
    import pickle
import sys

def batchDecorator():
    def _deco(func):
        def __deco(*args, **kwargs):
            f = __batchDecorator(func)
            f(*args, **kwargs)
        return __deco
    return _deco


@batchDecorator()
def batchUpLoad(df, table_name):
    setattr(df, 'table_name', table_name)
    return df


class __batchDecorator(object):
    def __init__(self, func):
        self.func = func
        self.parent_dir = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        self.dir = os.path.join(os.path.dirname(__file__), '__temp__'+self.parent_dir)
        self.child_dir = os.path.join(self.dir, str(os.getpid()))
        self.subffix = '.csv'

    def __call__(self, *args, **kwargs):
        obj = self.func(*args, **kwargs)
        self.__make_tmp_tables(obj)

    def __make_tmp_dir(self):
        if not os.path.exists(self.dir):
            os.mkdir(self.dir)
        if not os.path.exists(self.child_dir):
            os.mkdir(self.child_dir)

    def __make_tmp_table_path(self, name):
        self.__make_tmp_dir()
        tmp_table_path = os.path.join(self.child_dir, name + self.subffix)
        return tmp_table_path

    def __create_tmp_table(self, obj):
        if hasattr(obj, 'table_name'):
            name = getattr(obj, 'table_name')
            tmp_table_name = self.__make_tmp_table_path(name)
            obj.to_csv(tmp_table_name, header=False, index=False, mode='a+', encoding='utf-8')
        else:
            raise Exception('The returned value should have an attribute value as table_name.')

    def __make_tmp_tables(self, obj):
        if isinstance(obj, pd.DataFrame):
           pass
        elif isinstance(obj, pd.Series):
            _name = getattr(obj, 'table_name')
            obj = obj.to_frame().T
            setattr(obj, 'table_name', _name)
        else:
            raise Exception('The returned value should be dataFrame or series.')
        self.__create_tmp_table(obj)
        self.__save_columns(obj)

    def __save_columns(self, obj):
        __path = os.path.join(self.dir, 'str_columns')
        if not os.path.exists(__path):
            str_col = obj.columns.tolist()    # -- Read Dataframe
            str_columns = str(str_col).replace('[', '(').replace(']', ')').replace("'", '')
            with open(__path, 'wb') as out:
                pickle.dump(str_columns, out)



