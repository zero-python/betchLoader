# -*- coding: utf-8 -*-
"""
Author: zero
Email: 13256937698@163.com
Date: 2019-11-08
"""
from manager.mysqlManager import MysqlManager
from manager.batchDecorator import batchUpLoad
from manager.batchLoader import batchLoader
# session = MysqlManager(user='root', port=3306, host='localhost', charset='utf8', db='test', password='x')
#
# rsult_pd = session.read_sql('select * from order_min_flow_ext limit 1000;', to_DataFrame=True)
#
# with batchLoader(session):
#     batchUpLoad(rsult_pd, 'order_min_flow_ext')

import json
import re
ESCAPE = re.compile(r'[\x00-\x1f\\"\b\f\n\r\t]')
ESCAPE_ASCII = re.compile(r'([\\"]|[^\ -~])')
HAS_UTF8 = re.compile(b'[\x80-\xff]')
ESCAPE_DCT = {
    '\\': '\\\\',
    '"': '\\"',
    '\b': '\\b',
    '\f': '\\f',
    '\n': '\\n',
    '\r': '\\r',
    '\t': '\\t',
}

def py_encode_basestring(s):
    """Return a JSON representation of a Python string

    """
    def replace(match):
        return ESCAPE_DCT[match.group(0)]
    return '"' + ESCAPE.sub(replace, s) + '"'

def c_encode_basestring(string):
    return ""
encode_basestring = (c_encode_basestring or py_encode_basestring)
t_dict = {'a': 1, 'b': 2}

json.dumps("test", ensure_ascii=False)



