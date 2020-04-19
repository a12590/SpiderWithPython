#!/usr/local/miniconda2/bin/python
# _*_ coding: utf-8 _*_
from __future__ import absolute_import, division, print_function
import os
import sys

module_path = os.path.abspath(os.path.join('..'))
sys.path.append(module_path)

class DB_Configure(object):
    """
    database config
    """
    # mysql_host = '10.108.211.136'
    mysql_host = 'localhost'
    mysql_user = 'root'
    # mysql_password = ''
    mysql_password = ''
    mysql_database = ''
