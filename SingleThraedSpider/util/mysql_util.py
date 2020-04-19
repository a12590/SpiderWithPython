#!/usr/local/miniconda2/bin/python
# _*_ coding: utf-8 _*_

"""
@author: lhh
@time  : 2018-05-06 下午16:18
"""
import os
import sys

module_path = os.path.abspath(os.path.join('..'))
sys.path.append(module_path)

import MySQLdb
import pandas as pd
from app.conf.db_config import DB_Configure


class MysqlUtil(object):
    def __init__(self, host, user, password, database):
        self.connection = MySQLdb.connect(host=host,
                                          user=user,
                                          passwd=password,
                                          db=database,
                                          charset="utf8")

    def query_from_table_to_dataframe(self, table, selected_column, conditions=None):
        """
        按条件查询某张表的数据
        如: query_from_table('baby_situation', ["ID<10", "BABY_SEX=1"])

        :type selected_column: string, '*', 'ID', ...
        :type table: string
        :type conditions: list
        """
        query_conditions = ['1=1']
        if conditions:
            query_conditions.extend(conditions)

        query_conditions = ' and '.join(query_conditions)
        sql = 'SELECT {} FROM {} WHERE {}'.format(selected_column, table, query_conditions)

        # 获取数据库表字段名称
        columns = self.query_table_columns(table)
        print '==>', sql
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            results = cursor.fetchall()
            result_df = pd.DataFrame(data=list(results), index=None, columns=columns)
            return result_df
        except Exception as e:
            print e.message
            return None

    def query_table_columns(self, table):
        """
        获取数据库表字段名, 用于转换为 dataframe
        :param table:
        :return:
        """
        sql = "select COLUMN_NAME from information_schema.COLUMNS where table_name = '{}'".format(table)
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            results = cursor.fetchall()
            results = [column[0] for column in results]
            return results
        except Exception as e:
            print e.message
            return None

    def add_data_to_table(self, dbName, data_dict):
        try:

            data_values = "(" + "%s," * (len(data_dict)) + ")"
            data_values = data_values.replace(',)', ')')

            dbField = data_dict.keys()
            dataTuple = tuple(data_dict.values())
            dbField = str(tuple(dbField)).replace("'", '')
            cursor = self.connection.cursor()
            sql = """ insert into %s %s values %s """ % (dbName, dbField, data_values)
            params = dataTuple
            cursor.execute(sql, params)
            self.connection.commit()
            cursor.close()

            print "=====  插入成功  ====="
            return 1

        except Exception as e:
            print "********  插入失败    ********"
            print e
            return 0


db_util = MysqlUtil(DB_Configure.mysql_host,
                    DB_Configure.mysql_user,
                    DB_Configure.mysql_password,
                    DB_Configure.mysql_database)
