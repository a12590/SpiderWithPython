#!/usr/local/miniconda2/bin/python
# _*_ coding: utf-8 _*_

import os
import sys
import json
module_path = os.path.abspath(os.path.join('..'))
print module_path
sys.path.append(module_path)
reload(sys)
sys.setdefaultencoding('utf8')

from app.utils.aes_util import get_key
from app.utils.aes_util import cryptography_encode
from app.utils.mysql_util import MysqlUtil
from app.conf.job_meta_db_config import JOB_DB_Configure
from sqlalchemy import create_engine
#from pyspark import SparkContext, SparkConf, HiveContext
import pandas as pd
import datetime
import MySQLdb

#sc = SparkContext('local', 'to_hive_job2')
#sqlContext = HiveContext(sc)


def obtain_source_conf_by_id(id):
    db_util = MysqlUtil(JOB_DB_Configure.mysql_host,
                        JOB_DB_Configure.mysql_user,
                        JOB_DB_Configure.mysql_password,
                        JOB_DB_Configure.mysql_database)

    table = 'job_source_table_conf'
    selected_column = '*'
    print '====== table {} perform data cleaning ======'.format(table)
    print 'query data from mysql database...'
    condation = ['source_type = 0', 'id = {}'.format(id)]
    source_conf_df = db_util.query_from_table_to_dataframe(table, selected_column,  condation)
    return source_conf_df


def obtain_target_conf_by_id(id):
    db_util = MysqlUtil(JOB_DB_Configure.mysql_host,
                        JOB_DB_Configure.mysql_user,
                        JOB_DB_Configure.mysql_password,
                        JOB_DB_Configure.mysql_database)

    table = 'job_target_table_conf'
    selected_column = '*'
    print '====== table {} perform data cleaning ======'.format(table)
    print 'query data from mysql database...'
    condation = ['source_type = 1', 'id = {}'.format(id)]
    target_conf_df = db_util.query_from_table_to_dataframe(table, selected_column, condation)
    return target_conf_df


def obtain_job_conf_by_id(id):
    db_util = MysqlUtil(JOB_DB_Configure.mysql_host,
                        JOB_DB_Configure.mysql_user,
                        JOB_DB_Configure.mysql_password,
                        JOB_DB_Configure.mysql_database)

    table = 'mission'
    selected_column = '*'
    print '====== table {} perform data cleaning ======'.format(table)
    print 'query data from mysql database...'
    condation = ['id = {}'.format(id)]
    job_conf_df = db_util.query_from_table_to_dataframe(table, selected_column, condation)
    return job_conf_df


def obtain_source_date_by_conf(source_conf_df):
    if source_conf_df['db_type'].values[0] == 1:
        source_date_df = df_read_from_mysql(source_conf_df)
    #elif source_conf_df['db_type'].values[0] == 2:
     #   source_date_df = df_read_from_hive(source_conf_df)
    elif source_conf_df['db_type'].values[0] == 3:
        source_date_df = df_read_from_csv(source_conf_df)
    elif source_conf_df['db_type'].values[0] == 4:
        source_date_df = df_read_from_excel(source_conf_df)
    return source_date_df

def delete_target_date_by_conf(target_conf_df):
    host_name = target_conf_df['url'].values[0]
    user_name = target_conf_df['user_name'].values[0]
    pass_word = target_conf_df['pass_word'].values[0]
    db_name = target_conf_df['db_name'].values[0]
    table = target_conf_df['table_name'].values[0]

    db = MySQLdb.connect(host=host_name, port=3306,
                               user=user_name,
                               passwd=pass_word,
                               db=db_name, charset="utf8")

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # SQL 删除语句
    sql = "DELETE FROM {}".format(table);
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 提交修改
        db.commit()
    except:
        # 发生错误时回滚
        db.rollback()

    # 关闭连接
    db.close()

def obtain_target_date_by_conf(target_conf_df):
    if target_conf_df['db_type'].values[0] == 1:
        target_date_df = df_read_from_mysql(target_conf_df)
    #elif target_conf_df['db_type'].values[0] == 2:
    #    target_date_df = df_read_from_hive(target_conf_df)
    elif target_conf_df['db_type'].values[0] == 3:
        target_date_df = df_read_from_csv(target_conf_df)
    elif target_conf_df['db_type'].values[0] == 4:
        target_date_df = df_read_from_excel(target_conf_df)
    return target_date_df


def save_target_date_by_conf(souce_conf_df):
    db_util = MysqlUtil(JOB_DB_Configure.mysql_host,
                        JOB_DB_Configure.mysql_user,
                        JOB_DB_Configure.mysql_password,
                        JOB_DB_Configure.mysql_database)

    table = 'mission'
    selected_column = '*'
    print '====== table {} perform data cleaning ======'.format(table)
    print 'query data from mysql database...'
    condation = ['id = {}'.format(id)]
    job_conf_df = db_util.query_from_table_to_dataframe(table, selected_column, condation)
    return job_conf_df


def obtain_filter_rule_date_by_job_id(job_id):
    db_util = MysqlUtil(JOB_DB_Configure.mysql_host,
                        JOB_DB_Configure.mysql_user,
                        JOB_DB_Configure.mysql_password,
                        JOB_DB_Configure.mysql_database)

    table = 'filter'
    selected_column = '*'
    print '====== table {} perform data cleaning ======'.format(table)
    print 'query data from mysql database...'
    condation = ['job_id = {}'.format(job_id)]
    filter_rules_df = db_util.query_from_table_to_dataframe(table, selected_column, condation)
    return filter_rules_df


def save_key_and_job_id(key, job_id):
    db_util = MysqlUtil(JOB_DB_Configure.mysql_host,
                        JOB_DB_Configure.mysql_user,
                        JOB_DB_Configure.mysql_password,
                        JOB_DB_Configure.mysql_database)
    table = 'store_enc_keys'
    data_dict = {
        "enc_key":key,
        "job_id":job_id,
    }
    return db_util.add_data_to_table(table, data_dict)

def save_modifyed_columns_and_job_id(columns_str, job_id):
    db_util = MysqlUtil(JOB_DB_Configure.mysql_host,
                        JOB_DB_Configure.mysql_user,
                        JOB_DB_Configure.mysql_password,
                        JOB_DB_Configure.mysql_database)
    table = 'source_dupli_columns'
    data_dict = {
        "job_id":job_id,
        "columns_str":columns_str,
    }
    return db_util.add_data_to_table(table, data_dict)

def obtain_ignore_columns_by_filter_rules_df(filter_rules_df):
    columns_name = filter_rules_df["column_name"].values
    ignore_result = filter_rules_df["is_ignore"].values
    column_result = []
    for (column, ignore) in zip(columns_name,ignore_result):
        if ignore == 1:
            column_result.append(column)
    return column_result

def obtain_enc_columns_by_filter_rules_df(filter_rules_df):
    columns_name = filter_rules_df["column_name"].values
    enc_columns = filter_rules_df["isEnc"].values
    column_result = []
    for (column, enc) in zip(columns_name,enc_columns):
        if enc == 1:
            column_result.append(column)
    return column_result


def obtain_filter_rules_by_filter_rules_df(filter_rules_df):
    columns_name = filter_rules_df["column_name"].values
    filter_columns = filter_rules_df["filter_rules"].values
    column_result = []
    for (column, fil) in zip(columns_name, filter_columns):
        if fil is not None:
            dic_col = {}
            dic_col[column] = fil
            column_result.append(dic_col)
    return column_result


def execute_job(souce_conf_df, souce_date_df, filter_rules_df, target_conf_df, job_id):
    #根据过滤规则元数据得到过滤忽略字段
    if len(filter_rules_df) >0 and filter_rules_df is not None:
        ignore_columns = obtain_ignore_columns_by_filter_rules_df(filter_rules_df)
        # 执行过滤字段功能
        if len(ignore_columns) >0:
            souce_date_df.drop(ignore_columns, axis=1, inplace=True)
            #omit_columns_function(ignore_columns, souce_date_df)
            # 根据过滤规则元数据得到加密字段集
        enc_columns = obtain_enc_columns_by_filter_rules_df(filter_rules_df)
        # 执行加密字段功能
        if len(enc_columns) >0:
            if souce_date_df is not None:
                souce_date_df = enc_colums_function(enc_columns, souce_date_df, job_id)
        # 根据过滤规则元数据得到各字段的过滤条件集
        filter_rules = obtain_filter_rules_by_filter_rules_df(filter_rules_df)
        # 执行数据集过滤
        if len(filter_rules) >0:
            if len(souce_date_df) >0:
                print 'souce_date_df length is {}'.format(len(souce_date_df))
                print 'lenght of filter_rules {}'.format(len(filter_rules))
                print filter_rules['filter_rules'].value[0]
                souce_date_df = filer_columns_function(filter_rules, souce_date_df)
                print 'after souce_date_df length is {}'.format(len(souce_date_df))
    #将过滤完的数据保存起来
    if target_conf_df['db_type'].values[0] == 1:
        #保存为Mysql
        if souce_date_df is not None:
            df_save_to_mysql(souce_date_df, target_conf_df)
    #elif target_conf_df['db_type'].values[0] == 2:
        #保存到hive中
        #df_save_to_hive(souce_date_df, target_conf_df)
    elif target_conf_df['db_type'].values[0] == 3:
        #保存为CSV
        df_save_to_csv(souce_date_df, target_conf_df, job_id)
    elif target_conf_df['db_type'].values[0] == 4:
        #保存为xlsx
        df_save_to_excel(souce_date_df, target_conf_df, job_id)


def omit_columns_function(ignore_columns, data_df):
    data_df.drop(ignore_columns, axis=1, inplace=True)
    return data_df


def enc_colums_function(enc_columns, data_df, job_id):
    key = get_key()
    save_key_and_job_id(key, job_id)
    for col in enc_columns:
        print type(data_df[col])
        print col
        data_df[col] = data_df[col].map(lambda x: cryptography_encode(str(x), key))
    return data_df


def filer_columns_function(filter_rules, data_df):
    for filter_item in filter_rules:
        for (column, filter_rule) in filter_item.items():
            #print json.loads('{"num":[{"large":1000,"lit":1003}]}')
            filter_rule_dict = json.loads(filter_rule)
            for (column_type, rule_items) in filter_rule_dict.items():
                if column_type == 'tim':
                    #for rule_item in rule_items:
                        for (item_key, item_value) in rule_items.items():
                            if data_df is not None:
                                if item_key == 'equ':
                                    data_df = data_df[
                                        data_df[column] == datetime.datetime.strptime(item_value, "%Y-%m-%d").date()]
                                elif item_key == 'n_equ':
                                    data_df = data_df[
                                        data_df[column] != datetime.datetime.strptime(item_value, "%Y-%m-%d").date()]
                                elif item_key == 'lar':
                                    data_df = data_df[
                                        data_df[column] >= datetime.datetime.strptime(item_value, "%Y-%m-%d").date()]
                                elif item_key == 'lit':
                                    data_df = data_df[
                                        data_df[column] <= datetime.datetime.strptime(item_value, "%Y-%m-%d").date()]
                                elif item_key == 'lit_equ':
                                    data_df = data_df[
                                        data_df[column] <= datetime.datetime.strptime(item_value, "%Y-%m-%d").date()]
                                elif item_key == 'lar_equ':
                                    data_df = data_df[
                                        data_df[column] >= datetime.datetime.strptime(item_value, "%Y-%m-%d").date()]

                if column_type == 'num':
                    #for rule_item in rule_items:
                        for (item_key, item_value) in rule_items.items():
                            if data_df is not None:
                                if item_key == 'equ':
                                    data_df = data_df[data_df[column] == item_value]
                                elif item_key == 'n_equ':
                                    data_df = data_df[data_df[column] != item_value]
                                elif item_key == 'lit':
                                    data_df = data_df[data_df[column] < item_value]
                                elif item_key == 'lar':
                                    data_df = data_df[data_df[column] > item_value]
                                elif item_key == 'lit_equ':
                                    data_df = data_df[data_df[column] <= item_value]
                                elif item_key == 'lar_equ':
                                    data_df = data_df[data_df[column] >= item_value]


                if column_type == 'tex':
                    #for rule_item in rule_items:
                        for (item_key, item_value) in rule_items.items():
                            if data_df is not None:
                                if item_key == 'equ':
                                    data_df = data_df[data_df[column] == item_value]
                                elif item_key == 'n_equ':
                                    data_df = data_df[data_df[column] != item_value]
                                elif item_key == 'lik':
                                    data_df = data_df[data_df[column].str.contains(item_value)]

    return data_df

def df_save_to_mysql(data_df, target_conf_df):
    db_name = target_conf_df['db_name'].values[0]
    url =  target_conf_df['url'].values[0]
    user_name =  target_conf_df['user_name'].values[0]
    pass_word =  target_conf_df['pass_word'].values[0]
    table_name = target_conf_df['table_name'].values[0]
    engine = create_engine('mysql+pymysql://'+user_name+':'+pass_word+'@'+url+'/'+db_name+'?charset=utf8')
    data_df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=1000, index=None)


def df_save_to_csv(data_df, target_conf_df, job_id):
    #modified_columns_str_df = get_modified_columns_str_by_job_id(job_id)
    #if modified_columns_str_df.shape[0] > 0:
        #print "after get_modified_columns_str_by_job_id"
        #data_df.rename(columns=json.loads(modified_columns_str_df.values[0]), inplace=True)

    table_name = target_conf_df['table_name'].values[0]
    #address = url + table_name
    url = "/home/jsw-data-Analysis/elt/uncleaned/{}".format(table_name)
    print "url ======================"
    print url
    data_df.to_csv(url, encoding="utf-8")

def get_modified_columns_str_by_job_id(job_id):
    db_util = MysqlUtil(JOB_DB_Configure.mysql_host,
                        JOB_DB_Configure.mysql_user,
                        JOB_DB_Configure.mysql_password,
                        JOB_DB_Configure.mysql_database)

    table = 'source_dupli_columns'
    selected_column = 'columns_str'
    print '====== table {} perform data cleaning ======'.format(table)
    print 'query data from mysql database...'
    condation = ['job_id = {}'.format(job_id)]
    data_df = db_util.query_from_table_to_dataframe(table, selected_column, condation)
    return data_df

def df_save_to_excel(data_df, target_conf_df, job_id):
    modified_columns_str_df = get_modified_columns_str_by_job_id(job_id)
    if modified_columns_str_df.shape[0] > 0:
        print "after get_modified_columns_str_by_job_id"
        data_df.rename(columns=json.loads(modified_columns_str_df.values[0]), inplace=True)
    url = target_conf_df['url'].values[0]
    table_name = target_conf_df['table_name'].values[0]
    address = url + table_name + ".xlsx"
    print '================================'
    print address
    data_df.to_excel(address, encoding="utf-8")


def chenge_job_stat_starting(job_id):
    # 打开数据库连接
    db = MySQLdb.connect(JOB_DB_Configure.mysql_host, JOB_DB_Configure.mysql_user,
                         JOB_DB_Configure.mysql_password, JOB_DB_Configure.mysql_database, charset='utf8')

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # SQL 更新语句
    sql = "UPDATE mission SET stat = 1 WHERE id = {}".format(job_id)
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 提交到数据库执行
        db.commit()
    except:
        # 发生错误时回滚
        db.rollback()

    # 关闭数据库连接
    db.close()


def chenge_job_stat_end(job_id):
    # 打开数据库连接
    db = MySQLdb.connect(JOB_DB_Configure.mysql_host, JOB_DB_Configure.mysql_user,
                         JOB_DB_Configure.mysql_password, JOB_DB_Configure.mysql_database, charset='utf8')

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # SQL 更新语句
    sql = "UPDATE mission SET stat = 2 WHERE id = {}".format(job_id)
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 提交到数据库执行
        db.commit()
    except:
        # 发生错误时回滚
        db.rollback()

    # 关闭数据库连接
    db.close()


def chenge_job_stat_failure(job_id):
    # 打开数据库连接
    db = MySQLdb.connect(JOB_DB_Configure.mysql_host, JOB_DB_Configure.mysql_user,
                         JOB_DB_Configure.mysql_password, JOB_DB_Configure.mysql_database, charset='utf8')

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # SQL 更新语句
    sql = "UPDATE mission SET stat = 3 WHERE id = {}".format(job_id)
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 提交到数据库执行
        db.commit()
    except:
        # 发生错误时回滚
        db.rollback()

    # 关闭数据库连接
    db.close()
#def df_save_to_hive(data_df, target_conf_df):
    #from pyspark import SparkContext, SparkConf, HiveContext
    #sc = SparkContext('local', 'to_hive_job2')
    #sqlContext = HiveContext(sc)
    #spark_df = sqlContext.createDataFrame(data_df)
    #db_name = target_conf_df['db_name'].values[0]

    #table_name = target_conf_df['table_name'].values[0]

    #spark_df.registerTempTable(table_name)
    #result_df = sqlContext.sql("select* from "+table_name)
    #sqlContext.sql("use " + "lhh")

    #try:
        #result_df.write.saveAsTable("yufeng_test"+"."+table_name)
    #except Exception as e:
    #    print "hive insert error {}".format(e)
'''

def df_save_to_hive_test():
    from pyspark import SparkContext, SparkConf, HiveContext
    sc = SparkContext('local', 'to_hive_job2')
    sqlContext = HiveContext(sc)
    test_seckillTarget2_df = pd.read_excel('/home/jsw-data-Analysis/elt/uncleaned/test_seckillTarget2.xlsx',
                                           encoding="utf-8")
    spark_df = sqlContext.createDataFrame(test_seckillTarget2_df)
    print "dataFrame type =========================="
    print type(spark_df)
    print "spark_df size============================="
    spark_df.registerTempTable("test")
    result_df = sqlContext.sql("select* from " + "test")
    # sqlContext.sql("use " + "lhh")
    print "spark_df size============================="
    print type(result_df)
    try:
        result_df.write.saveAsTable("yufeng_test" + "." + "test_hive")
    except Exception as e:
        print "hive insert error {}".format(e)



def df_read_from_hive(souce_conf_df):
    from pyspark import SparkContext, SparkConf, HiveContext
    sc = SparkContext('local', 'to_hive_job2')
    sqlContext = HiveContext(sc)
    db_name = souce_conf_df['db_name'].values[0]
    table_name = souce_conf_df['table_name'].values[0]
    sqlContext.sql("use "+db_name)
    data_df = sqlContext.sql("select * from " + table_name).toPandas
    return data_df
    
    def obtain_hive_columns_by_souce_conf_df(souce_conf_df):
    from pyspark import SparkContext, SparkConf, HiveContext
    sc = SparkContext('local', 'to_hive_job2')
    sqlContext = HiveContext(sc)
    db_name = souce_conf_df['db_name'].values[0]
    table_name = souce_conf_df['table_name'].values[0]
    print "hive sql =========================="
    print "use " + db_name+";"
    sqlContext.sql("use " + db_name+";")
    hive_df = sqlContext.sql("select * from " + table_name).toPandas
    return hive_df.columns.tolist()

'''


def df_read_from_excel(souce_conf_df):
    url = souce_conf_df['url'].values[0]
    file_name = souce_conf_df['table_name'].values[0]
    data_df = pd.read_csv(url + file_name + '.xlxs')
    return data_df

def df_read_from_csv(souce_conf_df):
    url = souce_conf_df['url'].values[0]
    file_name = souce_conf_df['table_name'].values[0]
    data_df = pd.read_csv(url+file_name+'.csv')
    return data_df

def df_read_from_mysql(source_conf_df):
    host_name = source_conf_df['url'].values[0]
    user_name = source_conf_df['user_name'].values[0]
    pass_word = source_conf_df['pass_word'].values[0]
    db_name = source_conf_df['db_name'].values[0]
    table = source_conf_df['table_name'].values[0]

    mysql_cn = MySQLdb.connect(host=host_name, port=3306,
                               user=user_name,
                               passwd=pass_word,
                               db=db_name,charset="utf8")
    data_df = pd.read_sql('select * from {};'.format(table), con=mysql_cn)

    #db_util = MysqlUtil(source_conf_df['url'].values[0],
                        #source_conf_df['user_name'].values[0],
                        #source_conf_df['pass_word'].values[0],
                        #source_conf_df['db_name'].values[0])
    #table = source_conf_df['table_name'].values[0]
    #selected_column = '*'
    #print '====== table {} perform data fetching ======'.format(table)
    #print 'query data from mysql database...'
    #source_date_df = db_util.query_from_table_to_dataframe(table, selected_column, [])
    return data_df



#if __name__ == '__main__':
    #job_conf_df = obtain_job_conf_by_id(1)
    #source_conf_df = obtain_source_conf_by_id(int(job_conf_df['source_id']))
    #target_conf_df = obtain_target_conf_by_id(int(job_conf_df['target_id']))
    #source_data_df = obtain_source_date_by_conf(source_conf_df)
    #filter_rules_df = obtain_filter_rule_date_by_job_id(5)
    #column_result = obtain_ignore_columns_by_filter_rules_df(filter_rules_df)
    #column_result2 = obtain_enc_columns_by_filter_rules_df(filter_rules_df)
    #column_result3 = obtain_filter_rules_by_filter_rules_df(filter_rules_df)
    #print column_result3
    #df_save_to_hive_test()
    #result_df = execute_job(source_conf_df, source_data_df, filter_rules_df, target_conf_df, 1)


