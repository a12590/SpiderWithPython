#!/usr/local/miniconda2/bin/python
# _*_ coding: utf-8 _*_

import os
import sys
from cryptography.fernet import Fernet
import base64
import pandas as pd
import json
import ast

from app.utils.mysql_util import MysqlUtil
from app.models.mission import Mission

module_path = os.path.abspath(os.path.join('..'))
sys.path.append(module_path)
reload(sys)
sys.setdefaultencoding('utf8')

from flask import jsonify, request
from app.utils.meta_util import obtain_job_conf_by_id
from app.utils.meta_util import obtain_source_conf_by_id
from app.utils.meta_util import obtain_target_conf_by_id
from app.utils.meta_util import obtain_source_date_by_conf
from app.utils.meta_util import obtain_filter_rule_date_by_job_id
from app.utils.meta_util import execute_job
from app.utils.meta_util import save_modifyed_columns_and_job_id
from app.utils.meta_util import chenge_job_stat_starting
from app.utils.meta_util import chenge_job_stat_end
from app.utils.meta_util import chenge_job_stat_failure
from app.utils.meta_util import delete_target_date_by_conf

#from app.utils.meta_util import obtain_hive_columns_by_souce_conf_df
from app.utils.meta_util import obtain_target_date_by_conf
import pandas as pd
import json
from app import app, db




@app.route('/')
def hello_world():
    return 'Hello World!'


"""
根据任务id执行任务
"""
@app.route('/execute_job_by_id/', methods=['GET', 'POST'])
def execute_job_by_id(job_id=None):
    job_id = request.args.get("job_id")


    returnModel = {}
    try:
        job_conf_df = obtain_job_conf_by_id(job_id)
        target_conf_df = obtain_target_conf_by_id(int(job_conf_df['target_id']))
        db_type = target_conf_df['db_type'].values[0]
        if db_type == 1:
            delete_target_date_by_conf(target_conf_df)
        # 改变任务状态为已启动
        chenge_job_stat_starting(job_id)
        # 得到任务配置信息
        #job_conf_df = obtain_job_conf_by_id(job_id)
        # 得到源配置信息
        souce_conf_df = obtain_source_conf_by_id(int(job_conf_df['source_id']))
        # 得到数据源数据集
        souce_date_df = obtain_source_date_by_conf(souce_conf_df)
        # 根据任务id得到过滤规则
        filter_rules_df = obtain_filter_rule_date_by_job_id(job_id)
        # 得到目的配置信息
        target_conf_df = obtain_target_conf_by_id(int(job_conf_df['target_id']))
        # 执行任务
        execute_job(souce_conf_df, souce_date_df, filter_rules_df, target_conf_df, job_id)
        returnModel["result"] = True

        #改变状态为执行成功
        chenge_job_stat_end(job_id)

    except Exception as e:
        #改变状态为执行失败
        chenge_job_stat_failure(job_id)
        returnModel["result"] = False
        returnModel["reason"] = e
    response = jsonify(json.dumps(returnModel, ensure_ascii=False))
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

"""
根据target_id删除抽取目的地的数据
"""
@app.route('/delete_target_data_by_target_id/', methods=['GET'])
def delete_target_data_by_target_id(target_id=None):
    target_id = request.args.get("target_id")
    returnModel = {}
    try:
        target_conf_df = obtain_target_conf_by_id(target_id)
        delete_target_date_by_conf(target_conf_df)
        returnModel["result"] = True
    except Exception as e:
        returnModel["result"] = False
        returnModel["reason"] = e
    response = jsonify(json.dumps(returnModel, ensure_ascii=False))
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response



"""
根据任务id和预览条数预览执行结果数据集
"""

@app.route('/preview_processed_result_by_id_and_limit/', methods=['GET'])
def preview_processed_result_by_id_and_limit(job_id=None):
    job_id = request.args.get("job_id")
    try:
        # 得到任务配置信息
        job_conf_df = obtain_job_conf_by_id(job_id)
        # 得到目的配置信息
        target_conf_df = obtain_target_conf_by_id(int(job_conf_df['target_id']))
        if target_conf_df['db_type'].values[0] == 1:
            # 预览mysql中的数据
            mysql_df = obtain_source_date_by_conf(target_conf_df)
            response = jsonify(mysql_df[0:4].to_json(orient='table', force_ascii=False))
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response
        elif target_conf_df['db_type'].values[0] == 3:
            # 预览csv中的数据
            url = '/home/jsw-data-Analysis/elt/uncleaned/'
            table_name = target_conf_df['table_name'].values[0]
            csv_address = url + table_name
            print 'preview data form /home/jsw-data-Analysis/elt/uncleaned/{}'.format(table_name)
            df_csv = pd.read_csv(csv_address, index_col=None, encoding="utf-8")
            response = jsonify(df_csv[0:4].to_json(orient='table', force_ascii=False))
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response
        elif target_conf_df['db_type'].values[0] == 4:
            # 预览excel中的数据
            url = target_conf_df['url'].values[0]
            table_name = target_conf_df['table_name'].values[0]
            excel_address = url + table_name + ".xlsx"
            df_excel = pd.read_excel(excel_address, encoding="utf-8")
            response = jsonify(df_excel[0:4].to_json(orient='table', force_ascii=False))
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response
    except:
        return jsonify({'result': 'error'})


"""
获得excel文件的列名称
"""


@app.route('/save_job_info_and_obtain_csv_columns/', methods=['GET'])
def obtain_excel_columns(job_id=None):
    name = request.args.get("name")
    stat = request.args.get("stat")
    sourceId = request.args.get("sourceId")
    targetId = request.args.get("targetId")
    createName = request.args.get("createName")

    mission = Mission(
        name=name,
        stat=stat,
        remark="",
        source_id=sourceId,
        target_id=targetId,
        create_name=createName
    )
    #保存任务信息
    db.session.add(mission)
    db.session.commit()

    #通过任务名称获得任务Id
    tmpMission = Mission.query.filter_by(name=name).first()
    job_id = tmpMission.id
    #
    # 得到源配置信息
    souce_conf_df = obtain_source_conf_by_id(sourceId)
    url = souce_conf_df['url'].values[0]
    file_name = souce_conf_df['table_name'].values[0]
    csv_df = pd.read_excel(url + file_name, encoding="utf-8")
    columns_list = csv_df.columns.values.tolist()
    # 获得重复列名称
    duplicate_columns = []
    columns_set = set()
    for column in columns_list:
        if column not in columns_set:
            columns_set.add(column)
        else:
            duplicate_columns.append(column)
    returnModel = {}
    returnModel['datum'] = columns_list
    returnModel['dupli'] = duplicate_columns
    response = jsonify(json.dumps(returnModel, ensure_ascii=False))
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response



"""
获得csv文件的列名称
"""

@app.route('/save_job_info_and_obtain_csv_columns/', methods=['GET'])
def obtain_csv_columns(job_id=None):
    name = request.args.get("name")
    stat = request.args.get("stat")
    sourceId = request.args.get("sourceId")
    targetId = request.args.get("targetId")
    createName = request.args.get("createName")

    mission = Mission(
        name=name,
        stat=stat,
        remark="",
        source_id=sourceId,
        target_id=targetId,
        create_name=createName
    )
    #保存任务信息
    db.session.add(mission)
    db.session.commit()

    #通过任务名称获得任务Id
    tmpMission = Mission.query.filter_by(name=name).first()
    job_id = tmpMission.id
    #
    # 得到源配置信息
    souce_conf_df = obtain_source_conf_by_id(sourceId)
    url = souce_conf_df['url'].values[0]
    file_name = souce_conf_df['table_name'].values[0]
    csv_df = pd.read_csv(url + file_name, encoding="utf-8")
    columns_list = csv_df.columns.values.tolist()
    # 获得重复列名称
    duplicate_columns = []
    columns_set = set()
    for column in columns_list:
        if column not in columns_set:
            columns_set.add(column)
        else:
            duplicate_columns.append(column)
    returnModel = {}
    returnModel['datum'] = columns_list
    returnModel['dupli'] = duplicate_columns
    response = jsonify(json.dumps(returnModel, ensure_ascii=False))
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


"""
获得hive表的列名称
"""

@app.route('/save_job_info_and_obtain_hive_columns/', methods=['GET'])
def save_job_info_and_obtain_hive_columns(job_id=None):
    name = request.args.get("name")
    stat = request.args.get("stat")
    sourceId = request.args.get("sourceId")
    targetId = request.args.get("targetId")
    createName = request.args.get("createName")

    mission = Mission(
        name=name,
        stat=stat,
        remark="",
        source_id=sourceId,
        target_id=targetId,
        create_name=createName
    )
    #保存任务信息
    db.session.add(mission)
    db.session.commit()

    #通过任务名称获得任务Id
    tmpMission = Mission.query.filter_by(name=name).first()
    job_id = tmpMission.id

    # 得到源配置信息
    souce_conf_df = obtain_source_conf_by_id(sourceId)
    hive_columns = []
    #obtain_hive_columns_by_souce_conf_df(souce_conf_df)
    returnModel = {}
    returnModel['datum'] = hive_columns
    response = jsonify(json.dumps(returnModel, ensure_ascii=False))
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

"""
获得hive表的列名称
"""


@app.route('/obtain_hive_columns_by_job_id/<int:job_id>/', methods=['GET'])
def obtain_hive_columns(job_id=None):
    # 得到任务配置信息
    job_conf_df = obtain_job_conf_by_id(job_id)
    # 得到源配置信息
    souce_conf_df = obtain_source_conf_by_id(int(job_conf_df['source_id']))
    hive_columns = []
    #obtain_hive_columns_by_souce_conf_df(souce_conf_df)
    returnModel = {}
    returnModel['datum'] = hive_columns
    response = jsonify(json.dumps(returnModel, ensure_ascii=False))
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


"""
获得mysql表的列名称
"""


@app.route('/obtain_mysql_columns_by_job_id/<int:job_id>/', methods=['GET'])
def obtain_mysql_columns(job_id=None):
    # 得到任务配置信息
    job_conf_df = obtain_job_conf_by_id(job_id)
    # 得到源配置信息
    souce_conf_df = obtain_source_conf_by_id(int(job_conf_df['source_id']))
    db_util = MysqlUtil(souce_conf_df['url'].values[0],
                        souce_conf_df['user_name'].values[0],
                        souce_conf_df['pass_word'].values[0],
                        souce_conf_df['db_name'].values[0])
    table = souce_conf_df['table_name'].values[0]
    selected_column = '*'
    mysql_date_df = db_util.query_from_table_to_dataframe(table, selected_column, [])
    returnModel = {}
    returnModel['datum'] = str(list(set(mysql_date_df.columns.tolist()))).replace("u'", "'").decode("unicode_escape")
    response = jsonify(json.dumps(returnModel, ensure_ascii=False))
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

"""
保存修改的列名称
"""

@app.route('/save_modify_columns_by_job_id/<int:job_id>/<string:columns>', methods=['GET'])
def save_modify_columns_by_job_id(job_id=None, columns=None):
    # 得到任务配置信息
    save_modifyed_columns_and_job_id(columns, job_id)
    returnModel = {}
    returnModel["result"] = True
    response = jsonify(json.dumps(returnModel, ensure_ascii=False))
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


# 获取csv table
@app.route('/csv_tables')
def csv_tables():
    file_dir = "/home/jsw-data-Analysis/elt/uncleaned"
    tables = []
    res = {}
    for root, dirs, files in os.walk(file_dir):
        for file in files:
            if os.path.splitext(file)[1] == '.csv':
                tables.append(file)
    res["data"] = tables
    response = jsonify(res)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


# 获取excel csv_table
@app.route('/excel_tables')
def excel_tables():
    file_dir = "/home/jsw-data-Analysis/elt/uncleaned"
    tables = []
    res = {}
    for root, dirs, files in os.walk(file_dir):
        for file in files:
            if os.path.splitext(file)[1] == '.xlsx':
                tables.append(file)
    res["data"] = tables
    response = jsonify(res)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

# 预览认任务执行结果任务结果
@app.route('/results_preview')
def results_preview():
    targetId = request.args.values("targetId")
    returnModel = {}
    try:
        target_conf_df = obtain_target_conf_by_id(targetId)
        target_data_df = obtain_target_date_by_conf(target_conf_df)

        returnModel["result"] = True
        returnModel["datum"] = jsonify(target_data_df[0:10].to_json(orient='table', force_ascii=False))

    except Exception as e:
        returnModel["result"] = False
        returnModel["reason"] = e
    response = jsonify(json.dumps(returnModel, ensure_ascii=False))
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


if __name__ == '__main__':
    app.run(host='192.168.1.112', port=5009)
