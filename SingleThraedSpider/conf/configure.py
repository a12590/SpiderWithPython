#!/usr/local/miniconda2/bin/python
# _*_ coding: utf-8 _*_
from __future__ import absolute_import, division, print_function
"""
@author: yf
"""
import os
import sys
import time
module_path = os.path.abspath(os.path.join('..'))
sys.path.append(module_path)

reload(sys)
sys.setdefaultencoding('utf8')

class Configure(object):
    """global config"""
    # webapp
    host = '10.108.211.136'
    spark_master = 'yarn-client'
    CSRF_ENABLED = True
    SECRET_KEY = '\xa6\x82\xc9x\xf9\x9f2\x02L\xe1?;\x99\xe6>\x18_\xfe\x18`\xccVC\xc8'
    static_folder = module_path + '/main/webapp/templates/assets'
    template_folder = module_path + '/main/webapp/templates'
    template_folder2 = module_path + '/main/webapp/web/static/templates'
    # TODO:
    upload_folder = module_path + '/data/upload_folder'
    # TODO
    spark_events_path = module_path + '/data/spark_events'
    data_file_path = module_path + '/data/data_file/'
    output_path = module_path + '/data/output'
    hdfs_path = 'hdfs://10.108.211.136:8020'

    # webapp4
    SPARK_HOME = '/usr/hdp/current/spark-client'
    VCF_FOLDER = module_path + '/data/upload/'
    dataFile_folder = module_path + '/data/datafile_upload/'
    codeFile_folder = module_path + '/data/codefile_upload/'
    # VCF_FUNCTIONS = module_path + '/'
    SPA_PATH = '/usr/hdp/current/spark-client/examples/src/main/python/'

    # livy
    livy_host = 'master.hadoop:8998'
    spark_host = 'master.hadoop:7077'


