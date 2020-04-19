#!/usr/local/miniconda2/bin/python
# _*_ coding: utf-8 _*_
import ast

import DManager
from flask import jsonify
import json
from datetime import date, datetime
class MyEncoder(json.JSONEncoder):
    def default(self, obj):
      if isinstance(obj, datetime):
          return obj.strftime('%Y-%m-%d %H:%M:%S')
      elif isinstance(obj, date):
          return obj.strftime('%Y-%m-%d')
      else:
          return json.JSONEncoder.default(self, obj)


def row_dict_func(strName, row):
    row_dict = dict()
    row_dict["name"] = strName
    row_dict["value"] = row
    return row_dict


class OutputDataManager:

    def __init__(self):
        self.dal_manager = DManager.db_util

    def SearchJobResultName(self,userId, searchContext):
        rows = self.dal_manager.SearchJobResultName(userId, searchContext)
        output = dict()
        if rows is not None and len(rows) > 0:
            output["SUCCESS"] = True
            output["DATA"] = []
            row_dict = dict()
            for row in rows:
                row_dict_dict = dict()
                row_dict_dict["jobId"] = row[0]
                row_dict_dict["jobName"] = row[2]
                if row[3] == 0:
                    row_dict_dict["status"] = "Submited Failed"
                elif row[3] == 1:
                    row_dict_dict["status"] = "Success"
                elif row[4] == 2:
                    row_dict_dict["status"] = "Running"
                else:
                    row_dict_dict["status"] = "Failed"
                row_dict_dict["logView"] = "日志查看"
                row_dict_dict["ParaView"] = "配置查看"
                row_dict_dict["dataView"] = "数据集查看"
                output["DATA"].append(row_dict_dict)
        else:
            output["SUCCESS"] = False
        return output

    def get_filepath_res(self,userId,jobId):
        row = self.dal_manager.get_filepath_res(userId,jobId)
        return row[0]

    def get_filename_res(self,userId,jobId):
        row = self.dal_manager.get_filename_res(userId,jobId)
        return row[0]


    def SearchJobResultId(self, userId,JobId):
        row = self.dal_manager.SearchJobResultId(userId,JobId)
        output = dict()
        if row is not None and len(row) > 0:
            output["SUCCESS"] = True
            output["DATA"] = []
            row_dict = dict()
            row_dict["jobId"] = row[0]
            row_dict["jobName"] = row[2]
            if row[3] == 0:
                row_dict["status"] = "Submited Failed"
            elif row[3] == 1:
                row_dict["status"] = "Success"
            elif row[3] == 2:
                row_dict["status"] = "Running"
            else:
                row_dict["status"] = "Failed"
            row_dict["logView"] = "日志查看"
            row_dict["ParaView"] = "配置查看"
            row_dict["dataView"] = "数据集查看"
            output["DATA"].append(row_dict)
        else:
            output["SUCCESS"] = False
        return output

    def SearchResResultId(self, userId,jobId):
        rows = self.dal_manager.getResResult_id(userId,jobId)
        output = dict()
        if rows is not None and len(rows) > 0:
            output["SUCCESS"] = True
            output["DATA"] = []
            row_dict = dict()
            row_dict["jobId"] = rows[0]
            row_dict["path"] = rows[1]
            row_dict["filename"] = rows[2]
            row_dict["createdon"] = json.dumps(rows[3], cls=MyEncoder)
            # row_dict["createdon"] = json.dumps(row[2], cls=MyEncoder)
            # row_dict["modifiedon"] = json.dumps(row[3], cls=MyEncoder)
            output["DATA"].append(row_dict)
        else:
            output["SUCCESS"] = False
        return output

    def SearchResResultName(self,userId, searchContext):
        rows = self.dal_manager.getResResult_context(userId, searchContext)
        output = dict()
        if rows is not None and len(rows) > 0:
            output["SUCCESS"] = True
            output["DATA"] = []
            for row in rows:
                row_dict = dict()
                row_dict["jobId"] = row[0]
                row_dict["path"] = row[1]
                row_dict["filename"] = row[2]
                row_dict["createdon"] = json.dumps(row[3], cls=MyEncoder)
                output["DATA"].append(row_dict)
        else:
            output["SUCCESS"] = False
        return output

    def getJobResult(self,userId):
        rows = self.dal_manager.getJobResult(userId)
        output = dict()
        if rows is not None and len(rows) > 0:
            output["SUCCESS"] = True
            output["DATA"] = []
            for row in rows:
                row_dict_dict = dict()
                row_dict_dict["jobId"] = row[0]
                row_dict_dict["jobName"] = row[2]
                if row[3] == 0:
                    row_dict_dict["status"] = "Submited Failed"
                elif row[3] == 1:
                    row_dict_dict["status"] = "Success"
                elif row[3] == 2:
                    row_dict_dict["status"] = "Running"
                else:
                    row_dict_dict["status"] = "Failed"
                row_dict_dict["logView"] = "日志查看"
                row_dict_dict["ParaView"] = "配置查看"
                row_dict_dict["dataView"] = "数据集查看"
                output["DATA"].append(row_dict_dict)
        else:
            output["SUCCESS"] = False
        return output


    def getJobProperty(self,jobId):
        rows = self.dal_manager.getJobProperty(jobId)
        if rows is not None and len(rows) > 0:
            return rows[4]
        else:
            return -1

    def update_data_file(self,jobId,label,weights,firstLine,separate):
        rowcount = self.dal_manager. \
            update_data_file(jobId, label, weights,firstLine,separate)
        output = dict()
        if rowcount is not None and rowcount > 0:
            output["SUCCESS"] = True
        else:
            output["SUCCESS"] = False

        return output

    def update_date_jobId(self,id,jobId):
        rowcount = self.dal_manager. \
            update_date_jobId(id,jobId)

    def update_model_algos(self,jobId,algoPara):
        rowcount = self.dal_manager. \
            update_model_info(jobId,algoPara)
        output = dict()
        if rowcount is not None and rowcount > 0:
            output["SUCCESS"] = True
        else:
            output["SUCCESS"] = False

        return output

    def get_model_list_withDataSet(self, userId):
        all_rows = self.dal_manager. \
            get_model_list_withDataSet(userId)
        output_ = dict()
        if all_rows is not None and len(all_rows) > 0:
            output_["modelList"] = []
            row_key = dict()
            for row in all_rows:
                if row[1] in row_key.keys():
                    row_key[row[1]]["modelid"].append(row[0])
                    row_key[row[1]]["modelName"].append(row[2])
                else:
                    row_key[row[1]] = {"modelid": [], "modelName": []}
                    row_key[row[1]]["modelid"].append(row[0])
                    row_key[row[1]]["modelName"].append(row[2])
            output_["modelList"].append(row_key)
        else:
            output_["modelList"] = []

        output = dict()
        output["modelList"] = []
        if output_["modelList"]:
            for idexs, key in enumerate(output_["modelList"][0]):
                ids = output_["modelList"][0][key]["modelid"]
                names = output_["modelList"][0][key]["modelName"]
                row_dict_dict = dict()
                row_dict_dict["index"] = "3-" + str(idexs + 1)
                row_dict_dict["subFile"] = []
                row_dict_dict["Name"] = key
                for index, id in enumerate(ids):
                    row_subFile = dict()
                    row_subFile["type"] = "model"
                    row_subFile["id"] = id
                    row_subFile["fileName"] = names[index]
                    row_dict_dict["subFile"].append(row_subFile)
                output["modelList"].append(row_dict_dict)
        return output

    def insert_new_job(self,userId,jobName):
        rowid = self.dal_manager. \
            insert_new_job(userId,jobName)
        output = dict()
        if rowid is not None:
            output["SUCCESS"] = True
            output["DATA"] = dict()
            output["DATA"]["id"] = rowid
        else:
            output["SUCCESS"] = False
        return output

    def update_job_jobName(self,userId,jobId,jobName):
        rowid = self.dal_manager. \
            update_job_jobName(userId, jobId,jobName)
        output = dict()
        if rowid is not None:
            output["SUCCESS"] = True
            output["DATA"] = dict()
            output["DATA"]["id"] = rowid
        else:
            output["SUCCESS"] = False
        return output

    def insert_new_file(self, filename, filecontent, userid,hdfs_dataPath):
        rowid = self.dal_manager.\
            insert_new_file(filename, filecontent, userid,hdfs_dataPath)

        output = dict()
        if rowid is not None:
            output["SUCCESS"] = True
            output["DATA"] = dict()
            output["DATA"]["id"] = rowid
        else:
            output["SUCCESS"] = False

        return output

    def insert_new_user(self,userName, password):
        rowid = self.dal_manager. \
            insert_new_user(userName, password)

    def insert_new_data_file(self, filename, path, userid,datasetName,firstLine,separate):
        rowid = self.dal_manager.\
            insert_new_data_file(filename, path, userid,datasetName,firstLine,separate)

        output = dict()
        if rowid is not None:
            output["SUCCESS"] = True
            output["DATA"] = dict()
            output["DATA"]["id"] = rowid
        else:
            output["SUCCESS"] = False

        return output

    def update_file(self, fileid, filecontent):
        rowcount = self.dal_manager.update_file(fileid, filecontent)

        output = dict()
        if rowcount is not None and rowcount > 0:
            output["SUCCESS"] = True
        else:
            output["SUCCESS"] = False
        return output

    def get_evaluate_info(self,userId, ids):
        time_algos = self.dal_manager.get_evaluate_info(userId, ids)
        output = dict()
        output["DATA"] = dict()
        output["DATA"]["time"] = []
        output["DATA"]["auc_score"] = []
        output["DATA"]["auc_std_err"] = []
        if time_algos is not None and time_algos > 0:
            output["SUCCESS"] = True
            for time_algo in time_algos:
                output["DATA"]["time"].append(time_algo[0])
                # output["DATA"]["auc_score"].append(time_algo[1])
                # output["DATA"]["auc_std_err"].append(time_algo[2])
                json_str = ast.literal_eval(time_algo[1])
                print json_str
                for key in json_str:
                    print key
                    output["DATA"][key].append(json_str[key])
        else:
            output["SUCCESS"] = False
        return output

    def get_model_algos(self,jobId):
        algo = self.dal_manager.get_model_algos(jobId)
        output = dict()
        # output["DATA"] = []
        if algo is not None and algo > 0:
            output["SUCCESS"] = True
            json_str = ast.literal_eval(algo[0])
            output["DATA"] = json_str
            # for key in json_str:
            #     output["DATA"].append(row_dict_func(key, json_str[key]))
        else:
            output["SUCCESS"] = False

        return output

    def exist(self,jobId):
        exist = self.dal_manager.exist(jobId)
        output = dict()
        if exist is not None and exist > 0:
            output["SUCCESS"] = True
        else:
            output["SUCCESS"] = False
        return output

    def exist_datefile(self,jobId):
        exist = self.dal_manager.exist_datefile(jobId)
        output = dict()
        if exist is not None and exist > 0:
            output["SUCCESS"] = True
        else:
            output["SUCCESS"] = False
        return output

    def get_model_infos(self,id):
        algo = self.dal_manager.get_model_details(id)
        output = dict()
        # output["DATA"] = []
        if algo is not None and algo > 0:
            output["SUCCESS"] = True
            json_str = ast.literal_eval(algo[0])
            output["DATA"] = json_str
            # for key in json_str:
            #     output["DATA"].append(row_dict_func(key, json_str[key]))
        else:
            output["SUCCESS"] = False
        return output

    def insert_new_model_with_copy(self,userId,jobId,id):
        rowid = self.dal_manager.insert_new_model_with_copy(userId,jobId, id)
        print rowid
        algo = self.dal_manager.get_model_details(rowid)
        output = dict()
        # output["DATA"] = []
        if algo is not None and algo > 0:
            output["SUCCESS"] = True
            json_str = ast.literal_eval(algo[0])
            output["DATA"] = json_str
            self.dal_manager.update_job_property_model(jobId)
            # for key in json_str:
            #     output["DATA"].append(row_dict_func(key, json_str[key]))
        else:
            output["SUCCESS"] = False
        return output

    def get_algo_infos(self,userId,jobId,id):
        algo = self.dal_manager.get_algo_details(id)
        algoName = ""
        if id == 1:
            algoName = "Cor"
        elif id == 2:
            algoName = "Apriori"
        elif id == 3:
            algoName = "DecisionTreeClassfier"
        elif id == 4:
            algoName = "SVM"
        elif id == 5:
            algoName = "NaiveBayes"
        elif id == 6:
            algoName = "PU-learning"
        elif id == 7:
            algoName = "LogisticRegression"
        elif id == 8:
            algoName = "GradientBoostedTreesRegression"
        elif id == 9:
            algoName = "LinearRegression"
        elif id == 10:
            algoName = "RidgeRegression"
        output = dict()
        output["DATA"] = []
        if algo is not None and algo > 0:
            output["SUCCESS"] = True
            json_str = ast.literal_eval(algo[0])
            for key in json_str:
                output["DATA"].append(row_dict_func(key, json_str[key]))
            self.dal_manager.update_job_property(jobId)
            modelName = self.dal_manager.getJobName(jobId)
            self.dal_manager.insert_new_model_with_new(userId, jobId, output["DATA"], algoName,modelName)
        else:
            output["SUCCESS"] = False
        return output

    def update_job_log(self,jobId,output):
        rowupdated = self.dal_manager. \
            update_job_log(jobId,output)
        output = dict()
        if rowupdated is not None and rowupdated > 0:
            output["SUCCESS"] = True
        else:
            output["SUCCESS"] = False
        return output

    def update_job_status_s(self,jobId):
        rowupdated = self.dal_manager. \
            update_job_status_s(jobId)
        output = dict()
        if rowupdated is not None and rowupdated > 0:
            output["SUCCESS"] = True
        else:
            output["SUCCESS"] = False

        return output

    def update_job_status_r(self, jobId):
        rowupdated = self.dal_manager. \
            update_job_status_r(jobId)
        output = dict()
        if rowupdated is not None and rowupdated > 0:
            output["SUCCESS"] = True
        else:
            output["SUCCESS"] = False

        return output

    def update_job_status_f(self, jobId):
        rowupdated = self.dal_manager. \
            update_job_status_f(jobId)
        output = dict()
        if rowupdated is not None and rowupdated > 0:
            output["SUCCESS"] = True
        else:
            output["SUCCESS"] = False

        return output

    def get_job_log(self,jobId):
        row = self.dal_manager.get_job_log(jobId)
        output = dict()
        if row is not None and len(row) > 0:
            output["DATA"] = []
            output["SUCCESS"] = True
            # logs = """
            # INFO SparkContext: Running Spark version 2.3.1.3.0.1.0-187
            # INFO SparkContext: Submitted application: PU分类
            # INFO SecurityManager: Changing view acls to: yufeng,hdfs
            # INFO SecurityManager: Changing modify acls to: yufeng,hdfs
            # INFO SecurityManager: Changing view acls groups to:
            # INFO SecurityManager: Changing modify acls groups to:
            # INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with vie permissions: Set(yufeng, hdfs); groups with view permissions: Set(); users  with modify permissions: Set(yufeng, hfs); groups with modify permissions: Set()
            # INFO Utils: Successfully started service 'sparkDriver' on port 43931.
            # INFO SparkEnv: Registering MapOutputTracker
            # INFO SparkEnv: Registering BlockManagerMaster
            # INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for gettingtopology information
            # INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
            # INFO DiskBlockManager: Created local directory at /tmp/blockmgr-b2b3c797-d5fb-4ed0-babd-0ba5e3bb630
            # INFO MemoryStore: MemoryStore started with capacity 366.3 MB
            # INFO SparkEnv: Registering OutputCommitCoordinator
            # INFO log: Logging initialized @14762ms
            # INFO Server: jetty-9.3.z-SNAPSHOT, build timestamp: 2018-06-06T01:11:56+08:00, git hash: 84205aa2f11a4f31f2a3b86d1bba2cc8ab69827
            # INFO Server: Started @15226ms
            # INFO AbstractConnector: Started ServerConnector@595f4da5{HTTP/1.1,[http/1.1]}{0.0.0.0:4040}
            # INFO Utils: Successfully started service 'SparkUI' on port 4040.
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@503fbbc6{/jobs,null,AVAILABLE,@Spark}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@5e8a459{/jobs/json,null,AVAILABLE,@Spak}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@43d455c9{/jobs/job,null,AVAILABLE,@Spak}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@40147317{/jobs/job/json,null,AVAILABLE@Spark}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@210f0cc1{/stages,null,AVAILABLE,@Spark
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@19542407{/stages/json,null,AVAILABLE,@park}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@6f95cd51{/stages/stage,null,AVAILABLE,Spark}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@6d868997{/stages/stage/json,null,AVAILBLE,@Spark}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@2c383e33{/stages/pool,null,AVAILABLE,@park}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@74a195a4{/stages/pool/json,null,AVAILALE,@Spark}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@76304b46{/storage,null,AVAILABLE,@Spar}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@2fa3be26{/storage/json,null,AVAILABLE,Spark}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@4287d447{/storage/rdd,null,AVAILABLE,@park}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@3af37506{/storage/rdd/json,null,AVAILALE,@Spark}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@4e6d7365{/environment,null,AVAILABLE,@park}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@7c0da600{/environment/json,null,AVAILALE,@Spark}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@d4602a{/executors,null,AVAILABLE,@Spar}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@21ae6e73{/executors/json,null,AVAILABL,@Spark}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@47dd778{/executors/threadDump,null,AVALABLE,@Spark}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@15515c51{/executors/threadDump/json,nul,AVAILABLE,@Spark}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@36a7abe1{/static,null,AVAILABLE,@Spark
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@100f9bbe{/,null,AVAILABLE,@Spark}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@13e9f2e2{/api,null,AVAILABLE,@Spark}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@3c4bc9fc{/jobs/job/kill,null,AVAILABLE@Spark}
            # INFO ContextHandler: Started o.s.j.s.ServletContextHandler@680362a{/stages/stage/kill,null,AVAILALE,@Spark}
            # INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://slave2.hadoop:4040
            # INFO RMProxy: Connecting to ResourceManager at slave2.hadoop/10.108.211.130:8050
            # INFO Client: Requesting a new application from cluster with 4 NodeManagers
            # INFO Configuration: found resource resource-types.xml at file:/etc/hadoop/3.0.1.0-187/0/resource-ypes.xml
            # INFO Client: Verifying our application has not requested more than the maximum memory capability f the cluster (55296 MB per container)
            # INFO Client: Will allocate AM container, with 896 MB memory including 384 MB overhead
            # INFO Client: Setting up container launch context for our AM
            # INFO Client: Setting up the launch environment for our AM container
            # INFO Client: Preparing resources for our AM container
            # INFO Client: Use hdfs cache file as spark.yarn.archive for HDP, hdfsCacheFile:hdfs://slave2.hadoo:8020/hdp/apps/3.0.1.0-187/spark2/spark2-hdp-yarn-archive.tar.gz
            # INFO Client: Source and destination file systems are the same. Not copying hdfs://slave2.hadoop:820/hdp/apps/3.0.1.0-187/spark2/spark2-hdp-yarn-archive.tar.gz
            # INFO Client: Distribute hdfs cache file as spark.sql.hive.metastore.jars for HDP, hdfsCacheFile:hfs://slave2.hadoop:8020/hdp/apps/3.0.1.0-187/spark2/spark2-hdp-hive-archive.tar.gz
            # INFO Client: Source and destination file systems are the same. Not copying hdfs://slave2.hadoop:820/hdp/apps/3.0.1.0-187/spark2/spark2-hdp-hive-archive.tar.gz
            # INFO Client: Uploading resource file:/usr/hdp/current/spark2-client/examples/jars/scopt_2.11-3.7..jar -> hdfs://slave2.hadoop:8020/user/hdfs/.sparkStaging/application_1542595228437_0060/scopt_2.11-3.7.0.jar
            # INFO Client: Uploading resource file:/usr/hdp/current/spark2-client/examples/jars/spark-examples_.11-2.3.1.3.0.1.0-187.jar -> hdfs://slave2.hadoop:8020/user/hdfs/.sparkStaging/application_1542595228437_0060/sparkexamples_2.11-2.3.1.3.0.1.0-187.jar
            # INFO Client: Uploading resource file:/tmp/spark-ed1bcfd9-22bd-4432-a005-dc57e173ceb6/__spark_conf_3373162717692390614.zip -> hdfs://slave2.hadoop:8020/user/hdfs/.sparkStaging/application_1542595228437_0060/__spar_conf__.zip
            # INFO SecurityManager: Changing view acls to: yufeng,hdfs
            # INFO SecurityManager: Changing modify acls to: yufeng,hdfs
            # INFO SecurityManager: Changing view acls groups to:
            # INFO SecurityManager: Changing modify acls groups to:
            # INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with vie permissions: Set(yufeng, hdfs); groups with view permissions: Set(); users  with modify permissions: Set(yufeng, hfs); groups with modify permissions: Set()
            # INFO Client: Submitting application application_1542595228437_0060 to ResourceManager
            # INFO YarnClientImpl: Submitted application application_1542595228437_0060
            # INFO SchedulerExtensionServices: Starting Yarn extension services with app application_154259522837_0060 and attemptId None
            # INFO Client: Application report for application_1542595228437_0060 (state: ACCEPTED)
            # INFO Client:
            #     """
            # logs = logs + row[0]
            output["DATA"].append(row_dict_func("log",row[0]))
        else:
            output["SUCCESS"] = False
        return output

    def get_datasetName(self,jobId):
        datasetName = self.dal_manager.get_datasetName(jobId)
        return datasetName

    def get_data_file_info(self,jobId):
        row = self.dal_manager.get_data_file_info(jobId)
        output = dict()
        if row is not None and len(row) > 0:
            output["SUCCESS"] = True
            output["DATA"] = []
            output["DATA"].append(row_dict_func("datasetName",row[15]))
            output["DATA"].append(row_dict_func("createdon", row[7].strftime('%Y-%m-%d %H:%M:%S')))
            output["DATA"].append(row_dict_func("separate", row[9]))
            output["DATA"].append(row_dict_func("firstLine", row[10]))
            output["DATA"].append(row_dict_func("Label", row[11]))
            # output["DATA"].append(row_dict_func("cat_list", row[12]))
            # output["DATA"].append(row_dict_func("num_list", row[13]))
            output["DATA"].append(row_dict_func("weights", row[14]))
        else:
            output["SUCCESS"] = False
        return output

    def get_data_file_info_l(self, jobId):
        all_rows = self.dal_manager.get_data_file_info_l(jobId)
        output = dict()
        if all_rows is not None and len(all_rows) > 0:
            output["SUCCESS"] = True
            output["DATA"] = []
            for row in all_rows:
                row_dict = dict()
                row_dict["datasetName"] = row[15]
                row_dict["createdon"] = row[7].strftime('%Y-%m-%d %H:%M:%S')
                row_dict["separate"] = row[9]
                row_dict["firstLine"] = row[10]
                row_dict["Label"] = row[11]
                row_dict["cat_list"] = row[12]
                row_dict["num_list"] = row[13]
                row_dict["weights"] = row[14]
                row_dict["filename"] = row[3]
                output["DATA"].append(row_dict)
        else:
            output["SUCCESS"] = False
        return output

    def get_data_file_infos(self,id):
        row = self.dal_manager.get_data_file_detail(id)
        output = dict()
        if row is not None and len(row) > 0:
            output["SUCCESS"] = True
            output["DATA"] = []
            output["DATA"].append(row_dict_func("datasetName", row[15]))
            output["DATA"].append(row_dict_func("createdon", row[7].strftime('%Y-%m-%d %H:%M:%S')))
            output["DATA"].append(row_dict_func("separate", row[9]))
            output["DATA"].append(row_dict_func("firstLine", row[10]))
            output["DATA"].append(row_dict_func("Label", row[11]))
            output["DATA"].append(row_dict_func("cat_list", row[12]))
            output["DATA"].append(row_dict_func("num_list", row[13]))
            output["DATA"].append(row_dict_func("weights", row[14]))
        else:
            output["SUCCESS"] = False
        return output

    def insert_new_data_file_with_copy(self,jobId,id):
        rowid = self.dal_manager.insert_new_data_file_with_copy(jobId,id)
        print rowid
        row = self.dal_manager.get_data_file_detail(rowid)
        output = dict()
        if row is not None and len(row) > 0:
            output["SUCCESS"] = True
            output["DATA"] = []
            output["DATA"].append(row_dict_func("datasetName", row[15]))
            output["DATA"].append(row_dict_func("createdon", row[7].strftime('%Y-%m-%d %H:%M:%S')))
            output["DATA"].append(row_dict_func("separate", row[9]))
            output["DATA"].append(row_dict_func("firstLine", row[10]))
            output["DATA"].append(row_dict_func("Label", row[11]))
            # output["DATA"].append(row_dict_func("cat_list", row[12]))
            # output["DATA"].append(row_dict_func("num_list", row[13]))
            output["DATA"].append(row_dict_func("weights", row[14]))
        else:
            output["SUCCESS"] = False
        return output


    def get_file(self, fileid):
        filename, content = self.dal_manager.get_file(fileid)

        output = dict()
        if filename is not None:
            output["SUCCESS"] = True
            output["DATA"] = dict()
            output["DATA"]["filename"] = filename
            output["DATA"]["content"] = content
        else:
            output["SUCCESS"] = False

        return output

    def get_exec_details(self, execid):
        sessionid, fileid, arguments = self.dal_manager.get_exec_details(execid)

        output = dict()
        if fileid is not None:
            output["SUCCESS"] = True
            output["DATA"] = dict()
            output["DATA"]["fileid"] = fileid
            output["DATA"]["sessionid"] = sessionid
            output["DATA"]["arguments"] = arguments
        else:
            output["SUCCESS"] = False

        return output

    def get_all_files(self, userid):
        all_rows = self.dal_manager.get_all_files(userid)
        output = dict()
        if all_rows is not None and len(all_rows) > 0:
            output["SUCCESS"] = True
            output["DATA"] = []
            for row in all_rows:
                row_dict = dict()
                row_dict["id"] = row[0]
                row_dict["filename"] = row[1]
                row_dict["createdon"] = json.dumps(row[2], cls=MyEncoder)
                row_dict["modifiedon"] = json.dumps(row[3], cls=MyEncoder)

                output["DATA"].append(row_dict)
        else:
            output["SUCCESS"] = False

        return output

    def get_all_data_files(self,userid):
        all_rows = self.dal_manager.get_all_data_files(userid)
        output = dict()
        if all_rows is not None and len(all_rows) > 0:
            output["data_status"] = True
            output["data_result"] = []
            for row in all_rows:
                row_dict = dict()
                row_dict["fileid"] = row[0]
                row_dict["filename"] = row[2]
                # row_dict["createdon"] = json.dumps(row[2], cls=MyEncoder)
                # row_dict["modifiedon"] = json.dumps(row[3], cls=MyEncoder)
                output["data_result"].append(row_dict)
        else:
            output["data_status"] = False

        return output

    def get_all_data_files_withDataSet(self, userid):
        all_rows = self.dal_manager.get_all_data_files(userid)
        output_ = dict()
        if all_rows is not None and len(all_rows) > 0:
            output_["data_result"] = []
            row_key = dict()
            for row in all_rows:
                if row[1] in row_key.keys():
                    row_key[row[1]]["fileid"].append(row[0])
                    row_key[row[1]]["filename"].append(row[2])
                else:
                    row_key[row[1]] = {"fileid": [], "filename": []}
                    row_key[row[1]]["fileid"].append(row[0])
                    row_key[row[1]]["filename"].append(row[2])
                # row_dict["createdon"] = json.dumps(row[2], cls=MyEncoder)
                # row_dict["modifiedon"] = json.dumps(row[3], cls=MyEncoder)
            output_["data_result"].append(row_key)
        else:
            output_["data_result"] = []

        output = dict()
        output["datalist"] = []
        if output_["data_result"]:
            for idexs,key in enumerate(output_["data_result"][0]):
                ids = output_["data_result"][0][key]["fileid"]
                names = output_["data_result"][0][key]["filename"]
                row_dict_dict = dict()
                row_dict_dict["index"] = "1-" + str(idexs + 1)
                row_dict_dict["subFile"] = []
                row_dict_dict["Name"] = key
                for index, id in enumerate(ids):
                    row_subFile = dict()
                    row_subFile["type"] = "data"
                    row_subFile["id"] = id
                    row_subFile["fileName"] = names[index]
                    row_dict_dict["subFile"].append(row_subFile)
                output["datalist"].append(row_dict_dict)
        return output

    def get_all_algorithm_withDataSet(self):
        output = dict()

        return output


    def get_all_data_files_infolist(self,userId):
        all_rows = self.dal_manager.get_all_data_files_infolist(userId)
        output = dict()
        if all_rows is not None and len(all_rows) > 0:
            output["data_status"] = True
            output["DATA"] = []
            for row in all_rows:
                row_dict = dict()

                row_dict["jobId"] = row[0]
                row_dict["path"] = row[1]
                row_dict["filename"] = row[2]
                row_dict["createdon"] = json.dumps(row[3], cls=MyEncoder)
                # row_dict["createdon"] = json.dumps(row[2], cls=MyEncoder)
                # row_dict["modifiedon"] = json.dumps(row[3], cls=MyEncoder)
                output["DATA"].append(row_dict)
        else:
            output["data_status"] = False

        return output

    def getResResult(self, userId):
        rows = self.dal_manager.getResResult(userId)
        output = dict()
        if rows is not None and len(rows) > 0:
            output["SUCCESS"] = True
            output["DATA"] = []
            for row in rows:
                row_dict = dict()
                row_dict["jobId"] = row[0]
                row_dict["path"] = row[1]
                row_dict["filename"] = row[2]
                row_dict["createdon"] = json.dumps(row[3], cls=MyEncoder).replace("\"","")
                # row_dict["createdon"] = json.dumps(row[2], cls=MyEncoder)
                # row_dict["modifiedon"] = json.dumps(row[3], cls=MyEncoder)
                output["DATA"].append(row_dict)
        else:
            output["SUCCESS"] = False
        return output


    def delete_file(self, fileid):
        rowcount = self.dal_manager.delete_file(fileid)

        output = dict()
        if rowcount is not None and rowcount > 0:
            output["SUCCESS"] = True
        else:
            output["SUCCESS"] = False

        return output

    def delete_data_file(self, fileid):
        rowcount = self.dal_manager.delete_data_file(fileid)

        output = dict()
        if rowcount is not None and rowcount > 0:
            output["SUCCESS"] = True
        else:
            output["SUCCESS"] = False

        return output

    def insert_executed_file(self, fileid, args, output_log, output_print, sessionid, userid):
        rowid = self.dal_manager. \
            insert_executed_file(fileid, args, output_log, output_print, sessionid, userid)

        output = dict()
        if rowid is not None:
            output["SUCCESS"] = True
            output["DATA"] = dict()
            output["DATA"]["id"] = rowid
        else:
            output["SUCCESS"] = False

        return output

    def update_executed_file(self, fileid, output_log, output_print, sessionid, userid):
        rowupdated = self.dal_manager. \
            update_executed_file(fileid, output_log, output_print, sessionid, userid)

        output = dict()
        if rowupdated is not None and rowupdated > 0:
            output["SUCCESS"] = True
        else:
            output["SUCCESS"] = False

        return output

    def update_executed_file_wsession(self, execid, output_log, output_print, sessionid, userid):
        rowupdated = self.dal_manager. \
            update_executed_file_wsession(execid, output_log, output_print, sessionid, userid)

        output = dict()
        if rowupdated is not None and rowupdated > 0:
            output["SUCCESS"] = True
        else:
            output["SUCCESS"] = False

        return output

    def get_execution_details(self, fileid):
        all_rows = self.dal_manager.\
            get_execution_details(fileid)

        output = dict()
        if all_rows is not None and len(all_rows) > 0:
            output["SUCCESS"] = True
            output["DATA"] = []
            for row in all_rows:
                row_dict = dict()
                row_dict["id"] = row[0]
                row_dict["filecontent"] = row[1]
                row_dict["input_files"] = row[2]
                row_dict["output_files"] = row[3]
                row_dict["output_log"] = row[4]
                row_dict["createdon"] = json.dumps(row[6], cls=MyEncoder)
                row_dict["modifiedon"] = json.dumps(row[7], cls=MyEncoder)

                output["DATA"].append(row_dict)
        else:
            output["SUCCESS"] = False

        return output

    def get_model_status(self,id):
        status = self.dal_manager. \
            get_model_status(id)
        return status

    # datasetName,modelName,algoName,algoPara
    def get_model_train_info(self,userId,jobId):
        all_rows = self.dal_manager.get_model_train_info(userId,jobId)
        output = dict()
        if all_rows is not None and len(all_rows) > 0:
            output["SUCCESS"] = True
            output["DATA"] = []
            for row in all_rows:
                row_dict = dict()
                row_dict["datasetName"]  = row[0]
                row_dict["modelName"] = row[1]
                row_dict["algoName"] = row[2]
                row_dict["algoPara"] = row[3]
                output["DATA"].append(row_dict)
        else:
            output["SUCCESS"] = False
        return output

    def get_model_detail_list(self,userId):
        all_rows = self.dal_manager. \
            get_model_detail_list(userId)

        output = dict()
        if all_rows is not None and len(all_rows) > 0:
            output["SUCCESS"] = True
            output["DATA"] = []
            for row in all_rows:
                row_dict = dict()
                row_dict["jobId"] = row[2]
                row_dict["modelName"] = row[3]
                row_dict["algoName"] = row[4]
                if row[5] == 0:
                    row_dict["status"] = "Newed"
                elif row[5] == 1:
                    row_dict["status"] = "Saved"
                elif row[5] == 2:
                    row_dict["status"] = "Failed"
                row_dict["modelPath"] = row[6]
                row_dict["time"] = row[7]
                # row_dict["algoPara"] = eval(row[6])
                row_dict["createTime"] = row[9]
                row_dict["datasetName"] = row[10]

                output["DATA"].append(row_dict)
        else:
            output["SUCCESS"] = False
            output["info"] = "model: does not exist"

        return output

    def SearchModelResultId(self,userId,jobId):
        row = self.dal_manager. \
            SearchModelResultId(userId,jobId)
        output = dict()
        if row is not None and len(row) > 0:
            output["SUCCESS"] = True
            output["DATA"] = []
            row_dict = dict()
            row_dict["jobId"] = row[2]
            row_dict["modelName"] = row[3]
            row_dict["algoName"] = row[4]
            if row[5] == 0:
                row_dict["status"] = "Newed"
            elif row[5] == 1:
                row_dict["status"] = "Saved"
            elif row[5] == 2:
                row_dict["status"] = "Failed"
            row_dict["modelPath"] = row[6]
            row_dict["time"] = row[7]
            row_dict["createTime"] = row[9]
            row_dict["datasetName"] = row[10]
            output["DATA"].append(row_dict)
        else:
            output["SUCCESS"] = False
            output["info"] = "model: does not exist"
        return output

    def SearchModelResultName(self,userId,jobId):
        all_rows = self.dal_manager. \
            SearchModelResultName(userId, jobId)
        output = dict()
        if all_rows is not None and len(all_rows) > 0:
            output["SUCCESS"] = True
            output["DATA"] = []
            for row in all_rows:
                row_dict = dict()
                row_dict["jobId"] = row[2]
                row_dict["modelName"] = row[3]
                row_dict["algoName"] = row[4]
                if row[5] == 0:
                    row_dict["status"] = "Newed"
                elif row[5] == 1:
                    row_dict["status"] = "Saved"
                elif row[5] == 2:
                    row_dict["status"] = "Failed"
                row_dict["modelPath"] = row[6]
                row_dict["time"] = row[7]
                row_dict["createTime"] = row[9]
                row_dict["datasetName"] = row[10]
                output["DATA"].append(row_dict)
        else:
            output["SUCCESS"] = False
            output["info"] = "model: does not exist"
        return output

    def get_model_details_by_modelName(self, userId,modelName):
        all_rows = self.dal_manager. \
            get_model_detail(userId,modelName)

        output = dict()
        if all_rows is not None and len(all_rows) > 0:
            output["SUCCESS"] = True
            output["DATA"] = []
            for row in all_rows:
                row_dict = dict()
                row_dict["JobId"] = row[2]
                row_dict["modelName"] = row[3]
                row_dict["algoName"] = row[4]
                row_dict["status"] = row[5]
                row_dict["modelPath"] = row[6]
                row_dict["time"] = row[7]
                # row_dict["algoPara"] = eval(row[6])
                row_dict["createTime"] = row[8]
                row_dict["datasetName"] = row[9]

                output["DATA"].append(row_dict)
        else:
            output["SUCCESS"] = False
            output["info"] = "model: does not exist"

        return output

    def get_dataType(self, userId, datasetName):
        all_rows = self.dal_manager. \
            get_dataType(userId, datasetName)
        return all_rows[0][0]

    def get_data_filename(self, id):
        all_rows = self.dal_manager. \
            get_data_filename(id)
        return all_rows[0][0]

    def get_codePath_file(self,fileid):
        all_rows = self.dal_manager. \
            get_codePath_file(fileid)
        output = dict()
        output["filename"] = all_rows[0][0]
        output["codePath"] = all_rows[0][1]
        return output
        # return all_rows[0]

    def get_algoName(self, userId, modelName):
        all_rows = self.dal_manager. \
            get_algoName(userId, modelName)
        return all_rows[0][0]

    def insert_new_model(self, userId,jobId, modelName, algoName,modelPath,algoPara,datasetName):
        rowid = self.dal_manager. \
            insert_new_model(userId,jobId, modelName, algoName,modelPath,algoPara,datasetName)

        output = dict()
        if rowid is not None:
            output["SUCCESS"] = True
            output["DATA"] = dict()
            output["DATA"]["id"] = rowid
        else:
            output["SUCCESS"] = False
        return output

    def insert_res_result(self,jobId,userId ,filename,modelPath):
        rowid = self.dal_manager. \
            insert_res_result(jobId,userId ,filename,modelPath)
        output = dict()
        if rowid is not None:
            output["SUCCESS"] = True
            output["DATA"] = dict()
            output["DATA"]["id"] = rowid
        else:
            output["SUCCESS"] = False
        return output

    def insert_model_result(self,jobId,auc_score, auc_std_err):
        res = dict()
        res["auc_score"] = auc_score
        res["auc_std_err"] = auc_std_err
        print str(res)
        rowid = self.dal_manager. \
                insert_model_result(jobId,str(res))
        output = dict()
        if rowid is not None:
            output["SUCCESS"] = True
            output["DATA"] = dict()
            output["DATA"]["id"] = rowid
        else:
            output["SUCCESS"] = False
        return output

    def insert_dataset(self, userId, outputName,dataType, dataPath, t1,t0):
        rowid = self.dal_manager. \
            insert_dataset(userId, outputName,dataType, dataPath, t0)

        output = dict()
        if rowid is not None:
            output["SUCCESS"] = True
            output["prediction time"] = t1 - t0
        else:
            output["SUCCESS"] = False
        return output


    def update_old_model(self, algoPara,datasetName,modelPath,userId,modelName,jobId):
        rowcount = self.dal_manager.update_old_model(algoPara,datasetName,modelPath,userId,modelName,jobId)

        output = dict()
        if rowcount is not None and rowcount > 0:
            output["SUCCESS"] = True
        else:
            output["SUCCESS"] = False

        return output

    def update_trains_model(self, t, t0, userId, modelName):
        rowcount = self.dal_manager.update_trains_model(t, t0, userId, modelName)

        output = dict()
        if rowcount is not None and rowcount > 0:
            output["SUCCESS"] = True
            output["modelName"] = modelName
            output["time"] = t
        else:
            output["SUCCESS"] = False

        return output

    def update_trainf_model(self, t, t0, userId, modelName):
        rowcount = self.dal_manager.update_trainf_model(t, t0, userId, modelName)

        output = dict()
        if rowcount is not None and rowcount > 0:
            output["info"] = "train successed"
        return output

    def searchDataset(self, userId, datasetName):
        rowcount = self.dal_manager.searchDataset(userId, datasetName)
        if len(rowcount) == 0:
            return ""
        dataPath = rowcount[0][0]
        return dataPath

    def searchModel(self, userId, modelName):
        rowcount = self.dal_manager.get_modelPath(userId, modelName)
        if len(rowcount) == 0:
            return ""
        dataPath = rowcount[0][0]
        return dataPath

    def authentication(self, user, passwd):
        rowcount = self.dal_manager.authentication(user)
        if len(rowcount) == 0:
            return "fail"
        password = rowcount[0][1]
        userId = rowcount[0][0]

        if passwd == password:
            return userId
        else:
            return "fail"

    def check_duplicate(self,userId,modelName):
        rowcount = self.dal_manager.check_duplicate(userId,modelName)
        if len(rowcount) == 0:
            return "fail"
        else:
            return 1

