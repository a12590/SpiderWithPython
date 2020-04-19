#!/usr/local/miniconda2/bin/python
# _*_ coding: utf-8 _*_

import datetime
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
                                          # charset="utf8",autocommit = True)
                                          charset="utf8")
        self.cursor = self.connection.cursor()

    def update_data_file(self,jobId, label, weights,firstLine,separate):
        self.cursor.execute("""
                            UPDATE datafile SET label=%s ,weights = %s,firstLine= %s,separate = %s
                            WHERE jobId =%s
                            """, ( label, weights,firstLine,separate,int(jobId)))
        rowcount = self.cursor.rowcount
        self.connection.commit()
        return rowcount

    def update_date_jobId(self,id,jobId):
        self.cursor.execute("""
                            UPDATE datafile SET jobId=%s
                            WHERE id =%s
                            """, (jobId, int(id)))
        rowcount = self.cursor.rowcount
        self.connection.commit()
        return rowcount

    def update_job_property(self, jobId):
        self.cursor.execute("""
                            UPDATE job SET property= property + 1
                            WHERE jobId =%s
                            """, (int(jobId),))
        rowcount = self.cursor.rowcount
        self.connection.commit()
        return rowcount

    def update_job_log(self,jobId,output):
        self.cursor.execute("""
                            UPDATE job SET print_log= %s
                            WHERE jobId =%s
                            """, (output,int(jobId)))
        rowcount = self.cursor.rowcount
        self.connection.commit()
        return rowcount

    def update_job_jobName(self,userId,jobId,jobName):
        self.cursor.execute("""
                            UPDATE job SET jobName = %s
                            WHERE jobId =%s and userId = %s
                            """, (jobName, int(jobId),int(userId)))
        rowcount = self.cursor.rowcount
        self.connection.commit()
        return rowcount

    def update_job_status_s(self,jobId):
        self.cursor.execute("""
                            UPDATE job SET status= %s
                            WHERE jobId =%s
                            """, (1,int(jobId)))
        rowcount = self.cursor.rowcount
        self.connection.commit()
        return rowcount

    def update_job_status_f(self, jobId):
        self.cursor.execute("""
                            UPDATE job SET status= %s
                            WHERE jobId =%s
                            """, (3, int(jobId)))
        rowcount = self.cursor.rowcount
        self.connection.commit()
        return rowcount

    def update_job_status_r(self,jobId):
        self.cursor.execute("""
                            UPDATE job SET status= %s
                            WHERE jobId =%s
                            """, (2, int(jobId)))
        rowcount = self.cursor.rowcount
        self.connection.commit()
        return rowcount

    def update_job_property_model(self,jobId):
        self.cursor.execute("""
                            UPDATE job SET property= property + 2
                            WHERE jobId =%s
                            """, (int(jobId),))
        rowcount = self.cursor.rowcount
        self.connection.commit()
        return rowcount

    def update_model_info(self, jobId, algoPara):
        self.cursor.execute("""
                            UPDATE model SET algoPara=%s
                            WHERE jobId =%s
                            """, (algoPara, int(jobId)))
        rowcount = self.cursor.rowcount
        self.connection.commit()
        return rowcount

    def get_model_list_withDataSet(self, userId):
        self.cursor.execute("""
                        SELECT id,datasetName,modelName
                        FROM model
                        WHERE userId = %s AND Status = 1 AND algoName <> 'Cor' and algoName <> 'Apriori'
                        """, (userId))
        all_rows = self.cursor.fetchall()
        return all_rows

    def insert_new_file(self, filename, filecontent, userid,hdfs_dataPath):
        createdon = datetime.datetime.now()
        self.cursor.execute("""
                            INSERT INTO files(filename, content, userid, createdon,codePath)
                            VALUES(%s,%s,%s,%s,%s)
                            """, (filename, filecontent, userid, createdon,hdfs_dataPath))
        rowid = self.cursor.lastrowid
        self.connection.commit()

        return rowid

    def insert_new_user(self, userName, password):
        createdon = datetime.datetime.now()
        self.cursor.execute("""
                            INSERT INTO user(userName, password)
                            VALUES(%s,%s)
                            """, (userName, password))
        rowid = self.cursor.lastrowid
        self.connection.commit()
        return rowid

    def insert_new_data_file(self, filename, path, userid,datasetName,firstLine,separate):
        timenow = datetime.datetime.now()
        self.cursor.execute("""
                            INSERT INTO datafile(filename, path, userid, createdon,datasetName,firstLine,separate,firstStatus)
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
                            """, (filename, path, userid, timenow,datasetName,firstLine,separate,1))
        rowid = self.cursor.lastrowid
        self.connection.commit()

        return rowid

    def insert_new_data_file_with_copy(self,jobId,id):
        timenow = datetime.datetime.now()
        self.cursor.execute("""
                                INSERT INTO datafile(jobId,userid,filename,isdeleted,deletedon,modifiedon, createdon,path,separate ,firstLine,label,cat_list,num_list,weights,datasetName,firstStatus)
                                SELECT %s,userid,filename,isdeleted,deletedon,modifiedon, %s,path,separate ,firstLine,label,cat_list,num_list,weights,datasetName,-1  FROM datafile WHERE id = %s
                                """,(int(jobId),timenow,id))
        rowid = self.cursor.lastrowid
        self.connection.commit()
        return rowid

    def update_file(self, fileid, filecontent):
        updatedon = datetime.datetime.now()
        self.cursor.execute("""
                            UPDATE files SET content=%s , modifiedon = %s
                            WHERE id =%s AND isdeleted = 0
                            """, (filecontent, updatedon, fileid))
        rowcount = self.cursor.rowcount
        self.connection.commit()

        return rowcount

    def get_model_algos(self,jobId):
        self.cursor.execute("""
                                SELECT algoPara FROM model
                                WHERE jobId=%s
                                """, (jobId,))
        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows[0]
            return row
        return None

    def exist(self,jobId):
        self.cursor.execute("""
                                SELECT * FROM model
                                WHERE jobId=%s
                                """, (jobId,))
        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows[0]
            return row
        return None

    def exist_datefile(self,jobId):
        self.cursor.execute("""
                                SELECT * FROM datafile
                                WHERE jobId=%s
                                """, (jobId,))
        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows[0]
            return row
        return None

    def get_evaluate_info(self,userId, ids):
        self.cursor.execute("""
                                SELECT time,Result FROM model
                                WHERE userId=%s AND jobId in %s
                                """, (userId, ids))
        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows
            return row
        return None

    def get_model_details(self,id):
        self.cursor.execute("""
                                SELECT algoPara FROM model
                                WHERE id=%s
                                """, (id,))
        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows[0]
            return row
        return None

    def get_algo_details(self,id):
        self.cursor.execute("""
                                SELECT algoPara FROM algo
                                WHERE id=%s
                                """, (id,))
        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows[0]
            return row
        return None

    def get_datasetName(self,jobId):
        # TODO
        self.cursor.execute("""
                                SELECT datasetName FROM datafile
                                WHERE jobId=%s
                                """, (jobId,))
        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows[0]
            return row
        return None

    def get_job_log(self, jobId):
        self.cursor.execute("""
                                SELECT print_log FROM job
                                WHERE jobId=%s
                                """, (jobId,))
        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows[0]
            return row
        return None


    def get_model_train_info(self,userId,jobId):
        self.cursor.execute("""
                            SELECT datasetName,modelName,algoName,algoPara FROM model
                            WHERE userid=%s and jobId =%s
                            """, (userId,jobId))

        all_rows = self.cursor.fetchall()
        return all_rows

    def getJobResult(self,userId):
        self.cursor.execute("""
                            SELECT * FROM job
                            WHERE userId =%s order by jobId desc
                            """, (userId,))
        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows
            return row
        return None

    def getJobProperty(self,jobId):
        self.cursor.execute("""
                            SELECT property FROM job
                            WHERE jobId =%s
                            """, (jobId,))
        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows
            return row
        return None

    def getJobName(self,jobId):
        self.cursor.execute("""
                            SELECT jobName FROM job
                            WHERE jobId =%s
                            """, (jobId,))
        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows
            return row
        return None

    def get_filepath_res(self,userId,jobId):
        self.cursor.execute("""
                            SELECT path FROM result
                            WHERE userId=%s AND jobId = %s
                            """, (userId, int(jobId)))
        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows[0]
            return row
        return None

    def get_filename_res(self,userId,jobId):
        self.cursor.execute("""
                            SELECT filename FROM result
                            WHERE userId=%s AND jobId = %s
                            """, (userId, int(jobId)))
        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows[0]
            return row
        return None

    def SearchJobResultId(self,userId,jobId):
        self.cursor.execute("""
                            SELECT * FROM job
                            WHERE userId=%s AND jobId = %s
                            """, (userId,int(jobId)))
        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows[0]
            return row
        return None

    def SearchModelResultId(self,userId,jobId):
        self.cursor.execute("""
                    SELECT *
                    FROM model
                    WHERE userId = %s AND jobId = %s
                    """, (userId,jobId))
        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows[0]
            return row
        return None

    def SearchModelResultName(self, userId, searchContext):
        print searchContext
        self.cursor.execute(
            "SELECT * FROM model WHERE modelName LIKE '%%%%%s%%%%'" % searchContext + "AND userId= %s " % userId + "and algoName <> 'Cor' and algoName <> 'Apriori'"+" order by jobId desc")
        all_rows = self.cursor.fetchall()
        return all_rows

    def SearchJobResultName(self,userId, searchContext):
        self.cursor.execute("SELECT * FROM job WHERE jobName LIKE '%%%%%s%%%%'" % searchContext + "AND userId= %s " % userId + " order by jobId desc")

        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows
            return row
        return None

    def getResResult_context(self,userId, searchContext):
        self.cursor.execute(
            "SELECT jobId, path,filename, createdon FROM result WHERE filename LIKE '%%%%%s%%%%'" % searchContext + "AND userId= %s " % userId + " order by jobId desc")
        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows
            return row
        return None

    def get_data_file_info(self,jobId):
        # TODO
        self.cursor.execute("""
                            SELECT * FROM datafile
                            WHERE jobId=%s
                            """, (int(jobId),))
        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows[0]
            return row
        return None

    def get_data_file_info_l(self, jobId):
        # TODO
        self.cursor.execute("""
                            SELECT * FROM datafile
                            WHERE jobId=%s
                            """, (int(jobId),))
        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows
            return row
        return None

    def get_data_file_detail(self, id):
        # TODO
        self.cursor.execute("""
                            SELECT * FROM datafile
                            WHERE id=%s
                            """, (int(id),))
        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows[0]
            return row
        return None

    def get_file(self, fileid):
        self.cursor.execute("""
                            SELECT filename, content FROM files
                            WHERE id=%s AND isdeleted = 0
                            """, (fileid,))
        all_rows = self.cursor.fetchall()

        # Return filename and content
        if len(all_rows) > 0:
            row = all_rows[0]
            return row[0], row[1]

        return None, None

    def get_exec_details(self, execid):
        self.cursor.execute("""
                            SELECT sessionid, fileid, arguments FROM execution
                            WHERE id=%s
                            """, (execid,))
        all_rows = self.cursor.fetchall()

        # Return filename and content
        if len(all_rows) > 0:
            row = all_rows[0]
            return row[0], row[1], row[2]

        return None, None, None

    def get_all_files(self, userid):
        self.cursor.execute("""
                            SELECT id, filename, createdon, modifiedon FROM files
                            WHERE userid=%s AND isdeleted = 0
                            """, (userid,))
        all_rows = self.cursor.fetchall()
        return all_rows

    def get_all_data_files(self,userid):
        self.cursor.execute("""
                            SELECT id, datasetName,filename,createdon
                            FROM datafile
                            WHERE  userid=%s AND isdeleted = 0 AND firstStatus = 1
                            """, (userid,))
        all_rows = self.cursor.fetchall()
        return all_rows

    # def get_model_list_withDataSet(self,userId):
    #     self.cursor.execute("""
    #                         SELECT datafile.id, datafile.datasetName,datafile.filename,datafile.createdon,Ranking.num
    #                         FROM datafile,(
    #                         SELECT datasetName,COUNT(*) as num FROM datafile GROUP BY datasetName) Ranking
    #                         WHERE datafile.datasetName = Ranking.datasetName AND userid=%s AND isdeleted = 0
    #                         GROUP BY datafile.datasetName,id
    #                         """, (userId,))
    #
    #     all_rows = self.cursor.fetchall()
    #     return all_rows

    def get_all_data_files_withDataSet(self, userid):
        self.cursor.execute("""
                            SELECT datafile.id, datafile.datasetName,datafile.filename,datafile.createdon,Ranking.num
                            FROM datafile,(
                            SELECT datasetName,COUNT(*) as num FROM datafile GROUP BY datasetName) Ranking
                            WHERE datafile.datasetName = Ranking.datasetName AND userid=%s AND isdeleted = 0
                            GROUP BY datafile.datasetName,id
                            """, (userid,))
        all_rows = self.cursor.fetchall()
        return all_rows

    def get_all_data_files_infolist(self,userid):
        self.cursor.execute("""
                            SELECT jobId, path,filename, createdon FROM datafile
                            WHERE userid=%s AND isdeleted = 0
                            """, (userid,))
        all_rows = self.cursor.fetchall()
        return all_rows

    def getResResult(self,userid):
        self.cursor.execute("""
                            SELECT jobId, path,filename, createdon FROM result
                            WHERE userid=%s AND status = 0 order by jobId desc
                            """, (userid,))
        all_rows = self.cursor.fetchall()
        return all_rows

    def getResResult_id(self,userId,jobId):
        self.cursor.execute("""
                            SELECT jobId, path,filename, createdon FROM result
                            WHERE userid=%s AND status = 0 AND jobId = %s
                            """, (userId,jobId))
        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows[0]
            return row
        return None

    def delete_file(self, fileid):
        deletedon = datetime.datetime.now()
        self.cursor.execute("""
                            UPDATE files SET isdeleted=1, deletedon = %s
                            WHERE id =%s AND isdeleted = 0
                            """, (deletedon, fileid))

        rowcount = self.cursor.rowcount
        self.connection.commit()

        return rowcount

    def delete_data_file(self, fileid):
        deletedon = datetime.datetime.now()
        self.cursor.execute("""
                            UPDATE datafile SET isdeleted=1, deletedon = %s
                            WHERE id =%s AND isdeleted = 0
                            """, (deletedon, fileid))

        rowcount = self.cursor.rowcount
        self.connection.commit()

        return rowcount

    def insert_executed_file(self, fileid, args, output_log, output_print, sessionid, userid):
        createdon = datetime.datetime.now()
        self.cursor.execute("""
                            INSERT INTO execution(fileid, arguments,filecontent, outputlog,
                            print_output, sessionid, userid, createdon)
                            VALUES(%s,%s, (SELECT content FROM files WHERE id=%s),%s,%s,%s,%s,%s)
                            """, (fileid, args,fileid, output_log, output_print, sessionid, userid, createdon))

        rowid = self.cursor.lastrowid
        self.connection.commit()
        return rowid

    def update_executed_file(self, fileid, output_log, output_print, sessionid, userid):
        timenow = datetime.datetime.now()
        self.cursor.execute("""
                            UPDATE execution SET outputlog = %s,
                            print_output = %s,
                            modifiedon = %s
                            WHERE fileid = %s AND userid = %s AND sessionid = %s
                            """, (output_log, output_print, timenow, fileid, userid, sessionid))

        rowcount = self.cursor.rowcount
        self.connection.commit()

        return rowcount

    def update_executed_file_wsession(self, execid, output_log, output_print, sessionid, userid):
        timenow = datetime.datetime.now()
        self.cursor.execute("""
                            UPDATE execution SET outputlog = %s,
                            print_output = %s, sessionid = %s,
                            modifiedon = %s
                            WHERE id = %s
                            """, (output_log, output_print, sessionid, timenow, execid))

        rowcount = self.cursor.rowcount
        self.connection.commit()

        return rowcount

    def get_execution_details(self, fileid):
        self.cursor.execute("""
                            SELECT id,
                                   filecontent,
                                   input_files,
                                   output_files,
                                   outputlog,
                                   print_output,
                                   userid,
                                   createdon,
                                   modifiedon
                            FROM execution
                            WHERE fileid = %s;
                            """, (fileid,))
        all_rows = self.cursor.fetchall()
        return all_rows

    def get_model_status(self,id):
        self.cursor.execute("""
                                SELECT status FROM model
                                WHERE jobId=%s
                                """, (id,))

        all_rows = self.cursor.fetchall()
        if len(all_rows) > 0:
            row = all_rows[0]
            return row
        return None

    def get_model_detail_list(self, userId):
        self.cursor.execute("""
                        SELECT *
                        FROM model
                        WHERE userId = %s and algoName <> 'Cor' and algoName <> 'Apriori' order by jobId desc
                        """, (userId))
        all_rows = self.cursor.fetchall()
        return all_rows

    def get_model_detail(self,userId,modelName):
        self.cursor.execute("""
                        SELECT *
                        FROM model
                        WHERE userId = %s
                        AND modelName = %s
                        """, (userId,modelName))
        all_rows = self.cursor.fetchall()
        return all_rows


    def get_dataType(self, userId, datasetName):
        self.cursor.execute("""
                        SELECT dataType
                        FROM dataset
                        WHERE userId = %s
                        AND datasetName = %s
                        """, (userId, datasetName))
        all_rows = self.cursor.fetchall()
        return all_rows

    def get_data_filename(self, id):
        self.cursor.execute("""
                        SELECT filename
                        FROM datafile
                        WHERE id = %s
                        """, (id,))
        all_rows = self.cursor.fetchall()
        return all_rows

    def get_codePath_file(self,fileid):
        self.cursor.execute("""
                        SELECT filename,codePath
                        FROM files
                        WHERE id = %s
                        """, (fileid,))
        all_rows = self.cursor.fetchall()
        return all_rows

    def get_modelPath(self, userId, modelName):
        self.cursor.execute("""
                        SELECT modelPath
                        FROM model
                        WHERE userId = %s
                        AND modelName = %s
                        AND status = %s
                        """, (userId, modelName,0))
        all_rows = self.cursor.fetchall()
        return all_rows

    def get_algoName(self, userId, modelName):
        self.cursor.execute("""
                        SELECT algoName
                        FROM model
                        WHERE userId = %s
                        AND modelName = %s
                        """, (userId, modelName))
        all_rows = self.cursor.fetchall()
        return all_rows

    def insert_new_model(self, userId,jobId, modelName, algoName,modelPath,algoPara,datasetName):
        self.cursor.execute("""
                            INSERT INTO model(userId,jobId,modelName,algoName,status,modelPath,algoPara,datasetName)
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
                            """, (userId,jobId, modelName, algoName,0,str(modelPath),str(algoPara),datasetName))
        rowid = self.cursor.lastrowid
        self.connection.commit()
        return rowid

    def insert_res_result(self,jobId,userId ,filename,modelPath):
        createdon = datetime.datetime.now()
        self.cursor.execute("""
                                    INSERT INTO result(jobId,userId,filename,createdon,path,status)
                                    VALUES(%s,%s,%s,%s,%s,%s)
                                    """, (jobId, userId, filename,createdon,modelPath,0))
        rowid = self.cursor.lastrowid
        self.connection.commit()
        return rowid

    def insert_model_result(self,jobId,res):
        self.cursor.execute("""
                    UPDATE model SET Result = %s
                    WHERE jobId = %s
            """, (res, jobId))
        rowid = self.cursor.lastrowid
        self.connection.commit()
        return rowid

    def insert_new_model_with_copy(self,userId,jobId, id):
        self.cursor.execute("""
                                    INSERT INTO model(jobId,userId,modelName,algoName,status,modelPath,algoPara,datasetName)
                                    SELECT %s,%s,%s,algoName,0,%s,algoPara,%s FROM model WHERE id = %s
                                    """, (jobId, userId,"","","",id))
        rowid = self.cursor.lastrowid
        self.connection.commit()
        return rowid

    def insert_new_model_with_new(self,userId, jobId, algo,algoName,modelName):
        self.cursor.execute("""
                            INSERT INTO model(userId,jobId,modelName,algoName,status,modelPath,algoPara)
                            VALUES(%s,%s,%s,%s,%s,%s,%s)
                            """, (userId, jobId, modelName, algoName, 0, str(""), str(algo)))
        rowid = self.cursor.lastrowid
        self.connection.commit()

        return rowid

    def insert_new_job(self, userid,jobName):
        self.cursor.execute("""
                            INSERT INTO job(userId,status,jobName)
                            VALUES(%s,%s,%s)
                            """, (userid, 0,jobName))
        rowid = self.cursor.lastrowid
        self.connection.commit()
        return rowid

    def insert_dataset(self, userId, outputName,dataType, dataPath, t0):
        self.cursor.execute("""
                            INSERT INTO dataset(userId,datasetName,dataType,dataPath,createTime)
                            VALUES(%s,%s,%s,%s,%s)
                            """, (userId, outputName, dataType,dataPath, str(t0)))
        rowid = self.cursor.lastrowid
        self.connection.commit()
        return rowid

    def update_old_model(self,algoPara,datasetName,modelPath,userId,modelName,jobId):
        self.cursor.execute("""
                            UPDATE model SET status = %s,
                            algoPara = %s, datasetName = %s ,modelPath = %s
                            WHERE userId = %s AND modelName = %s AND jobId = %s
                            """, (0,str(algoPara),datasetName,modelPath,userId,modelName,int(jobId)))

        rowcount = self.cursor.rowcount
        self.connection.commit()
        return rowcount

    def update_trains_model(self, t, t0, userId, modelName):
        self.cursor.execute("""
                            UPDATE model SET status = %s,
                            time = %s, createTime = %s
                            WHERE userId = %s AND modelName = %s
                            """, (1, str(t), str(t0), userId, modelName))
        rowcount = self.cursor.rowcount
        self.connection.commit()
        return rowcount

    def update_trainf_model(self, t, t0, userId, modelName):
        self.cursor.execute("""
                            UPDATE model SET status = %s,
                            time = %s, createTime = %s
                            WHERE userId = %s AND modelName = %s
                            """, (2, str(t), str(t0), userId, modelName))
        rowcount = self.cursor.rowcount
        self.connection.commit()
        return rowcount

    def searchDataset(self,userId, datasetName):
        self.cursor.execute("""
                        SELECT dataPath
                        FROM dataset
                        WHERE userId = %s
                        AND datasetName = %s
                        """, (userId, datasetName))
        all_rows = self.cursor.fetchall()
        return all_rows

    def authentication(self,user):
        self.cursor.execute("""
                        SELECT userId,password
                        FROM user
                        WHERE userName = %s
                        """, (user, ))
        all_rows = self.cursor.fetchall()
        return all_rows

    def check_duplicate(self,userId,modelName):
        self.cursor.execute("""
                        SELECT id
                        FROM model
                        WHERE userId = %s AND modelName = %s
                        """, (userId,modelName))
        all_rows = self.cursor.fetchall()
        return all_rows

db_util = MysqlUtil(DB_Configure.mysql_host,
                    DB_Configure.mysql_user,
                    DB_Configure.mysql_password,
                    DB_Configure.mysql_database)