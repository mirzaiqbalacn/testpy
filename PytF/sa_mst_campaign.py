import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.types import *
from pyspark.rdd import reduce
import time
import datetime
import logging
from pyspark.sql import SQLContext

args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sparkContext = SparkContext()
glueContext = GlueContext(sparkContext)
job = Job(glueContext)
sparkSession = glueContext.spark_session
sqlContext = SQLContext(sparkContext)
spark = glueContext.spark_session
logger = glueContext.get_logger()
job.init(args['JOB_NAME'], args)


## configs
# bucket_name = args['BUCKET_NAME']
temp_dir = args['TempDir']
glue_id = args["JOB_RUN_ID"]
glue_name = args["JOB_NAME"]

from suse_functions import glue_log_table, create_logs, cr_time

## configs

######### procedures and truncate in here
######### To be modified for each job ->
target_table = 'bi_sf_int.sa_mst_Campaign'
procedure = 'bi_sf_int.sa_mst_Campaign_load'
obj_name = 'SALESFORCE_CAMPAIGN' #object name ie prefix in each file
obj_name_sub = 'Campaign' #exact folder name
catalog_connection = 'sa_stg_con'

target_db = 'dwh'
######### <- To be modified for each job

truncate_statement = f"begin;truncate table {target_table};end;";
procedure_statement = f"begin;call {procedure}('{glue_id}');commit;end;"
######### 


## This try/expection
## Logic is as following

## There are three steps for which a logging df is created 
## The process: Create 1 log -> ingest the log in redshift -> run truncate statement as postaction  | log indicates that the job has started
## 				Create 2 log -> run procedure as preaction -> ingest the log into redshift 			| log indicates that postaction and preaction are successfull
##If error		Create 3 log -> ingest into redshift with error description							| log indicates that there is an error
for i in range(0,1):
	while True:
		try:
			logger.info(f"{':'.join([obj_name, obj_name_sub])}: Start")
			start_time = cr_time()
			###log -> truncate -> insert table into redshift -> log 
			row_n = 0
			applylogging = create_logs(
				sc=sparkContext,
				object_name=':'.join([obj_name, obj_name_sub]), 
				glue_job_name=glue_name, 
				glue_run_id=glue_id, 
				start_time=start_time, 
				end_time=cr_time(),
				layer='INTERMEDIATE', 
				table=target_table, 
				row_n=0, 
				process='PROCEDURE', 
				status='START', 
				error='', 
				other="||".join([f'{i} try',truncate_statement,procedure_statement]),
				file_names='')
			datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = applylogging,
																		catalog_connection = catalog_connection, 
																		connection_options = {  #"preactions":procedure_statement,
																								"postactions":truncate_statement, 
																								"dbtable": glue_log_table, 
																								"database": target_db}, 
																		redshift_tmp_dir = temp_dir, 
																		transformation_ctx = "datasink4")
			applylogging = create_logs(
				sc=sparkContext,
				object_name=':'.join([obj_name, obj_name_sub]), 
				glue_job_name=glue_name, 
				glue_run_id=glue_id, 
				start_time=start_time, 
				end_time=cr_time(),
				layer='INTERMEDIATE', 
				table=target_table, 
				row_n=0, 
				process='PROCEDURE', 
				status='SUCCESS', 
				error='', 
				other="||".join([f'{i} try',truncate_statement,procedure_statement]),
				file_names='')																		
			datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = applylogging,
																		catalog_connection = catalog_connection, 
																		connection_options = {  "preactions":procedure_statement,
																								#"postactions":truncate_statement, 
																								"dbtable": glue_log_table, 
																								"database": target_db},
																		redshift_tmp_dir = temp_dir, 
																		transformation_ctx = "datasink4")
			logger.info(f"{':'.join([obj_name, obj_name_sub])}: Success")
		except Exception as e:
			applylogging = create_logs(
				sc=sparkContext,
				object_name=':'.join([obj_name, obj_name_sub]), 
				glue_job_name=glue_name, 
				glue_run_id=glue_id, 
				start_time=start_time, 
				end_time=cr_time(),
				layer='INTERMEDIATE', 
				table=target_table, 
				row_n=0, 
				process='PROCEDURE', 
				status='FAIL', 
				error= f'{i} try.{str(e)}', 
				other="||".join([f'{i} try',truncate_statement,procedure_statement]),
				file_names='')		
			datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = applylogging,
																		catalog_connection = catalog_connection, 
																		connection_options = {#"preactions":procedure_statement,
																								#"postactions":truncate_statement, 
																								"dbtable": glue_log_table, 
																								"database": target_db},
																		redshift_tmp_dir = temp_dir, 
																		transformation_ctx = "datasink4")
			logger.info(f"{':'.join([obj_name, obj_name_sub])}: Fail")
		break


job.commit()