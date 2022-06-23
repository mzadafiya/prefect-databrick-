# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Data Load
# MAGIC
# MAGIC Combine all results into csv file in dbfs

# COMMAND ----------

import os
import json
import pandas as pd
from setup_logger import create_logger

# COMMAND ----------

logger = create_logger(task='Data Load')
logger.info('Starting task run...')

# COMMAND ----------

logger.info('Reading widgets(passed parameters) dbfs_file_path, dbfs_response_files')

dbfs_file_path = dbutils.widgets.get('dbfs_file_path')
response_file_path = dbutils.widgets.get('dbfs_response_files')

dbfs_response_files = "/dbfs%s" % response_file_path

# COMMAND ----------

logger.info('Combine results: Starting')

response_data = []
if os.path.isdir(dbfs_response_files):

    for res_file in os.listdir(dbfs_response_files):
        logger.info('Combine results: Reading %s file' % res_file)

        f = open(os.path.join(dbfs_response_files, res_file))
        try:
            response_data.append(json.load(f))
        except ValueError as verr:
            logger.error('Combine results: Decoding JSON has failed')
        f.close()
else:
    raise Exception("Temp directory with results does not exist. Dir: %s" % dbfs_response_files)

if not response_data:
    raise Exception("Combine results: Failed. no valid results found.")

logger.info('Combine results: Completed')

# COMMAND ----------

dbutils.fs.rm(response_file_path, True)

# COMMAND ----------

dbfs_filename = '/dbfs%s' % dbfs_file_path

# create directories if not exist
os.makedirs(os.path.dirname(dbfs_filename), exist_ok=True)

# COMMAND ----------

logger.info('Creating panda dataframe from collected response')

df = pd.read_json(json.dumps(response_data))
df.to_csv(dbfs_filename)

# COMMAND ----------

logger.info('Creating or overwriting dbfs file from panda dataframe')

df = spark.read.format('csv').option('header', 'true').load(dbfs_file_path)
df.show()

# COMMAND ----------

if not os.path.exists(dbfs_filename):
    raise Exception('Combine all results to dbfs file is failed. A file does not exist')

logger.info('Combine all results to dbfs file is successful')
